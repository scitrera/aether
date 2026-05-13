package timer

import (
	"sync"
	"time"

	"github.com/scitrera/aether/internal/logging"
)

// TimerSequence manages multiple timers for task timeouts
type TimerSequence struct {
	mu        sync.RWMutex
	timers    map[string]*TimeoutInfo // taskID -> TimeoutInfo
	timerChan chan *Timer
	stopChan  chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup

	// Timeout callbacks
	onScheduleToStartTimeout func(taskID string)
	onStartToCloseTimeout    func(taskID string)
	onHeartbeatTimeout       func(taskID string)
	onScheduleToCloseTimeout func(taskID string)
}

// NewTimerSequence creates a new TimerSequence manager
func NewTimerSequence() *TimerSequence {
	ts := &TimerSequence{
		timers:    make(map[string]*TimeoutInfo),
		timerChan: make(chan *Timer, 1000),
		stopChan:  make(chan struct{}),
	}
	ts.wg.Add(1)
	go ts.run()
	return ts
}

// SetScheduleToStartCallback sets the callback for ScheduleToStart timeout
func (ts *TimerSequence) SetScheduleToStartCallback(cb func(taskID string)) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.onScheduleToStartTimeout = cb
}

// SetStartToCloseCallback sets the callback for StartToClose timeout
func (ts *TimerSequence) SetStartToCloseCallback(cb func(taskID string)) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.onStartToCloseTimeout = cb
}

// SetHeartbeatCallback sets the callback for Heartbeat timeout
func (ts *TimerSequence) SetHeartbeatCallback(cb func(taskID string)) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.onHeartbeatTimeout = cb
}

// SetScheduleToCloseCallback sets the callback for ScheduleToClose timeout
func (ts *TimerSequence) SetScheduleToCloseCallback(cb func(taskID string)) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.onScheduleToCloseTimeout = cb
}

// CreateTimers creates all appropriate timers for a task based on its state and config
func (ts *TimerSequence) CreateTimers(taskID string, createdAt time.Time, config TimeoutConfig, state string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	info := &TimeoutInfo{
		TaskID:    taskID,
		Timeouts:  make(map[TimeoutType]*Timer),
		State:     state,
		CreatedAt: createdAt,
	}

	now := time.Now()

	// ScheduleToStart: Worker must claim task (for pending, assigned, or starting states)
	if config.ScheduleToStart > 0 && (state == "pending" || state == "assigned" || state == "starting") {
		expiry := createdAt.Add(config.ScheduleToStart)
		info.Timeouts[TimeoutScheduleToStart] = ts.scheduleTimerLocked(info, TimeoutScheduleToStart, expiry, now)
	}

	// StartToClose: Worker must complete execution (only for running state)
	if config.StartToClose > 0 && state == "running" {
		startedAt := info.StartedAt
		if startedAt == nil {
			startedAt = &now
		}
		expiry := startedAt.Add(config.StartToClose)
		info.Timeouts[TimeoutStartToClose] = ts.scheduleTimerLocked(info, TimeoutStartToClose, expiry, now)
	}

	// Heartbeat: Worker must send progress updates (only for running state)
	if config.Heartbeat > 0 && state == "running" {
		expiry := now.Add(config.Heartbeat)
		info.Timeouts[TimeoutHeartbeat] = ts.scheduleTimerLocked(info, TimeoutHeartbeat, expiry, now)
	}

	// ScheduleToClose: Overall deadline (always active)
	if config.ScheduleToClose > 0 {
		expiry := createdAt.Add(config.ScheduleToClose)
		info.Timeouts[TimeoutScheduleToClose] = ts.scheduleTimerLocked(info, TimeoutScheduleToClose, expiry, now)
	}

	ts.timers[taskID] = info
	logging.Logger.Info().Str("task_id", taskID).Str("state", state).Msg("created timers for task")
}

// ScheduleHeartbeatTimer schedules or reschedules the heartbeat timer
func (ts *TimerSequence) ScheduleHeartbeatTimer(taskID string, config TimeoutConfig, lastHeartbeat time.Time) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	info, exists := ts.timers[taskID]
	if !exists {
		return
	}

	// Remove existing heartbeat timer if present
	if existingTimer, ok := info.Timeouts[TimeoutHeartbeat]; ok && existingTimer.Cancel != nil {
		existingTimer.Cancel()
	}

	// Schedule new heartbeat timer
	if config.Heartbeat > 0 {
		expiry := lastHeartbeat.Add(config.Heartbeat)
		info.Timeouts[TimeoutHeartbeat] = ts.scheduleTimerLocked(info, TimeoutHeartbeat, expiry, time.Now())
		info.LastHeartbeat = &lastHeartbeat
	}
}

// UpdateTaskState updates the task state and creates/releases appropriate timers
func (ts *TimerSequence) UpdateTaskState(taskID string, newState string, startedAt *time.Time, config TimeoutConfig) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	info, exists := ts.timers[taskID]
	if !exists {
		// Task not tracked yet, create timers for new state
		ts.createTimersForStateLocked(taskID, newState, startedAt, config)
		return
	}

	// State transition handling
	oldState := info.State
	info.State = newState
	info.StartedAt = startedAt

	// Handle state transitions
	switch {
	case (oldState == "pending" || oldState == "assigned") && newState == "starting":
		// Task is starting (orchestrator launching) - keep ScheduleToStart active
		// No timer changes needed

	case (oldState == "pending" || oldState == "assigned" || oldState == "starting") && newState == "running":
		// Task started - cancel ScheduleToStart, start StartToClose and Heartbeat
		if timer, ok := info.Timeouts[TimeoutScheduleToStart]; ok && timer.Cancel != nil {
			timer.Cancel()
			delete(info.Timeouts, TimeoutScheduleToStart)
		}
		// Start StartToClose timer
		if config.StartToClose > 0 {
			startTime := startedAt
			if startTime == nil {
				now := time.Now()
				startTime = &now
			}
			expiry := startTime.Add(config.StartToClose)
			info.Timeouts[TimeoutStartToClose] = ts.scheduleTimerLocked(info, TimeoutStartToClose, expiry, time.Now())
		}
		// Start Heartbeat timer
		if config.Heartbeat > 0 {
			now := time.Now()
			expiry := now.Add(config.Heartbeat)
			info.Timeouts[TimeoutHeartbeat] = ts.scheduleTimerLocked(info, TimeoutHeartbeat, expiry, now)
		}

	case (oldState == "pending" || oldState == "assigned" || oldState == "starting") && newState == "COMPLETED",
		oldState == "running" && newState == "COMPLETED":
		// Task completed - cancel all timers
		ts.cancelAllTimersLocked(info)

	case (oldState == "pending" || oldState == "assigned" || oldState == "starting") && newState == "FAILED",
		oldState == "running" && newState == "FAILED":
		// Task failed - cancel all timers
		ts.cancelAllTimersLocked(info)
	}
}

// createTimersForStateLocked creates timers for a task in a specific state (internal helper)
func (ts *TimerSequence) createTimersForStateLocked(taskID string, state string, startedAt *time.Time, config TimeoutConfig) {
	now := time.Now()

	info := &TimeoutInfo{
		TaskID:    taskID,
		Timeouts:  make(map[TimeoutType]*Timer),
		State:     state,
		CreatedAt: now,
		StartedAt: startedAt,
	}

	switch state {
	case "pending", "assigned", "starting":
		// ScheduleToStart for tasks not yet running
		if config.ScheduleToStart > 0 {
			expiry := now.Add(config.ScheduleToStart)
			info.Timeouts[TimeoutScheduleToStart] = ts.scheduleTimerLocked(info, TimeoutScheduleToStart, expiry, now)
		}
	case "running":
		// StartToClose for running tasks
		if config.StartToClose > 0 {
			startTime := startedAt
			if startTime == nil {
				startTime = &now
			}
			expiry := startTime.Add(config.StartToClose)
			info.Timeouts[TimeoutStartToClose] = ts.scheduleTimerLocked(info, TimeoutStartToClose, expiry, now)
		}
		// Heartbeat timer for running tasks
		if config.Heartbeat > 0 {
			expiry := now.Add(config.Heartbeat)
			info.Timeouts[TimeoutHeartbeat] = ts.scheduleTimerLocked(info, TimeoutHeartbeat, expiry, now)
		}
	}

	// ScheduleToClose is always active (end-to-end deadline)
	if config.ScheduleToClose > 0 {
		expiry := now.Add(config.ScheduleToClose)
		info.Timeouts[TimeoutScheduleToClose] = ts.scheduleTimerLocked(info, TimeoutScheduleToClose, expiry, now)
	}

	ts.timers[taskID] = info
}

// scheduleTimerLocked schedules a timer (must hold lock)
func (ts *TimerSequence) scheduleTimerLocked(info *TimeoutInfo, timeoutType TimeoutType, expiry time.Time, now time.Time) *Timer {
	delay := time.Until(expiry)

	timer := time.AfterFunc(delay, func() {
		ts.timerChan <- &Timer{
			Type:      timeoutType,
			Expiry:    expiry,
			TaskID:    info.TaskID,
			CreatedAt: now,
		}
	})
	cancel := func() {
		timer.Stop()
	}

	return &Timer{
		Type:      timeoutType,
		Expiry:    expiry,
		TaskID:    info.TaskID,
		Cancel:    cancel,
		CreatedAt: now,
	}
}

// cancelAllTimersLocked cancels all timers for a task (must hold lock)
func (ts *TimerSequence) cancelAllTimersLocked(info *TimeoutInfo) {
	for timeoutType, timer := range info.Timeouts {
		if timer.Cancel != nil {
			timer.Cancel()
		}
		delete(info.Timeouts, timeoutType)
	}
}

// Remove removes a task's timers from the sequence
func (ts *TimerSequence) Remove(taskID string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	info, exists := ts.timers[taskID]
	if !exists {
		return
	}

	ts.cancelAllTimersLocked(info)
	delete(ts.timers, taskID)
	logging.Logger.Info().Str("task_id", taskID).Msg("removed timers for task")
}

// GetState returns the current tracked state for a task
func (ts *TimerSequence) GetState(taskID string) (string, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	info, exists := ts.timers[taskID]
	if !exists {
		return "", false
	}
	return info.State, true
}

// run is the main timer processing loop
func (ts *TimerSequence) run() {
	defer ts.wg.Done()

	for {
		select {
		case timer := <-ts.timerChan:
			ts.processTimer(timer)
		case <-ts.stopChan:
			return
		}
	}
}

// processTimer handles a fired timer
func (ts *TimerSequence) processTimer(t *Timer) {
	ts.mu.RLock()
	info, exists := ts.timers[t.TaskID]
	ts.mu.RUnlock()

	if !exists {
		logging.Logger.Warn().Str("task_id", t.TaskID).Str("type", string(t.Type)).Msg("timer fired for unknown task")
		return
	}

	// Verify timer is still active (hasn't been cancelled)
	ts.mu.RLock()
	activeTimer, stillActive := info.Timeouts[t.Type]
	ts.mu.RUnlock()

	if !stillActive || activeTimer != t {
		logging.Logger.Debug().Str("task_id", t.TaskID).Str("type", string(t.Type)).Msg("timer for task was cancelled")
		return
	}

	logging.Logger.Info().Str("task_id", t.TaskID).Str("type", string(t.Type)).Msg("timeout fired for task")

	// Call appropriate callback
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	switch t.Type {
	case TimeoutScheduleToStart:
		if ts.onScheduleToStartTimeout != nil {
			ts.onScheduleToStartTimeout(t.TaskID)
		}
	case TimeoutStartToClose:
		if ts.onStartToCloseTimeout != nil {
			ts.onStartToCloseTimeout(t.TaskID)
		}
	case TimeoutHeartbeat:
		if ts.onHeartbeatTimeout != nil {
			ts.onHeartbeatTimeout(t.TaskID)
		}
	case TimeoutScheduleToClose:
		if ts.onScheduleToCloseTimeout != nil {
			ts.onScheduleToCloseTimeout(t.TaskID)
		}
	}
}

// Stop gracefully shuts down the timer sequence.
// Safe to call multiple times.
func (ts *TimerSequence) Stop() {
	ts.stopOnce.Do(func() {
		close(ts.stopChan)
		ts.wg.Wait()

		// Cancel all remaining timers
		ts.mu.Lock()
		defer ts.mu.Unlock()
		for _, info := range ts.timers {
			ts.cancelAllTimersLocked(info)
		}
	})
}

// GetStats returns timer statistics
func (ts *TimerSequence) GetStats() (int, int, int, int) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	var scheduled, running, heartbeat, overall int
	for _, info := range ts.timers {
		for t := range info.Timeouts {
			switch t {
			case TimeoutScheduleToStart:
				scheduled++
			case TimeoutStartToClose:
				running++
			case TimeoutHeartbeat:
				heartbeat++
			case TimeoutScheduleToClose:
				overall++
			}
		}
	}
	return scheduled, running, heartbeat, overall
}
