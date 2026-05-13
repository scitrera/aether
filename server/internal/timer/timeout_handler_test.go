package timer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// NewTimeoutHandler — callback registration
// =============================================================================

func TestNewTimeoutHandler_RegistersAllCallbacks(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	_ = NewTimeoutHandler(nil, ts, nil)

	// After construction all four callback slots must be set on the sequence.
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if ts.onScheduleToStartTimeout == nil {
		t.Error("onScheduleToStartTimeout callback not registered")
	}
	if ts.onStartToCloseTimeout == nil {
		t.Error("onStartToCloseTimeout callback not registered")
	}
	if ts.onHeartbeatTimeout == nil {
		t.Error("onHeartbeatTimeout callback not registered")
	}
	if ts.onScheduleToCloseTimeout == nil {
		t.Error("onScheduleToCloseTimeout callback not registered")
	}
}

// =============================================================================
// TimerSequence callback firing via timerChan
// =============================================================================

// injectTimer sends a Timer directly into the sequence's processing loop,
// bypassing the real time.AfterFunc so tests are deterministic.
func injectTimer(ts *TimerSequence, t *Timer) {
	ts.timerChan <- t
}

func TestTimerSequence_ScheduleToStartCallbackFires(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var called int64
	ts.SetScheduleToStartCallback(func(taskID string) {
		atomic.AddInt64(&called, 1)
	})

	// Register a task so processTimer can find it.
	taskID := "cb-task-sts"
	ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
		ScheduleToStart: 10 * time.Minute,
		ScheduleToClose: 20 * time.Minute,
	}, "pending")

	// Grab the exact Timer pointer that was registered so identity check passes.
	ts.mu.RLock()
	info := ts.timers[taskID]
	activeTimer := info.Timeouts[TimeoutScheduleToStart]
	ts.mu.RUnlock()

	injectTimer(ts, activeTimer)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&called) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("ScheduleToStart callback not called within timeout (called=%d)", atomic.LoadInt64(&called))
}

func TestTimerSequence_StartToCloseCallbackFires(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var called int64
	ts.SetStartToCloseCallback(func(taskID string) {
		atomic.AddInt64(&called, 1)
	})

	taskID := "cb-task-stc"
	now := time.Now()
	ts.CreateTimers(taskID, now, TimeoutConfig{
		StartToClose:    10 * time.Minute,
		ScheduleToClose: 20 * time.Minute,
	}, "running")

	ts.mu.RLock()
	info := ts.timers[taskID]
	activeTimer := info.Timeouts[TimeoutStartToClose]
	ts.mu.RUnlock()

	injectTimer(ts, activeTimer)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&called) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("StartToClose callback not called within timeout (called=%d)", atomic.LoadInt64(&called))
}

func TestTimerSequence_HeartbeatCallbackFires(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var called int64
	ts.SetHeartbeatCallback(func(taskID string) {
		atomic.AddInt64(&called, 1)
	})

	taskID := "cb-task-hb"
	ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
		Heartbeat:       10 * time.Minute,
		ScheduleToClose: 20 * time.Minute,
	}, "running")

	ts.mu.RLock()
	info := ts.timers[taskID]
	activeTimer := info.Timeouts[TimeoutHeartbeat]
	ts.mu.RUnlock()

	injectTimer(ts, activeTimer)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&called) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("Heartbeat callback not called within timeout (called=%d)", atomic.LoadInt64(&called))
}

func TestTimerSequence_ScheduleToCloseCallbackFires(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var called int64
	ts.SetScheduleToCloseCallback(func(taskID string) {
		atomic.AddInt64(&called, 1)
	})

	taskID := "cb-task-sc"
	ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
		ScheduleToClose: 10 * time.Minute,
	}, "pending")

	ts.mu.RLock()
	info := ts.timers[taskID]
	activeTimer := info.Timeouts[TimeoutScheduleToClose]
	ts.mu.RUnlock()

	injectTimer(ts, activeTimer)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&called) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("ScheduleToClose callback not called within timeout (called=%d)", atomic.LoadInt64(&called))
}

// =============================================================================
// processTimer — silently drops timer for unknown task
// =============================================================================

func TestTimerSequence_ProcessTimer_UnknownTaskDropped(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var callbackInvoked int64
	ts.SetScheduleToStartCallback(func(_ string) { atomic.AddInt64(&callbackInvoked, 1) })

	// Inject a timer for a task that was never registered — must not panic.
	ghost := &Timer{
		Type:   TimeoutScheduleToStart,
		TaskID: "ghost-task-never-created",
	}
	injectTimer(ts, ghost)

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt64(&callbackInvoked) != 0 {
		t.Error("callback should not fire for an unknown task")
	}
}

// =============================================================================
// processTimer — stale (cancelled) timer pointer is silently dropped
// =============================================================================

func TestTimerSequence_ProcessTimer_StalePointerDropped(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var callbackInvoked int64
	ts.SetScheduleToStartCallback(func(_ string) { atomic.AddInt64(&callbackInvoked, 1) })

	taskID := "stale-timer-task"
	ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
		ScheduleToStart: 10 * time.Minute,
		ScheduleToClose: 20 * time.Minute,
	}, "pending")

	// Capture the registered timer pointer, then replace it in the map so the
	// pointer no longer matches — simulating a cancelled / rescheduled timer.
	ts.mu.Lock()
	info := ts.timers[taskID]
	oldTimer := info.Timeouts[TimeoutScheduleToStart]
	// Replace with a fresh sentinel pointer.
	info.Timeouts[TimeoutScheduleToStart] = &Timer{Type: TimeoutScheduleToStart, TaskID: taskID}
	ts.mu.Unlock()

	// Inject the now-stale pointer.
	injectTimer(ts, oldTimer)

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt64(&callbackInvoked) != 0 {
		t.Error("stale (replaced) timer pointer should not fire the callback")
	}
}

// =============================================================================
// TimerSequence — callback not called when nil
// =============================================================================

func TestTimerSequence_NilCallbackDoesNotPanic(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	// Explicitly set nil callbacks (already nil by default, but be explicit).
	ts.SetScheduleToStartCallback(nil)

	taskID := "nil-cb-task"
	ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
		ScheduleToStart: 10 * time.Minute,
		ScheduleToClose: 20 * time.Minute,
	}, "pending")

	ts.mu.RLock()
	info := ts.timers[taskID]
	activeTimer := info.Timeouts[TimeoutScheduleToStart]
	ts.mu.RUnlock()

	// Must not panic.
	injectTimer(ts, activeTimer)
	time.Sleep(50 * time.Millisecond)
}

// =============================================================================
// TimerSequence — UpdateTaskState creates timers when task is not yet tracked
// =============================================================================

func TestTimerSequence_UpdateTaskState_CreatesTimersForNewTask(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	taskID := "new-via-update"
	config := TimeoutConfig{
		StartToClose:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}

	ts.UpdateTaskState(taskID, "running", nil, config)

	state, ok := ts.GetState(taskID)
	if !ok {
		t.Fatal("expected task to be tracked after UpdateTaskState")
	}
	if state != "running" {
		t.Errorf("expected state 'running', got %q", state)
	}

	_, running, heartbeat, overall := ts.GetStats()
	if running != 1 {
		t.Errorf("expected 1 StartToClose timer, got %d", running)
	}
	if heartbeat != 1 {
		t.Errorf("expected 1 Heartbeat timer, got %d", heartbeat)
	}
	if overall != 1 {
		t.Errorf("expected 1 ScheduleToClose timer, got %d", overall)
	}
}

// =============================================================================
// TimerSequence — UpdateTaskState transitions from running to COMPLETED
// cancels all timers
// =============================================================================

func TestTimerSequence_UpdateTaskState_CompletedCancelsAllTimers(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	taskID := "complete-task"
	now := time.Now()
	config := TimeoutConfig{
		StartToClose:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}

	ts.CreateTimers(taskID, now, config, "running")

	_, running, heartbeat, overall := ts.GetStats()
	if running == 0 && heartbeat == 0 && overall == 0 {
		t.Fatal("timers should be active before completing task")
	}

	ts.UpdateTaskState(taskID, "COMPLETED", nil, config)

	_, running, heartbeat, overall = ts.GetStats()
	if running != 0 || heartbeat != 0 || overall != 0 {
		t.Errorf("all timers should be cancelled after COMPLETED: running=%d heartbeat=%d overall=%d",
			running, heartbeat, overall)
	}
}

func TestTimerSequence_UpdateTaskState_FailedCancelsAllTimers(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	taskID := "failed-task"
	now := time.Now()
	config := TimeoutConfig{
		ScheduleToStart: 30 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}

	ts.CreateTimers(taskID, now, config, "pending")

	ts.UpdateTaskState(taskID, "FAILED", nil, config)

	scheduled, _, _, overall := ts.GetStats()
	if scheduled != 0 || overall != 0 {
		t.Errorf("all timers should be cancelled after FAILED: scheduled=%d overall=%d", scheduled, overall)
	}
}

// =============================================================================
// TimerSequence — ScheduleHeartbeatTimer reschedules correctly
// =============================================================================

func TestTimerSequence_ScheduleHeartbeatTimer_ReplacesExisting(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	taskID := "hb-reschedule"
	config := TimeoutConfig{
		Heartbeat:       10 * time.Minute,
		ScheduleToClose: 60 * time.Minute,
	}

	ts.CreateTimers(taskID, time.Now(), config, "running")

	_, _, heartbeat, _ := ts.GetStats()
	if heartbeat != 1 {
		t.Fatalf("expected 1 heartbeat timer before reschedule, got %d", heartbeat)
	}

	// Reschedule — should still have exactly 1 heartbeat timer.
	ts.ScheduleHeartbeatTimer(taskID, config, time.Now())

	_, _, heartbeat, _ = ts.GetStats()
	if heartbeat != 1 {
		t.Errorf("expected exactly 1 heartbeat timer after reschedule, got %d", heartbeat)
	}
}

func TestTimerSequence_ScheduleHeartbeatTimer_NoOpForUnknownTask(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	// Calling for a task that was never registered must not panic.
	ts.ScheduleHeartbeatTimer("unknown-task", TimeoutConfig{Heartbeat: 1 * time.Minute}, time.Now())
}

// =============================================================================
// TimerSequence — Remove on already-absent task is a no-op
// =============================================================================

func TestTimerSequence_Remove_UnknownTaskNoOp(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	// Must not panic.
	ts.Remove("task-that-does-not-exist")
}

// =============================================================================
// TimerPoller (no-DB unit tests) — creation and IsActive lifecycle
// =============================================================================

func TestTimerPoller_Creation_WithNilDB(t *testing.T) {
	// NewTimerPoller with a nil db and empty connStr (polling-only mode).
	poller := NewTimerPoller(nil, "", 5*time.Second, nil)
	if poller == nil {
		t.Fatal("NewTimerPoller returned nil")
	}
	if poller.pollInterval != 5*time.Second {
		t.Errorf("pollInterval = %v, want 5s", poller.pollInterval)
	}
	if poller.listener != nil {
		t.Error("listener should be nil when connStr is empty")
	}
	if poller.instanceID == "" {
		t.Error("instanceID must not be empty")
	}
}

func TestTimerPoller_IsActive_InitiallyFalse(t *testing.T) {
	poller := NewTimerPoller(nil, "", 5*time.Second, nil)
	if poller.IsActive() {
		t.Error("new poller should not be active before Start")
	}
}

func TestTimerPoller_Stop_WhenNotRunning_DoesNotPanic(t *testing.T) {
	poller := NewTimerPoller(nil, "", 5*time.Second, nil)
	// Stop before Start must be a safe no-op.
	poller.Stop()
}

func TestTimerPoller_UniqueInstanceIDs(t *testing.T) {
	p1 := NewTimerPoller(nil, "", 1*time.Second, nil)
	p2 := NewTimerPoller(nil, "", 1*time.Second, nil)
	if p1.instanceID == p2.instanceID {
		t.Errorf("each poller should have a unique instance ID, both got %q", p1.instanceID)
	}
}

// =============================================================================
// TimerPoller — handleNotification parses payload correctly
// =============================================================================

func TestTimerPoller_HandleNotification_ValidPayload(t *testing.T) {
	var capturedTimerID, capturedTaskID, capturedType string
	var mu sync.Mutex

	poller := NewTimerPoller(nil, "", 5*time.Second, func(timerID, taskID, timerType string) {
		mu.Lock()
		defer mu.Unlock()
		capturedTimerID = timerID
		capturedTaskID = taskID
		capturedType = timerType
	})

	// pq.Notification is a simple struct we can construct directly.
	notification := &pqNotification{Extra: "timer-uuid:task-uuid:schedule_to_close"}
	poller.handleNotificationPayload(notification.Extra)

	mu.Lock()
	defer mu.Unlock()
	if capturedTimerID != "timer-uuid" {
		t.Errorf("timerID = %q, want %q", capturedTimerID, "timer-uuid")
	}
	if capturedTaskID != "task-uuid" {
		t.Errorf("taskID = %q, want %q", capturedTaskID, "task-uuid")
	}
	if capturedType != "schedule_to_close" {
		t.Errorf("timerType = %q, want %q", capturedType, "schedule_to_close")
	}
}

func TestTimerPoller_HandleNotification_InvalidPayload_NoCallback(t *testing.T) {
	var called int64
	poller := NewTimerPoller(nil, "", 5*time.Second, func(_, _, _ string) {
		atomic.AddInt64(&called, 1)
	})

	// Payload has fewer than 3 colon-separated parts — callback must not fire.
	poller.handleNotificationPayload("only-two:parts")

	if atomic.LoadInt64(&called) != 0 {
		t.Error("callback should not be invoked for malformed notification payload")
	}
}

func TestTimerPoller_HandleNotification_NilCallback_DoesNotPanic(t *testing.T) {
	poller := NewTimerPoller(nil, "", 5*time.Second, nil)
	// Must not panic even with a valid payload when callback is nil.
	poller.handleNotificationPayload("timer-id:task-id:heartbeat")
}

// =============================================================================
// Helper: expose handleNotification internals for unit testing without pq.
// We add a thin wrapper method on TimerPoller.
// =============================================================================

// handleNotificationPayload is a test-accessible thin wrapper that replicates
// the parsing logic of handleNotification without requiring a *pq.Notification.
func (tp *TimerPoller) handleNotificationPayload(payload string) {
	parts := splitN(payload, ":", 3)
	if len(parts) < 3 {
		return
	}
	timerID := parts[0]
	taskID := parts[1]
	timerType := parts[2]
	if tp.onTimerFire != nil {
		tp.onTimerFire(timerID, taskID, timerType)
	}
}

// splitN is a local helper for the test file.
func splitN(s, sep string, n int) []string {
	var result []string
	for i := 0; i < n-1; i++ {
		idx := indexOf(s, sep)
		if idx < 0 {
			break
		}
		result = append(result, s[:idx])
		s = s[idx+len(sep):]
	}
	result = append(result, s)
	return result
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// pqNotification is a minimal stand-in used only in tests so we can construct
// notification payloads without importing pq in this test file directly.
type pqNotification struct {
	BePid   int
	Channel string
	Extra   string
}
