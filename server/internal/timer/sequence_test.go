package timer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// itoa converts an integer to a string safely
func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

func TestTimerSequence_CreateTimers(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	// Create timers for a queued task
	taskID := "test-task-1"
	createdAt := time.Now()
	config := TimeoutConfig{
		ScheduleToStart: 5 * time.Second,
		StartToClose:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}

	ts.CreateTimers(taskID, createdAt, config, "pending")

	// Verify state is tracked
	state, ok := ts.GetState(taskID)
	if !ok {
		t.Fatal("Expected task to be tracked")
	}
	if state != "pending" {
		t.Errorf("Expected state pending, got %s", state)
	}

	// Get stats and verify timers were created
	scheduled, _, heartbeat, overall := ts.GetStats()
	if scheduled != 1 {
		t.Errorf("Expected 1 ScheduleToStart timer, got %d", scheduled)
	}
	if heartbeat != 0 {
		// Heartbeat is not created for pending state
		t.Errorf("Expected 0 Heartbeat timers for pending, got %d", heartbeat)
	}
	if overall != 1 {
		t.Errorf("Expected 1 ScheduleToClose timer, got %d", overall)
	}
}

func TestTimerSequence_StateTransition(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	taskID := "test-task-2"
	now := time.Now()
	config := TimeoutConfig{
		ScheduleToStart: 5 * time.Second,
		StartToClose:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}

	// Start in pending state
	ts.CreateTimers(taskID, now, config, "pending")

	state, _ := ts.GetState(taskID)
	if state != "pending" {
		t.Errorf("Expected pending, got %s", state)
	}

	// Transition to running
	startedAt := now.Add(6 * time.Second) // After ScheduleToStart would have fired
	ts.UpdateTaskState(taskID, "running", &startedAt, config)

	state, _ = ts.GetState(taskID)
	if state != "running" {
		t.Errorf("Expected running, got %s", state)
	}

	// Verify timers changed
	_, running, heartbeat, _ := ts.GetStats()
	if running != 1 {
		t.Errorf("Expected 1 StartToClose timer for running, got %d", running)
	}
	if heartbeat != 1 {
		t.Errorf("Expected 1 Heartbeat timer for running, got %d", heartbeat)
	}
}

func TestTimerSequence_Remove(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	taskID := "test-task-3"
	ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
		ScheduleToStart: 5 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}, "pending")

	// Remove task timers
	ts.Remove(taskID)

	// Verify state is gone
	_, ok := ts.GetState(taskID)
	if ok {
		t.Error("Expected task to be removed")
	}

	// Verify stats are zero
	scheduled, _, _, overall := ts.GetStats()
	if scheduled != 0 || overall != 0 {
		t.Errorf("Expected all timers to be removed, got scheduled=%d overall=%d", scheduled, overall)
	}
}

func TestTimerSequence_Concurrency(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	var wg sync.WaitGroup
	numTasks := 100

	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(taskNum int) {
			defer wg.Done()
			taskID := fmt.Sprintf("test-task-concurrent-%d", taskNum)
			ts.CreateTimers(taskID, time.Now(), TimeoutConfig{
				ScheduleToStart: time.Duration(taskNum%10+1) * time.Second,
				ScheduleToClose: 60 * time.Second,
			}, "pending")
		}(i)
	}

	wg.Wait()

	// Verify all tasks are tracked
	scheduled, _, _, overall := ts.GetStats()
	if scheduled != numTasks || overall != numTasks {
		t.Errorf("Expected %d timers each, got scheduled=%d overall=%d", numTasks, scheduled, overall)
	}
}

func TestTimerSequence_GetStats(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	// Create some tasks in different states
	now := time.Now()
	config := TimeoutConfig{
		ScheduleToStart: 5 * time.Second,
		StartToClose:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}

	// 3 queued tasks
	for i := 0; i < 3; i++ {
		ts.CreateTimers("queued-"+itoa(i), now, config, "pending")
	}

	// 2 running tasks
	for i := 0; i < 2; i++ {
		started := now.Add(6 * time.Second)
		ts.UpdateTaskState("running-"+itoa(i), "running", &started, config)
	}

	scheduled, running, heartbeat, overall := ts.GetStats()

	if scheduled != 3 {
		t.Errorf("Expected 3 ScheduleToStart timers, got %d", scheduled)
	}
	if running != 2 {
		t.Errorf("Expected 2 StartToClose timers, got %d", running)
	}
	if heartbeat != 2 {
		t.Errorf("Expected 2 Heartbeat timers, got %d", heartbeat)
	}
	if overall != 5 {
		t.Errorf("Expected 5 ScheduleToClose timers, got %d", overall)
	}
}

func TestTimeoutTypes(t *testing.T) {
	// Verify timeout type constants use snake_case (matching DB storage)
	if TimeoutScheduleToStart != "schedule_to_start" {
		t.Errorf("Expected schedule_to_start, got %s", TimeoutScheduleToStart)
	}
	if TimeoutStartToClose != "start_to_close" {
		t.Errorf("Expected start_to_close, got %s", TimeoutStartToClose)
	}
	if TimeoutHeartbeat != "heartbeat" {
		t.Errorf("Expected heartbeat, got %s", TimeoutHeartbeat)
	}
	if TimeoutScheduleToClose != "schedule_to_close" {
		t.Errorf("Expected schedule_to_close, got %s", TimeoutScheduleToClose)
	}
}
