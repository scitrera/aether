package timer

import (
	"testing"
	"time"

	"github.com/scitrera/aether/pkg/tasks"
)

// Test helper types

type mockTaskStore struct {
	tasks map[string]*tasks.TaskRecord
}

func newMockTaskStore() *mockTaskStore {
	return &mockTaskStore{
		tasks: make(map[string]*tasks.TaskRecord),
	}
}

func (m *mockTaskStore) GetTask(taskID string) (*tasks.TaskRecord, error) {
	task, ok := m.tasks[taskID]
	if !ok {
		return nil, nil
	}
	copy := *task
	return &copy, nil
}

func (m *mockTaskStore) addTask(task *tasks.TaskRecord) {
	m.tasks[task.TaskID] = task
}

// Tests

func TestTimeoutHandler_StartTaskTimers(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	h := NewTimeoutHandler(nil, ts, func(taskID string, delay time.Duration) {})

	// Create a task
	now := time.Now()
	task := &tasks.TaskRecord{
		TaskID:             "test-task-1",
		Status:             tasks.TaskStatusPending,
		CreatedAt:          now,
		ScheduleToStartMs:  5000,
		StartToCloseMs:     30000,
		HeartbeatTimeoutMs: 10000,
		ScheduleToCloseMs:  60000,
	}

	h.StartTaskTimers(task)

	// Verify timers were created
	state, ok := ts.GetState("test-task-1")
	if !ok {
		t.Fatal("Expected task to be tracked by timer sequence")
	}
	if state != "pending" {
		t.Errorf("Expected state pending, got %s", state)
	}

	scheduled, _, _, overall := ts.GetStats()
	if scheduled != 1 {
		t.Errorf("Expected 1 ScheduleToStart timer, got %d", scheduled)
	}
	if overall != 1 {
		t.Errorf("Expected 1 ScheduleToClose timer, got %d", overall)
	}
}

func TestTimeoutHandler_UpdateTaskExecution(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	h := NewTimeoutHandler(nil, ts, func(taskID string, delay time.Duration) {})

	now := time.Now()
	task := &tasks.TaskRecord{
		TaskID:             "test-task-2",
		Status:             tasks.TaskStatusPending,
		CreatedAt:          now,
		ScheduleToStartMs:  5000,
		StartToCloseMs:     30000,
		HeartbeatTimeoutMs: 10000,
		ScheduleToCloseMs:  60000,
	}

	// Start with QUEUED state
	h.StartTaskTimers(task)

	// Verify QUEUED state has ScheduleToStart
	scheduled, _, _, _ := ts.GetStats()
	if scheduled != 1 {
		t.Errorf("Expected 1 ScheduleToStart timer, got %d", scheduled)
	}

	// Update to RUNNING state
	startedAt := now.Add(6 * time.Second)
	config := TimeoutConfig{
		ScheduleToStart: 5 * time.Second,
		StartToClose:    30 * time.Second,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Second,
	}
	h.UpdateTaskExecution("test-task-2", startedAt, config)

	// Verify state transition
	state, ok := ts.GetState("test-task-2")
	if !ok {
		t.Fatal("Expected task to still be tracked")
	}
	if state != "running" {
		t.Errorf("Expected state running, got %s", state)
	}

	// Verify timers updated
	_, running, heartbeat, _ := ts.GetStats()
	if running != 1 {
		t.Errorf("Expected 1 StartToClose timer, got %d", running)
	}
	if heartbeat != 1 {
		t.Errorf("Expected 1 Heartbeat timer, got %d", heartbeat)
	}
}

func TestTimeoutHandler_CompleteTask(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	h := NewTimeoutHandler(nil, ts, func(taskID string, delay time.Duration) {})

	now := time.Now()
	task := &tasks.TaskRecord{
		TaskID:             "test-task-3",
		Status:             tasks.TaskStatusRunning,
		CreatedAt:          now,
		ScheduleToStartMs:  5000,
		StartToCloseMs:     30000,
		HeartbeatTimeoutMs: 10000,
		ScheduleToCloseMs:  60000,
	}

	h.StartTaskTimers(task)

	// Complete the task
	h.CompleteTask("test-task-3")

	// Verify timers were removed
	_, ok := ts.GetState("test-task-3")
	if ok {
		t.Error("Expected task to be removed from timer sequence")
	}
}

func TestTimeoutHandler_RecordHeartbeat(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	h := NewTimeoutHandler(nil, ts, func(taskID string, delay time.Duration) {})

	now := time.Now()
	task := &tasks.TaskRecord{
		TaskID:             "test-task-4",
		Status:             tasks.TaskStatusRunning,
		CreatedAt:          now,
		ScheduleToStartMs:  5000,
		StartToCloseMs:     30000,
		HeartbeatTimeoutMs: 5000, // 5 second heartbeat
		ScheduleToCloseMs:  60000,
	}

	h.StartTaskTimers(task)

	// Record heartbeat
	config := TimeoutConfig{
		Heartbeat: 5 * time.Second,
	}
	h.RecordHeartbeat("test-task-4", config)

	// Timer should still be active - we just reset it
	_, ok := ts.GetState("test-task-4")
	if !ok {
		t.Error("Expected task to still be tracked after heartbeat")
	}
}

func TestTimeoutHandler_FailTask(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	h := NewTimeoutHandler(nil, ts, func(taskID string, delay time.Duration) {})

	now := time.Now()
	task := &tasks.TaskRecord{
		TaskID:             "test-task-5",
		Status:             tasks.TaskStatusRunning,
		CreatedAt:          now,
		ScheduleToStartMs:  5000,
		StartToCloseMs:     30000,
		HeartbeatTimeoutMs: 10000,
		ScheduleToCloseMs:  60000,
	}

	h.StartTaskTimers(task)

	// Fail the task
	h.FailTask("test-task-5")

	// Verify timers were removed
	_, ok := ts.GetState("test-task-5")
	if ok {
		t.Error("Expected task to be removed from timer sequence after fail")
	}
}

func TestTimeoutHandler_GetStats(t *testing.T) {
	ts := NewTimerSequence()
	defer ts.Stop()

	h := NewTimeoutHandler(nil, ts, func(taskID string, delay time.Duration) {})

	now := time.Now()

	// Create tasks in different states
	queuedTask := &tasks.TaskRecord{
		TaskID:            "queued-task",
		Status:            tasks.TaskStatusPending,
		CreatedAt:         now,
		ScheduleToStartMs: 5000,
		StartToCloseMs:    30000,
		ScheduleToCloseMs: 60000,
	}
	h.StartTaskTimers(queuedTask)

	runningTask := &tasks.TaskRecord{
		TaskID:             "running-task",
		Status:             tasks.TaskStatusRunning,
		CreatedAt:          now,
		ScheduleToStartMs:  5000,
		StartToCloseMs:     30000,
		HeartbeatTimeoutMs: 10000,
		ScheduleToCloseMs:  60000,
	}
	h.StartTaskTimers(runningTask)

	// Stats:
	// - scheduled (ScheduleToStart): only queued-task gets it = 1
	// - running (StartToClose): only running-task gets it = 1
	// - heartbeat: only running-task gets it = 1
	// - overall (ScheduleToClose): both tasks get it = 2
	scheduled, running, heartbeat, overall := ts.GetStats()
	if scheduled != 1 {
		t.Errorf("Expected 1 ScheduleToStart timer (only queued), got %d", scheduled)
	}
	if running != 1 {
		t.Errorf("Expected 1 StartToClose timer (only running), got %d", running)
	}
	if heartbeat != 1 {
		t.Errorf("Expected 1 Heartbeat timer (only running), got %d", heartbeat)
	}
	if overall != 2 {
		t.Errorf("Expected 2 ScheduleToClose timers (both tasks), got %d", overall)
	}
}

func TestTimeoutConfig_Fields(t *testing.T) {
	config := TimeoutConfig{
		ScheduleToStart: 5 * time.Minute,
		StartToClose:    30 * time.Minute,
		Heartbeat:       10 * time.Second,
		ScheduleToClose: 60 * time.Minute,
	}

	if config.ScheduleToStart != 5*time.Minute {
		t.Errorf("Expected ScheduleToStart 5m, got %v", config.ScheduleToStart)
	}
	if config.StartToClose != 30*time.Minute {
		t.Errorf("Expected StartToClose 30m, got %v", config.StartToClose)
	}
	if config.Heartbeat != 10*time.Second {
		t.Errorf("Expected Heartbeat 10s, got %v", config.Heartbeat)
	}
	if config.ScheduleToClose != 60*time.Minute {
		t.Errorf("Expected ScheduleToClose 60m, got %v", config.ScheduleToClose)
	}
}

func TestTimer_Fields(t *testing.T) {
	expiry := time.Now().Add(5 * time.Second)
	cancelCalled := false
	cancel := func() {
		cancelCalled = true
	}

	timer := &Timer{
		Type:      TimeoutHeartbeat,
		Expiry:    expiry,
		TaskID:    "test-task",
		Cancel:    cancel,
		CreatedAt: time.Now(),
	}

	if timer.Type != TimeoutHeartbeat {
		t.Errorf("Expected Type TimeoutHeartbeat, got %s", timer.Type)
	}
	if timer.TaskID != "test-task" {
		t.Errorf("Expected TaskID test-task, got %s", timer.TaskID)
	}
	if timer.Cancel == nil {
		t.Error("Expected Cancel to be set")
	}

	// Test cancel function
	timer.Cancel()
	if !cancelCalled {
		t.Error("Expected Cancel to be called")
	}
}

func TestTimeoutInfo_Fields(t *testing.T) {
	now := time.Now()
	startedAt := now.Add(5 * time.Second)
	lastHeartbeat := now.Add(10 * time.Second)

	info := &TimeoutInfo{
		TaskID:        "test-task",
		Timeouts:      make(map[TimeoutType]*Timer),
		State:         "running",
		CreatedAt:     now,
		StartedAt:     &startedAt,
		LastHeartbeat: &lastHeartbeat,
	}

	if info.TaskID != "test-task" {
		t.Errorf("Expected TaskID test-task, got %s", info.TaskID)
	}
	if info.State != "running" {
		t.Errorf("Expected State running, got %s", info.State)
	}
	if info.StartedAt == nil {
		t.Error("Expected StartedAt to be set")
	}
	if info.LastHeartbeat == nil {
		t.Error("Expected LastHeartbeat to be set")
	}
}
