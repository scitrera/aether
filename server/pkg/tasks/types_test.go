package tasks

import (
	"testing"
	"time"
)

func TestTaskStatus(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   string
	}{
		{TaskStatusPending, "pending"},
		{TaskStatusAssigned, "assigned"},
		{TaskStatusStarting, "starting"},
		{TaskStatusRunning, "running"},
		{TaskStatusCompleted, "completed"},
		{TaskStatusFailed, "failed"},
		{TaskStatusCancelled, "cancelled"},
		{TaskStatusDLQ, "dlq"},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if string(tt.status) != tt.want {
				t.Errorf("TaskStatus = %q, want %q", tt.status, tt.want)
			}
		})
	}
}

func TestTaskCategory(t *testing.T) {
	tests := []struct {
		category TaskCategory
		want     string
	}{
		{TaskCategoryRegular, "regular"},
		{TaskCategoryOrchestrated, "orchestrated"},
		{TaskCategorySystem, "system"},
	}

	for _, tt := range tests {
		t.Run(string(tt.category), func(t *testing.T) {
			if string(tt.category) != tt.want {
				t.Errorf("TaskCategory = %q, want %q", tt.category, tt.want)
			}
		})
	}
}

func TestAssignmentMode(t *testing.T) {
	tests := []struct {
		mode AssignmentMode
		want string
	}{
		{AssignmentModeSelfAssign, "self_assign"},
		{AssignmentModeTargeted, "targeted"},
		{AssignmentModePool, "pool"},
		{AssignmentModeBroadcast, "broadcast"},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			if string(tt.mode) != tt.want {
				t.Errorf("AssignmentMode = %q, want %q", tt.mode, tt.want)
			}
		})
	}
}

func TestTimerType(t *testing.T) {
	tests := []struct {
		timerType TimerType
		want      string
	}{
		{TimerTypeScheduleToStart, "schedule_to_start"},
		{TimerTypeStartToClose, "start_to_close"},
		{TimerTypeHeartbeat, "heartbeat"},
		{TimerTypeScheduleToClose, "schedule_to_close"},
		{TimerTypeRetry, "retry"},
	}

	for _, tt := range tests {
		t.Run(string(tt.timerType), func(t *testing.T) {
			if string(tt.timerType) != tt.want {
				t.Errorf("TimerType = %q, want %q", tt.timerType, tt.want)
			}
		})
	}
}

func TestEventTypeConstants(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"EventTypeCreated", EventTypeCreated, "created"},
		{"EventTypeAssigned", EventTypeAssigned, "assigned"},
		{"EventTypeStarting", EventTypeStarting, "starting"},
		{"EventTypeStarted", EventTypeStarted, "started"},
		{"EventTypeCompleted", EventTypeCompleted, "completed"},
		{"EventTypeFailed", EventTypeFailed, "failed"},
		{"EventTypeRetryScheduled", EventTypeRetryScheduled, "retry_scheduled"},
		{"EventTypeCheckpointed", EventTypeCheckpointed, "checkpointed"},
		{"EventTypeTimedOut", EventTypeTimedOut, "timed_out"},
		{"EventTypeMovedToDLQ", EventTypeMovedToDLQ, "moved_to_dlq"},
		{"EventTypeCancelled", EventTypeCancelled, "cancelled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, tt.value, tt.want)
			}
		})
	}
}

func TestBackwardsCompatibilityAliases(t *testing.T) {
	// Test that backwards compatibility aliases work correctly
	t.Run("TaskStateAliases", func(t *testing.T) {
		if TaskStateQueued != TaskStatusPending {
			t.Errorf("TaskStateQueued = %q, want %q", TaskStateQueued, TaskStatusPending)
		}
		if TaskStateDispatched != TaskStatusAssigned {
			t.Errorf("TaskStateDispatched = %q, want %q", TaskStateDispatched, TaskStatusAssigned)
		}
		if TaskStateStarting != TaskStatusStarting {
			t.Errorf("TaskStateStarting = %q, want %q", TaskStateStarting, TaskStatusStarting)
		}
		if TaskStateRunning != TaskStatusRunning {
			t.Errorf("TaskStateRunning = %q, want %q", TaskStateRunning, TaskStatusRunning)
		}
		if TaskStateCompleted != TaskStatusCompleted {
			t.Errorf("TaskStateCompleted = %q, want %q", TaskStateCompleted, TaskStatusCompleted)
		}
		if TaskStateFailed != TaskStatusFailed {
			t.Errorf("TaskStateFailed = %q, want %q", TaskStateFailed, TaskStatusFailed)
		}
		if TaskStateDLQ != TaskStatusDLQ {
			t.Errorf("TaskStateDLQ = %q, want %q", TaskStateDLQ, TaskStatusDLQ)
		}
	})

	t.Run("TypeAliases", func(t *testing.T) {
		// Verify ExtendedTask is alias for Task
		var extTask ExtendedTask = Task{TaskID: "test"}
		var task Task = extTask
		if task.TaskID != "test" {
			t.Error("ExtendedTask should be compatible with Task")
		}

		// Verify TaskRecord is alias for Task
		var taskRecord TaskRecord = Task{TaskID: "test2"}
		task = taskRecord
		if task.TaskID != "test2" {
			t.Error("TaskRecord should be compatible with Task")
		}
	})
}

func TestTask_DefaultValues(t *testing.T) {
	task := Task{}

	// Verify zero values
	if task.TaskID != "" {
		t.Errorf("TaskID zero value = %q, want empty", task.TaskID)
	}
	if task.Status != "" {
		t.Errorf("Status zero value = %q, want empty", task.Status)
	}
	if task.Priority != 0 {
		t.Errorf("Priority zero value = %d, want 0", task.Priority)
	}
	if task.RetryCount != 0 {
		t.Errorf("RetryCount zero value = %d, want 0", task.RetryCount)
	}
	if task.MaxRetries != 0 {
		t.Errorf("MaxRetries zero value = %d, want 0", task.MaxRetries)
	}
	if !task.CreatedAt.IsZero() {
		t.Errorf("CreatedAt zero value = %v, want zero", task.CreatedAt)
	}
}

func TestTask_WithValues(t *testing.T) {
	now := time.Now()
	scheduled := now.Add(time.Hour)

	task := Task{
		TaskID:         "test-task-123",
		TaskType:       "process",
		Workspace:      "production",
		Implementation: "worker",
		Specifier:      "instance-1",
		Status:         TaskStatusRunning,
		Priority:       10,
		CreatedAt:      now,
		ScheduledFor:   &scheduled,
		AssignedTo:     "ag::prod::worker::inst-1",
		AssignmentMode: AssignmentModeTargeted,
		TaskCategory:   TaskCategoryOrchestrated,
		RetryCount:     2,
		MaxRetries:     5,
		Payload:        []byte("test payload"),
		Metadata: map[string]interface{}{
			"key": "value",
		},
		ScheduleToStartMs:  30000,
		StartToCloseMs:     300000,
		HeartbeatTimeoutMs: 10000,
	}

	if task.TaskID != "test-task-123" {
		t.Errorf("TaskID = %q, want %q", task.TaskID, "test-task-123")
	}
	if task.TaskType != "process" {
		t.Errorf("TaskType = %q, want %q", task.TaskType, "process")
	}
	if task.Status != TaskStatusRunning {
		t.Errorf("Status = %q, want %q", task.Status, TaskStatusRunning)
	}
	if task.Priority != 10 {
		t.Errorf("Priority = %d, want 10", task.Priority)
	}
	if task.AssignmentMode != AssignmentModeTargeted {
		t.Errorf("AssignmentMode = %q, want %q", task.AssignmentMode, AssignmentModeTargeted)
	}
	if task.TaskCategory != TaskCategoryOrchestrated {
		t.Errorf("TaskCategory = %q, want %q", task.TaskCategory, TaskCategoryOrchestrated)
	}
	if task.ScheduledFor == nil || !task.ScheduledFor.Equal(scheduled) {
		t.Errorf("ScheduledFor = %v, want %v", task.ScheduledFor, scheduled)
	}
	if task.Metadata["key"] != "value" {
		t.Errorf("Metadata[key] = %v, want %q", task.Metadata["key"], "value")
	}
	if string(task.Payload) != "test payload" {
		t.Errorf("Payload = %q, want %q", string(task.Payload), "test payload")
	}
}

func TestTaskFilter(t *testing.T) {
	status := TaskStatusPending
	category := TaskCategoryRegular
	queuedForStartup := true

	filter := TaskFilter{
		Status:               &status,
		Workspace:            "production",
		TaskType:             "process",
		TaskCategory:         &category,
		AssignedTo:           "worker-1",
		TargetAgentID:        "ag::prod::worker::inst-1",
		TargetImplementation: "worker",
		QueuedForStartup:     &queuedForStartup,
		Limit:                100,
		Offset:               50,
	}

	if *filter.Status != TaskStatusPending {
		t.Errorf("Status = %v, want %v", *filter.Status, TaskStatusPending)
	}
	if filter.Workspace != "production" {
		t.Errorf("Workspace = %q, want %q", filter.Workspace, "production")
	}
	if *filter.TaskCategory != TaskCategoryRegular {
		t.Errorf("TaskCategory = %v, want %v", *filter.TaskCategory, TaskCategoryRegular)
	}
	if *filter.QueuedForStartup != true {
		t.Errorf("QueuedForStartup = %v, want true", *filter.QueuedForStartup)
	}
	if filter.Limit != 100 {
		t.Errorf("Limit = %d, want 100", filter.Limit)
	}
	if filter.Offset != 50 {
		t.Errorf("Offset = %d, want 50", filter.Offset)
	}
}

func TestTaskCounts(t *testing.T) {
	counts := TaskCounts{
		Total:     100,
		Pending:   20,
		Assigned:  15,
		Starting:  5,
		Running:   30,
		Completed: 25,
		Failed:    5,
	}

	// Verify values
	if counts.Total != 100 {
		t.Errorf("Total = %d, want 100", counts.Total)
	}
	if counts.Pending != 20 {
		t.Errorf("Pending = %d, want 20", counts.Pending)
	}

	// Verify sum makes sense (Total should approximately equal sum of statuses)
	sum := counts.Pending + counts.Assigned + counts.Starting +
		counts.Running + counts.Completed + counts.Failed
	if sum != counts.Total {
		t.Logf("Note: sum of statuses (%d) != Total (%d) - may include other statuses like cancelled/dlq",
			sum, counts.Total)
	}
}

func TestTimerRecord(t *testing.T) {
	now := time.Now()
	firesAt := now.Add(time.Hour)
	firedAt := now.Add(30 * time.Minute)

	timer := TimerRecord{
		TimerID:   "timer-123",
		TaskID:    "task-456",
		TimerType: TimerTypeHeartbeat,
		FiresAt:   firesAt,
		CreatedAt: now,
		Fired:     true,
		FiredAt:   &firedAt,
		Metadata: map[string]interface{}{
			"interval": "30s",
		},
	}

	if timer.TimerID != "timer-123" {
		t.Errorf("TimerID = %q, want %q", timer.TimerID, "timer-123")
	}
	if timer.TimerType != TimerTypeHeartbeat {
		t.Errorf("TimerType = %q, want %q", timer.TimerType, TimerTypeHeartbeat)
	}
	if !timer.Fired {
		t.Error("Fired = false, want true")
	}
	if timer.FiredAt == nil || !timer.FiredAt.Equal(firedAt) {
		t.Errorf("FiredAt = %v, want %v", timer.FiredAt, firedAt)
	}
}

func TestDLQRecord(t *testing.T) {
	now := time.Now()
	reprocessedAt := now.Add(time.Hour)

	dlq := DLQRecord{
		DLQMessageID:    "dlq-123",
		OriginalTaskID:  "task-456",
		Category:        "exhausted_retries",
		Workspace:       "production",
		OriginalPayload: []byte("original payload"),
		OriginalMeta: map[string]interface{}{
			"source": "test",
		},
		FailureReason: "max retries exceeded",
		FailureDetails: map[string]interface{}{
			"last_error": "connection timeout",
		},
		EnqueuedAt:      now,
		AttemptCount:    5,
		LastAttemptAt:   now,
		ReprocessedAt:   &reprocessedAt,
		Resolved:        true,
		ResolvedBy:      "admin",
		ResolutionNotes: "manually resolved after fix",
	}

	if dlq.DLQMessageID != "dlq-123" {
		t.Errorf("DLQMessageID = %q, want %q", dlq.DLQMessageID, "dlq-123")
	}
	if dlq.Category != "exhausted_retries" {
		t.Errorf("Category = %q, want %q", dlq.Category, "exhausted_retries")
	}
	if dlq.AttemptCount != 5 {
		t.Errorf("AttemptCount = %d, want 5", dlq.AttemptCount)
	}
	if !dlq.Resolved {
		t.Error("Resolved = false, want true")
	}
	if dlq.ResolvedBy != "admin" {
		t.Errorf("ResolvedBy = %q, want %q", dlq.ResolvedBy, "admin")
	}
}
