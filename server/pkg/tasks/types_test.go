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
		// Explicit type annotations below are the *point* of this test: they
		// verify at compile time that ExtendedTask and TaskRecord remain
		// alias-compatible with Task. Suppressing ST1023 here keeps the
		// type assertion visible to readers.
		//nolint:staticcheck // ST1023: explicit types are the test assertion
		var extTask ExtendedTask = Task{TaskID: "test"}
		//nolint:staticcheck // ST1023: explicit types are the test assertion
		var task Task = extTask
		if task.TaskID != "test" {
			t.Error("ExtendedTask should be compatible with Task")
		}

		// Verify TaskRecord is alias for Task
		//nolint:staticcheck // ST1023: explicit types are the test assertion
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

// =============================================================================
// Phase 1: paused-state helpers
// =============================================================================

func TestTaskStatus_Phase1Strings(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   string
	}{
		{TaskStatusWaitingInput, "waiting_input"},
		{TaskStatusWaitingAuthority, "waiting_authority"},
		{TaskStatusWaitingDependency, "waiting_dependency"},
		{TaskStatusHibernated, "hibernated"},
		{TaskStatusRejected, "rejected"},
	}
	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if string(tt.status) != tt.want {
				t.Errorf("TaskStatus = %q, want %q", tt.status, tt.want)
			}
		})
	}
}

func TestWaitReason_Strings(t *testing.T) {
	tests := []struct {
		reason WaitReason
		want   string
	}{
		{WaitReasonInput, "input"},
		{WaitReasonAuthority, "authority"},
		{WaitReasonDependency, "dependency"},
		{WaitReasonHibernation, "hibernation"},
	}
	for _, tt := range tests {
		t.Run(string(tt.reason), func(t *testing.T) {
			if string(tt.reason) != tt.want {
				t.Errorf("WaitReason = %q, want %q", tt.reason, tt.want)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	terminal := []TaskStatus{
		TaskStatusCompleted,
		TaskStatusFailed,
		TaskStatusCancelled,
		TaskStatusDLQ,
		TaskStatusRejected,
	}
	for _, s := range terminal {
		t.Run("terminal/"+string(s), func(t *testing.T) {
			if !IsTerminal(s) {
				t.Errorf("IsTerminal(%q) = false, want true", s)
			}
		})
	}

	nonTerminal := []TaskStatus{
		TaskStatusPending,
		TaskStatusAssigned,
		TaskStatusStarting,
		TaskStatusRunning,
		TaskStatusWaitingInput,
		TaskStatusWaitingAuthority,
		TaskStatusWaitingDependency,
		TaskStatusHibernated,
	}
	for _, s := range nonTerminal {
		t.Run("nonterminal/"+string(s), func(t *testing.T) {
			if IsTerminal(s) {
				t.Errorf("IsTerminal(%q) = true, want false", s)
			}
		})
	}
}

func TestIsWaiting(t *testing.T) {
	waiting := []TaskStatus{
		TaskStatusWaitingInput,
		TaskStatusWaitingAuthority,
		TaskStatusWaitingDependency,
		TaskStatusHibernated,
	}
	for _, s := range waiting {
		t.Run("waiting/"+string(s), func(t *testing.T) {
			if !IsWaiting(s) {
				t.Errorf("IsWaiting(%q) = false, want true", s)
			}
		})
	}

	notWaiting := []TaskStatus{
		TaskStatusPending,
		TaskStatusRunning,
		TaskStatusCompleted,
		TaskStatusFailed,
		TaskStatusRejected,
	}
	for _, s := range notWaiting {
		t.Run("notwaiting/"+string(s), func(t *testing.T) {
			if IsWaiting(s) {
				t.Errorf("IsWaiting(%q) = true, want false", s)
			}
		})
	}
}

func TestValidateTransition_Legal(t *testing.T) {
	legal := [][2]TaskStatus{
		// idempotent self-transitions
		{TaskStatusPending, TaskStatusPending},
		{TaskStatusRunning, TaskStatusRunning},
		// happy path
		{TaskStatusPending, TaskStatusAssigned},
		{TaskStatusAssigned, TaskStatusStarting},
		{TaskStatusStarting, TaskStatusRunning},
		{TaskStatusRunning, TaskStatusCompleted},
		// pause + resume
		{TaskStatusRunning, TaskStatusWaitingInput},
		{TaskStatusRunning, TaskStatusWaitingAuthority},
		{TaskStatusRunning, TaskStatusWaitingDependency},
		{TaskStatusRunning, TaskStatusHibernated},
		{TaskStatusWaitingInput, TaskStatusRunning},
		{TaskStatusWaitingAuthority, TaskStatusRunning},
		{TaskStatusWaitingDependency, TaskStatusRunning},
		{TaskStatusHibernated, TaskStatusRunning},
		// reject path
		{TaskStatusPending, TaskStatusRejected},
		{TaskStatusAssigned, TaskStatusRejected},
		{TaskStatusWaitingInput, TaskStatusRejected},
		// retry path
		{TaskStatusFailed, TaskStatusPending},
		{TaskStatusCancelled, TaskStatusPending},
		// cancel paths
		{TaskStatusRunning, TaskStatusCancelled},
		{TaskStatusWaitingDependency, TaskStatusCancelled},
		{TaskStatusHibernated, TaskStatusCancelled},
	}
	for _, p := range legal {
		from, to := p[0], p[1]
		t.Run(string(from)+"->"+string(to), func(t *testing.T) {
			if err := ValidateTransition(from, to); err != nil {
				t.Errorf("ValidateTransition(%q, %q) = %v, want nil", from, to, err)
			}
		})
	}
}

func TestValidateTransition_Illegal(t *testing.T) {
	illegal := [][2]TaskStatus{
		// terminal states cannot transition out
		{TaskStatusCompleted, TaskStatusRunning},
		{TaskStatusCompleted, TaskStatusFailed},
		{TaskStatusDLQ, TaskStatusPending},
		{TaskStatusRejected, TaskStatusRunning},
		{TaskStatusRejected, TaskStatusPending},
		// invalid jumps
		{TaskStatusPending, TaskStatusCompleted},
		{TaskStatusPending, TaskStatusRunning},
		{TaskStatusAssigned, TaskStatusCompleted},
		// can't go from waiting straight to completed
		{TaskStatusWaitingInput, TaskStatusCompleted},
		{TaskStatusHibernated, TaskStatusCompleted},
		// can't reject a running task (use fail)
		{TaskStatusRunning, TaskStatusRejected},
	}
	for _, p := range illegal {
		from, to := p[0], p[1]
		t.Run(string(from)+"->"+string(to), func(t *testing.T) {
			if err := ValidateTransition(from, to); err == nil {
				t.Errorf("ValidateTransition(%q, %q) = nil, want error", from, to)
			}
		})
	}
}

func TestWaitSpec_AllReasons(t *testing.T) {
	// Sanity: a WaitSpec can carry payload appropriate to each reason.
	specs := []WaitSpec{
		{Reason: WaitReasonInput, ExpectedPrincipal: "user::alice", InputMatch: map[string]string{"kind": "approval"}, TimeoutMs: 60_000},
		{Reason: WaitReasonAuthority, AuthorityRequestID: "ar-123", TimeoutMs: 30_000},
		{Reason: WaitReasonDependency, DependsOn: []string{"t-1", "t-2"}, WakeOnAny: true},
		{Reason: WaitReasonHibernation, ScheduledWakeUnixMs: time.Now().Add(time.Hour).UnixMilli()},
	}
	for _, s := range specs {
		t.Run(string(s.Reason), func(t *testing.T) {
			if s.Reason == "" {
				t.Fatal("WaitSpec.Reason should be set")
			}
		})
	}
}

func TestTaskFilter_Phase1Fields(t *testing.T) {
	f := TaskFilter{
		ContextID:       "session-42",
		ExcludeStatuses: []TaskStatus{TaskStatusCompleted, TaskStatusFailed, TaskStatusRejected},
	}
	if f.ContextID != "session-42" {
		t.Errorf("ContextID = %q, want %q", f.ContextID, "session-42")
	}
	if len(f.ExcludeStatuses) != 3 {
		t.Fatalf("len(ExcludeStatuses) = %d, want 3", len(f.ExcludeStatuses))
	}
}

func TestTask_Phase1Fields(t *testing.T) {
	now := time.Now()
	spec := &WaitSpec{Reason: WaitReasonDependency, DependsOn: []string{"t-1"}}
	task := Task{
		TaskID:    "t-42",
		Status:    TaskStatusWaitingDependency,
		WaitSpec:  spec,
		DependsOn: []string{"t-1"},
		ContextID: "ctx-7",
		PausedAt:  &now,
	}
	if task.ContextID != "ctx-7" {
		t.Errorf("ContextID = %q, want ctx-7", task.ContextID)
	}
	if task.WaitSpec == nil || task.WaitSpec.Reason != WaitReasonDependency {
		t.Errorf("WaitSpec not round-tripped, got %+v", task.WaitSpec)
	}
	if len(task.DependsOn) != 1 || task.DependsOn[0] != "t-1" {
		t.Errorf("DependsOn = %v, want [t-1]", task.DependsOn)
	}
	if task.PausedAt == nil || !task.PausedAt.Equal(now) {
		t.Errorf("PausedAt = %v, want %v", task.PausedAt, now)
	}
}
