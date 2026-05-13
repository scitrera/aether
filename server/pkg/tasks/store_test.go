package tasks

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/testutil"
)

// TestTaskStoreIntegration tests the task store against a real PostgreSQL instance
func TestTaskStoreIntegration(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return // Skip was called in SetupTestDB
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	store := NewTaskStore(testDB.DB)

	t.Run("CreateTask", func(t *testing.T) {
		task := &Task{
			TaskType:       "test-task",
			Workspace:      "test-workspace",
			Implementation: "test-impl",
			Specifier:      "test-spec",
			Priority:       5,
			MaxRetries:     3,
			Payload:        []byte("test payload"),
			Metadata: map[string]interface{}{
				"key": "value",
			},
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// TaskID should be generated
		if task.TaskID == "" {
			t.Error("CreateTask() should generate TaskID")
		}

		// Verify defaults were set
		if task.Status != TaskStatusPending {
			t.Errorf("CreateTask() Status = %q, want %q", task.Status, TaskStatusPending)
		}
		if task.AssignmentMode != AssignmentModeSelfAssign {
			t.Errorf("CreateTask() AssignmentMode = %q, want %q", task.AssignmentMode, AssignmentModeSelfAssign)
		}
		if task.TaskCategory != TaskCategoryRegular {
			t.Errorf("CreateTask() TaskCategory = %q, want %q", task.TaskCategory, TaskCategoryRegular)
		}
	})

	t.Run("CreateTaskWithExistingID", func(t *testing.T) {
		customID := uuid.New().String()
		task := &Task{
			TaskID:    customID,
			TaskType:  "test-task",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		if task.TaskID != customID {
			t.Errorf("CreateTask() should preserve custom TaskID, got %q, want %q", task.TaskID, customID)
		}
	})

	t.Run("GetTask", func(t *testing.T) {
		task := &Task{
			TaskType:       "get-test",
			Workspace:      "test-workspace",
			Implementation: "worker",
			Specifier:      "inst-1",
			Priority:       10,
			Payload:        []byte("test payload"),
			Metadata: map[string]interface{}{
				"source": "test",
			},
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved == nil {
			t.Fatal("GetTask() returned nil")
		}

		if retrieved.TaskID != task.TaskID {
			t.Errorf("GetTask() TaskID = %q, want %q", retrieved.TaskID, task.TaskID)
		}
		if retrieved.TaskType != "get-test" {
			t.Errorf("GetTask() TaskType = %q, want %q", retrieved.TaskType, "get-test")
		}
		if retrieved.Workspace != "test-workspace" {
			t.Errorf("GetTask() Workspace = %q, want %q", retrieved.Workspace, "test-workspace")
		}
		if retrieved.Priority != 10 {
			t.Errorf("GetTask() Priority = %d, want 10", retrieved.Priority)
		}
		if string(retrieved.Payload) != "test payload" {
			t.Errorf("GetTask() Payload = %q, want %q", string(retrieved.Payload), "test payload")
		}
	})

	t.Run("GetTask_NotFound", func(t *testing.T) {
		_, err := store.GetTask(ctx, "nonexistent-task-id")
		if err == nil {
			t.Error("GetTask() should return error for nonexistent task")
		}
	})

	t.Run("UpdateTaskStatus", func(t *testing.T) {
		task := &Task{
			TaskType:  "status-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Update status
		err = store.UpdateTaskStatus(ctx, task.TaskID, TaskStatusCompleted)
		if err != nil {
			t.Fatalf("UpdateTaskStatus() error = %v", err)
		}

		// Verify update
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Status != TaskStatusCompleted {
			t.Errorf("GetTask() Status = %q, want %q", retrieved.Status, TaskStatusCompleted)
		}
	})

	t.Run("AssignTask", func(t *testing.T) {
		task := &Task{
			TaskType:  "assign-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Assign task
		workerID := "ag::test::worker::inst-1"
		err = store.AssignTask(ctx, task.TaskID, workerID)
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		// Verify assignment
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Status != TaskStatusAssigned {
			t.Errorf("AssignTask() Status = %q, want %q", retrieved.Status, TaskStatusAssigned)
		}
		if retrieved.AssignedTo != workerID {
			t.Errorf("AssignTask() AssignedTo = %q, want %q", retrieved.AssignedTo, workerID)
		}
		if retrieved.AssignedAt == nil {
			t.Error("AssignTask() AssignedAt should be set")
		}
	})

	t.Run("UpdateTaskMetadata", func(t *testing.T) {
		task := &Task{
			TaskType:  "metadata-update",
			Workspace: "test-workspace",
			Metadata: map[string]interface{}{
				"original": "value",
			},
		}

		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		updatedMetadata := map[string]interface{}{
			"root_authority_grant_id": "grant-root",
			"authority_grant_id":      "grant-child",
		}
		if err := store.UpdateTaskMetadata(ctx, task.TaskID, updatedMetadata); err != nil {
			t.Fatalf("UpdateTaskMetadata() error = %v", err)
		}

		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if got, _ := retrieved.Metadata["root_authority_grant_id"].(string); got != "grant-root" {
			t.Errorf("GetTask() Metadata[root_authority_grant_id] = %q, want %q", got, "grant-root")
		}
		if got, _ := retrieved.Metadata["authority_grant_id"].(string); got != "grant-child" {
			t.Errorf("GetTask() Metadata[authority_grant_id] = %q, want %q", got, "grant-child")
		}
	})

	t.Run("UpdateTaskAuthority", func(t *testing.T) {
		task := &Task{
			TaskType:  "authority-update",
			Workspace: "test-workspace",
		}

		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		authority := TaskAuthorityInfo{
			Mode:                   "on_behalf_of",
			SubjectType:            "User",
			SubjectID:              "alice",
			RootSubjectType:        "User",
			RootSubjectID:          "alice",
			AuthorityGrantID:       uuid.New().String(),
			RootAuthorityGrantID:   uuid.New().String(),
			ParentAuthorityGrantID: uuid.New().String(),
			AudienceType:           "agent",
			AudienceID:             "ag::test::agent::worker-1",
			DelegateType:           "Agent",
			DelegateID:             "ag::test::agent::worker-1",
		}
		metadata := map[string]interface{}{
			"authority_grant_id": authority.AuthorityGrantID,
		}

		if err := store.UpdateTaskAuthority(ctx, task.TaskID, authority, metadata); err != nil {
			t.Fatalf("UpdateTaskAuthority() error = %v", err)
		}

		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Authority.Mode != authority.Mode {
			t.Errorf("Authority.Mode = %q, want %q", retrieved.Authority.Mode, authority.Mode)
		}
		if retrieved.Authority.RootAuthorityGrantID != authority.RootAuthorityGrantID {
			t.Errorf("Authority.RootAuthorityGrantID = %q, want %q", retrieved.Authority.RootAuthorityGrantID, authority.RootAuthorityGrantID)
		}
		if retrieved.Authority.AudienceID != authority.AudienceID {
			t.Errorf("Authority.AudienceID = %q, want %q", retrieved.Authority.AudienceID, authority.AudienceID)
		}
	})

	t.Run("AssignTask_NotPending", func(t *testing.T) {
		task := &Task{
			TaskType:  "assign-fail-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Assign first time (should succeed)
		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("First AssignTask() error = %v", err)
		}

		// Try to assign again (should fail - not pending)
		err = store.AssignTask(ctx, task.TaskID, "worker-2")
		if err == nil {
			t.Error("Second AssignTask() should fail for non-pending task")
		}
	})

	t.Run("StartingTask", func(t *testing.T) {
		task := &Task{
			TaskType:  "starting-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Assign first
		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		// Mark as starting
		err = store.StartingTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartingTask() error = %v", err)
		}

		// Verify status
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Status != TaskStatusStarting {
			t.Errorf("StartingTask() Status = %q, want %q", retrieved.Status, TaskStatusStarting)
		}
	})

	t.Run("StartTask", func(t *testing.T) {
		task := &Task{
			TaskType:  "start-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Assign first
		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		// Start task
		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartTask() error = %v", err)
		}

		// Verify status and started_at
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Status != TaskStatusRunning {
			t.Errorf("StartTask() Status = %q, want %q", retrieved.Status, TaskStatusRunning)
		}
		if retrieved.StartedAt == nil {
			t.Error("StartTask() StartedAt should be set")
		}
	})

	t.Run("StartTask_Idempotent", func(t *testing.T) {
		task := &Task{
			TaskType:  "start-idempotent-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		// Start task first time
		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("First StartTask() error = %v", err)
		}

		// Get the first started_at time
		first, _ := store.GetTask(ctx, task.TaskID)
		firstStartedAt := first.StartedAt

		// Start task again (should succeed and not change started_at)
		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("Second StartTask() error = %v", err)
		}

		// Verify started_at wasn't changed
		second, _ := store.GetTask(ctx, task.TaskID)
		if !second.StartedAt.Equal(*firstStartedAt) {
			t.Error("StartTask() should be idempotent and not change started_at")
		}
	})

	t.Run("CompleteTask", func(t *testing.T) {
		task := &Task{
			TaskType:  "complete-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartTask() error = %v", err)
		}

		// Complete task
		err = store.CompleteTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("CompleteTask() error = %v", err)
		}

		// Verify status and completed_at
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Status != TaskStatusCompleted {
			t.Errorf("CompleteTask() Status = %q, want %q", retrieved.Status, TaskStatusCompleted)
		}
		if retrieved.CompletedAt == nil {
			t.Error("CompleteTask() CompletedAt should be set")
		}
	})

	t.Run("FailTask", func(t *testing.T) {
		task := &Task{
			TaskType:  "fail-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartTask() error = %v", err)
		}

		// Fail task
		err = store.FailTask(ctx, task.TaskID, "connection timeout after 30s")
		if err != nil {
			t.Fatalf("FailTask() error = %v", err)
		}

		// Verify status and error fields
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.Status != TaskStatusFailed {
			t.Errorf("FailTask() Status = %q, want %q", retrieved.Status, TaskStatusFailed)
		}
		if retrieved.FailedAt == nil {
			t.Error("FailTask() FailedAt should be set")
		}
		if retrieved.ErrorMessage != "connection timeout after 30s" {
			t.Errorf("FailTask() ErrorMessage = %q, want %q", retrieved.ErrorMessage, "connection timeout after 30s")
		}
	})

	t.Run("ListTasks", func(t *testing.T) {
		// Create multiple tasks
		for i := 0; i < 5; i++ {
			task := &Task{
				TaskType:  "list-test",
				Workspace: "list-workspace",
			}
			err := store.CreateTask(ctx, task)
			if err != nil {
				t.Fatalf("CreateTask() error = %v", err)
			}
		}

		// List tasks
		filter := TaskFilter{
			Workspace: "list-workspace",
			Limit:     10,
		}
		tasks, err := store.ListTasks(ctx, &filter)
		if err != nil {
			t.Fatalf("ListTasks() error = %v", err)
		}

		if len(tasks) < 5 {
			t.Errorf("ListTasks() returned %d tasks, want >= 5", len(tasks))
		}
	})

	t.Run("ListTasks_ByStatus", func(t *testing.T) {
		// Create a completed task
		task := &Task{
			TaskType:  "list-status-test",
			Workspace: "status-workspace",
		}
		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartTask() error = %v", err)
		}

		err = store.CompleteTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("CompleteTask() error = %v", err)
		}

		// List completed tasks
		status := TaskStatusCompleted
		filter := TaskFilter{
			Status:    &status,
			Workspace: "status-workspace",
			Limit:     10,
		}
		tasks, err := store.ListTasks(ctx, &filter)
		if err != nil {
			t.Fatalf("ListTasks() error = %v", err)
		}

		// All returned tasks should be completed
		for _, task := range tasks {
			if task.Status != TaskStatusCompleted {
				t.Errorf("ListTasks() returned task with status %q, want %q", task.Status, TaskStatusCompleted)
			}
		}
	})

	t.Run("UpdateHeartbeat", func(t *testing.T) {
		task := &Task{
			TaskType:  "heartbeat-test",
			Workspace: "test-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartTask() error = %v", err)
		}

		// Update heartbeat
		details := map[string]interface{}{
			"progress": 50,
			"message":  "halfway done",
		}
		err = store.UpdateHeartbeat(ctx, task.TaskID, details)
		if err != nil {
			t.Fatalf("UpdateTaskHeartbeat() error = %v", err)
		}

		// Verify heartbeat
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.LastHeartbeat == nil {
			t.Error("UpdateTaskHeartbeat() LastHeartbeat should be set")
		}
		if retrieved.HeartbeatDetails["progress"] != float64(50) {
			t.Errorf("UpdateTaskHeartbeat() progress = %v, want 50", retrieved.HeartbeatDetails["progress"])
		}
	})

	t.Run("PurgeOldTasks", func(t *testing.T) {
		// Create a completed task with old timestamp
		task := &Task{
			TaskType:  "purge-test",
			Workspace: "purge-workspace",
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		err = store.AssignTask(ctx, task.TaskID, "worker-1")
		if err != nil {
			t.Fatalf("AssignTask() error = %v", err)
		}

		err = store.StartTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("StartTask() error = %v", err)
		}

		err = store.CompleteTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("CompleteTask() error = %v", err)
		}

		// PurgeOldTasks with 0 retention should delete it
		result, err := store.PurgeOldTasks(ctx, 0, 0, 0)
		if err != nil {
			t.Fatalf("PurgeOldTasks() error = %v", err)
		}

		t.Logf("PurgeOldTasks deleted %d completed, %d failed, %d cancelled tasks",
			result.Completed, result.Failed, result.Cancelled)

		// Task should be gone
		_, err = store.GetTask(ctx, task.TaskID)
		if err == nil {
			t.Error("PurgeOldTasks() should have deleted the task")
		}
	})

	t.Run("WriteToDLQ", func(t *testing.T) {
		// Create a task that will be moved to DLQ
		task := &Task{
			TaskType:  "dlq-test",
			Workspace: "dlq-workspace",
			Payload:   []byte("test payload for DLQ"),
			Metadata: map[string]interface{}{
				"source": "test",
			},
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Create DLQ record
		dlqRecord := &DLQRecord{
			OriginalTaskID:  task.TaskID,
			Category:        "exhausted_retries",
			Workspace:       task.Workspace,
			OriginalPayload: task.Payload,
			OriginalMeta: map[string]interface{}{
				"source": "test",
			},
			FailureReason: "Maximum retries exceeded",
			FailureDetails: map[string]interface{}{
				"retry_count": 3,
				"last_error":  "connection timeout",
			},
			AttemptCount:  3,
			LastAttemptAt: task.CreatedAt,
		}

		// Write to DLQ
		err = store.WriteToDLQ(ctx, dlqRecord)
		if err != nil {
			t.Fatalf("WriteToDLQ() error = %v", err)
		}

		// Verify DLQ record was created
		if dlqRecord.DLQMessageID == "" {
			t.Error("WriteToDLQ() should generate DLQMessageID")
		}
		if dlqRecord.EnqueuedAt.IsZero() {
			t.Error("WriteToDLQ() should set EnqueuedAt")
		}

		// Verify the record exists in the database
		var count int
		err = testDB.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dlq WHERE dlq_message_id = $1", dlqRecord.DLQMessageID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query DLQ table: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 DLQ record, got %d", count)
		}
	})

	t.Run("GetDLQTasks", func(t *testing.T) {
		// Create a task that will be moved to DLQ
		task := &Task{
			TaskType:  "dlq-query-test",
			Workspace: "dlq-query-workspace",
			Payload:   []byte("test payload for DLQ query"),
			Metadata: map[string]interface{}{
				"source": "query-test",
			},
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		// Create DLQ record
		dlqRecord := &DLQRecord{
			OriginalTaskID:  task.TaskID,
			Category:        "exhausted_retries",
			Workspace:       task.Workspace,
			OriginalPayload: task.Payload,
			OriginalMeta: map[string]interface{}{
				"source": "query-test",
			},
			FailureReason: "Maximum retries exceeded",
			FailureDetails: map[string]interface{}{
				"retry_count": 3,
				"last_error":  "connection timeout",
			},
			AttemptCount:  3,
			LastAttemptAt: task.CreatedAt,
		}

		// Write to DLQ
		err = store.WriteToDLQ(ctx, dlqRecord)
		if err != nil {
			t.Fatalf("WriteToDLQ() error = %v", err)
		}

		// Query DLQ records
		records, err := store.GetDLQTasks(ctx, "dlq-query-workspace", "", 10, 0)
		if err != nil {
			t.Fatalf("GetDLQTasks() error = %v", err)
		}

		// Verify we got at least one record
		if len(records) == 0 {
			t.Error("GetDLQTasks() returned no records, expected at least 1")
		}

		// Find our record
		var found *DLQRecord
		for _, r := range records {
			if r.DLQMessageID == dlqRecord.DLQMessageID {
				found = r
				break
			}
		}

		if found == nil {
			t.Error("GetDLQTasks() did not return the expected DLQ record")
		} else {
			// Verify record fields
			if found.OriginalTaskID != task.TaskID {
				t.Errorf("GetDLQTasks() OriginalTaskID = %q, want %q", found.OriginalTaskID, task.TaskID)
			}
			if found.Category != "exhausted_retries" {
				t.Errorf("GetDLQTasks() Category = %q, want %q", found.Category, "exhausted_retries")
			}
			if found.Workspace != "dlq-query-workspace" {
				t.Errorf("GetDLQTasks() Workspace = %q, want %q", found.Workspace, "dlq-query-workspace")
			}
			if found.FailureReason != "Maximum retries exceeded" {
				t.Errorf("GetDLQTasks() FailureReason = %q, want %q", found.FailureReason, "Maximum retries exceeded")
			}
			if string(found.OriginalPayload) != "test payload for DLQ query" {
				t.Errorf("GetDLQTasks() OriginalPayload = %q, want %q", string(found.OriginalPayload), "test payload for DLQ query")
			}
			if found.AttemptCount != 3 {
				t.Errorf("GetDLQTasks() AttemptCount = %d, want 3", found.AttemptCount)
			}
			// Verify OriginalMeta was unmarshaled correctly
			if found.OriginalMeta["source"] != "query-test" {
				t.Errorf("GetDLQTasks() OriginalMeta[source] = %v, want %q", found.OriginalMeta["source"], "query-test")
			}
			// Verify FailureDetails was unmarshaled correctly
			if found.FailureDetails["retry_count"] != float64(3) {
				t.Errorf("GetDLQTasks() FailureDetails[retry_count] = %v, want 3", found.FailureDetails["retry_count"])
			}
		}
	})

	t.Run("GetDLQTasks_FilterByCategory", func(t *testing.T) {
		// Create a task and DLQ record with specific category
		task := &Task{
			TaskType:  "dlq-filter-test",
			Workspace: "dlq-filter-workspace",
			Payload:   []byte("test payload"),
		}

		err := store.CreateTask(ctx, task)
		if err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}

		dlqRecord := &DLQRecord{
			OriginalTaskID:  task.TaskID,
			Category:        "timeout",
			Workspace:       task.Workspace,
			OriginalPayload: task.Payload,
			FailureReason:   "Task execution timeout",
			AttemptCount:    1,
			LastAttemptAt:   task.CreatedAt,
		}

		err = store.WriteToDLQ(ctx, dlqRecord)
		if err != nil {
			t.Fatalf("WriteToDLQ() error = %v", err)
		}

		// Query DLQ records filtered by category
		records, err := store.GetDLQTasks(ctx, "", "timeout", 10, 0)
		if err != nil {
			t.Fatalf("GetDLQTasks() error = %v", err)
		}

		// All returned records should have category 'timeout'
		for _, r := range records {
			if r.Category != "timeout" {
				t.Errorf("GetDLQTasks() returned record with category %q, want %q", r.Category, "timeout")
			}
		}
	})

	// Track A: parent_task_id is persisted as a first-class column and round-trips.
	t.Run("ParentTaskIDRoundTrip", func(t *testing.T) {
		parentID := uuid.New().String()
		task := &Task{
			TaskType:     "child-task",
			Workspace:    "test-workspace",
			ParentTaskID: parentID,
		}
		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask() error = %v", err)
		}
		retrieved, err := store.GetTask(ctx, task.TaskID)
		if err != nil {
			t.Fatalf("GetTask() error = %v", err)
		}
		if retrieved.ParentTaskID != parentID {
			t.Errorf("GetTask() ParentTaskID = %q, want %q", retrieved.ParentTaskID, parentID)
		}
	})

	// Track B: the new authority/lineage filters on TaskFilter actually filter in SQL.
	t.Run("FilterBySubjectAndAuthority", func(t *testing.T) {
		parentID := uuid.New().String()
		subjectAlice := Task{
			TaskType:  "worker",
			Workspace: "filter-ws-a",
			Authority: TaskAuthorityInfo{
				Mode:                 "on_behalf_of",
				SubjectType:          "user",
				SubjectID:            "alice",
				RootSubjectType:      "user",
				RootSubjectID:        "alice",
				AuthorityGrantID:     uuid.New().String(),
				RootAuthorityGrantID: uuid.New().String(),
			},
			ParentTaskID: parentID,
		}
		subjectBob := Task{
			TaskType:  "worker",
			Workspace: "filter-ws-a",
			Authority: TaskAuthorityInfo{
				Mode:                 "on_behalf_of",
				SubjectType:          "user",
				SubjectID:            "bob",
				AuthorityGrantID:     uuid.New().String(),
				RootAuthorityGrantID: uuid.New().String(),
			},
		}
		if err := store.CreateTask(ctx, &subjectAlice); err != nil {
			t.Fatalf("CreateTask(alice) error = %v", err)
		}
		if err := store.CreateTask(ctx, &subjectBob); err != nil {
			t.Fatalf("CreateTask(bob) error = %v", err)
		}

		aliceResults, err := store.ListTasks(ctx, &TaskFilter{
			Workspace: "filter-ws-a",
			SubjectID: "alice",
		})
		if err != nil {
			t.Fatalf("ListTasks(subject=alice) error = %v", err)
		}
		if len(aliceResults) != 1 {
			t.Fatalf("ListTasks(subject=alice) returned %d tasks, want 1", len(aliceResults))
		}
		if aliceResults[0].TaskID != subjectAlice.TaskID {
			t.Errorf("filter returned %q, want %q", aliceResults[0].TaskID, subjectAlice.TaskID)
		}

		grantResults, err := store.ListTasks(ctx, &TaskFilter{
			Workspace:        "filter-ws-a",
			AuthorityGrantID: subjectBob.Authority.AuthorityGrantID,
		})
		if err != nil {
			t.Fatalf("ListTasks(authority_grant_id=bob) error = %v", err)
		}
		if len(grantResults) != 1 || grantResults[0].TaskID != subjectBob.TaskID {
			t.Fatalf("filter by authority_grant_id returned wrong tasks: got %+v", grantResults)
		}

		modeResults, err := store.ListTasks(ctx, &TaskFilter{
			Workspace:     "filter-ws-a",
			AuthorityMode: "on_behalf_of",
		})
		if err != nil {
			t.Fatalf("ListTasks(authority_mode=on_behalf_of) error = %v", err)
		}
		if len(modeResults) < 2 {
			t.Errorf("filter by authority_mode returned %d tasks, want >= 2", len(modeResults))
		}

		parentResults, err := store.ListTasks(ctx, &TaskFilter{
			Workspace:    "filter-ws-a",
			ParentTaskID: parentID,
		})
		if err != nil {
			t.Fatalf("ListTasks(parent_task_id) error = %v", err)
		}
		if len(parentResults) != 1 || parentResults[0].TaskID != subjectAlice.TaskID {
			t.Fatalf("filter by parent_task_id returned wrong tasks: got %+v", parentResults)
		}
	})
}

// TestTaskStoreUnit tests without database
func TestTaskStoreUnit(t *testing.T) {
	t.Run("NewTaskStore", func(t *testing.T) {
		// Just verify it doesn't panic with nil
		store := NewTaskStore(nil)
		if store == nil {
			t.Error("NewTaskStore() returned nil")
		}
	})

	t.Run("TaskDefaults", func(t *testing.T) {
		task := &Task{}

		// Simulate what CreateTask does for defaults
		if task.TaskID == "" {
			task.TaskID = uuid.New().String()
		}
		if task.Status == "" {
			task.Status = TaskStatusPending
		}
		if task.AssignmentMode == "" {
			task.AssignmentMode = AssignmentModeSelfAssign
		}
		if task.TaskCategory == "" {
			task.TaskCategory = TaskCategoryRegular
		}
		if task.MaxRetries == 0 {
			task.MaxRetries = 3
		}

		if task.TaskID == "" {
			t.Error("TaskID should be generated")
		}
		if task.Status != TaskStatusPending {
			t.Errorf("Default Status = %q, want %q", task.Status, TaskStatusPending)
		}
		if task.AssignmentMode != AssignmentModeSelfAssign {
			t.Errorf("Default AssignmentMode = %q, want %q", task.AssignmentMode, AssignmentModeSelfAssign)
		}
		if task.TaskCategory != TaskCategoryRegular {
			t.Errorf("Default TaskCategory = %q, want %q", task.TaskCategory, TaskCategoryRegular)
		}
		if task.MaxRetries != 3 {
			t.Errorf("Default MaxRetries = %d, want 3", task.MaxRetries)
		}
	})
}
