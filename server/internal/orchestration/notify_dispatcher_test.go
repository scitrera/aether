package orchestration

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/tasks"
)

// TestUnclaimTaskRetryLimit verifies that UnclaimTask correctly implements retry limiting
func TestUnclaimTaskRetryLimit(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return // Skip was called in SetupTestDB
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	// Use isolated metrics registry for tests to avoid conflicts
	testMetrics := NewDispatcherMetricsWithRegistry(prometheus.NewRegistry())
	dispatcher, err := NewNotifyTaskDispatcher(testDB.DB, "", 0, nil, testMetrics)
	if err != nil {
		t.Fatalf("Failed to create dispatcher: %v", err)
	}

	t.Run("UnclaimTask increments retry_count", func(t *testing.T) {
		// Create a task in the queue
		queueID := uuid.New().String()
		taskID := uuid.New().String()
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 0, 3)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Unclaim the task
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() error = %v", err)
		}

		// Verify retry_count was incremented and status is pending
		var retryCount int
		var status string
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT retry_count, status FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&retryCount, &status)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}

		if retryCount != 1 {
			t.Errorf("Expected retry_count = 1, got %d", retryCount)
		}
		if status != "pending" {
			t.Errorf("Expected status = 'pending', got %q", status)
		}
	})

	t.Run("UnclaimTask fails task when max_retries exceeded", func(t *testing.T) {
		// Create a task in the tasks table first (required for FK constraint on audit events)
		queueID := uuid.New().String()
		taskID := uuid.New().String()
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 2, 3)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Unclaim the task (should send to DLQ instead)
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() error = %v", err)
		}

		// Verify task was marked as failed and sent to DLQ
		var retryCount int
		var status string
		var errorMsg sql.NullString
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT retry_count, status, error_message FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&retryCount, &status, &errorMsg)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}

		// retry_count stays at 2 because moveToDeadLetter doesn't increment it (following timer/handler.go:56 pattern)
		if retryCount != 2 {
			t.Errorf("Expected retry_count = 2, got %d", retryCount)
		}
		if status != "failed" {
			t.Errorf("Expected status = 'failed', got %q", status)
		}
		if !errorMsg.Valid || errorMsg.String != "Max retries exceeded - failed to deliver to orchestrator" {
			t.Errorf("Expected error_message = 'Max retries exceeded - failed to deliver to orchestrator', got %q", errorMsg.String)
		}

		// Verify task was sent to DLQ
		var dlqCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM dlq WHERE original_task_id = $1
		`, taskID).Scan(&dlqCount)
		if err != nil {
			t.Fatalf("Failed to query DLQ: %v", err)
		}
		if dlqCount != 1 {
			t.Errorf("Expected task to be in DLQ, found %d entries", dlqCount)
		}
	})

	t.Run("UnclaimTask respects custom max_retries", func(t *testing.T) {
		// Create a task with max_retries = 5
		queueID := uuid.New().String()
		taskID := uuid.New().String()
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 3, 5)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Unclaim the task (should still retry since 3 < 5-1)
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() error = %v", err)
		}

		// Verify task is still pending
		var status string
		var retryCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT retry_count, status FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&retryCount, &status)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}

		if retryCount != 4 {
			t.Errorf("Expected retry_count = 4, got %d", retryCount)
		}
		if status != "pending" {
			t.Errorf("Expected status = 'pending', got %q", status)
		}
	})
}

// TestRetryBackoff verifies that UnclaimTask implements exponential backoff
func TestRetryBackoff(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return // Skip was called in SetupTestDB
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	// Use isolated metrics registry for tests to avoid conflicts
	testMetrics := NewDispatcherMetricsWithRegistry(prometheus.NewRegistry())
	dispatcher, err := NewNotifyTaskDispatcher(testDB.DB, "", 0, nil, testMetrics)
	if err != nil {
		t.Fatalf("Failed to create dispatcher: %v", err)
	}

	t.Run("Exponential backoff increases with retry count", func(t *testing.T) {
		// Test that next_retry_at is set and increases exponentially
		testCases := []struct {
			retryCount int
		}{
			{0}, // Will become retry 1: backoff = 1 << 1 = 2 seconds
			{1}, // Will become retry 2: backoff = 1 << 2 = 4 seconds
			{2}, // Will become retry 3: backoff = 1 << 3 = 8 seconds
		}

		var previousNextRetry time.Time

		for i, tc := range testCases {
			queueID := uuid.New().String()
			taskID := uuid.New().String()

			// Create parent task (required for FK on audit events)
			_, err := testDB.DB.ExecContext(ctx, `
				INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
				VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
			`, taskID)
			if err != nil {
				t.Fatalf("Failed to insert task: %v", err)
			}

			// Create task with specific retry_count
			_, err = testDB.DB.ExecContext(ctx, `
				INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
				VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', $3, 5)
			`, queueID, taskID, tc.retryCount)
			if err != nil {
				t.Fatalf("Failed to insert test task: %v", err)
			}

			// Unclaim the task
			err = dispatcher.UnclaimTask(ctx, queueID)
			if err != nil {
				t.Fatalf("UnclaimTask() error = %v", err)
			}

			// Verify next_retry_at is set and retry_count incremented
			var nextRetryAt sql.NullTime
			var newRetryCount int
			err = testDB.DB.QueryRowContext(ctx, `
				SELECT next_retry_at, retry_count FROM orchestrated_task_queue WHERE queue_id = $1
			`, queueID).Scan(&nextRetryAt, &newRetryCount)
			if err != nil {
				t.Fatalf("Failed to query task: %v", err)
			}

			if !nextRetryAt.Valid {
				t.Errorf("Expected next_retry_at to be set for retry_count %d", tc.retryCount)
				continue
			}

			if newRetryCount != tc.retryCount+1 {
				t.Errorf("Expected retry_count to be %d, got %d", tc.retryCount+1, newRetryCount)
			}

			// Verify that next_retry_at increases (later retries have longer backoff)
			if i > 0 {
				// Each subsequent retry should have a later next_retry_at due to exponential backoff
				// (Note: We're not checking exact duration due to timezone complexity,
				//  just that it's increasing which proves exponential backoff is working)
				if !nextRetryAt.Time.After(previousNextRetry) {
					t.Errorf("Retry %d: next_retry_at should increase with higher retry counts (exponential backoff)", i)
				}
			}

			previousNextRetry = nextRetryAt.Time
		}
	})

	t.Run("PollPendingTasks respects next_retry_at", func(t *testing.T) {
		// Clean up any existing pending tasks first
		_, err := testDB.DB.ExecContext(ctx, `DELETE FROM orchestrated_task_queue WHERE status = 'pending'`)
		if err != nil {
			t.Fatalf("Failed to clean up tasks: %v", err)
		}

		// Create two tasks: one ready now, one with future next_retry_at
		readyQueueID := uuid.New().String()
		readyTaskID := uuid.New().String()
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, next_retry_at)
			VALUES ($1, $2, 'test-impl-ready', 'test-workspace', 'kubernetes', 'pending', NULL)
		`, readyQueueID, readyTaskID)
		if err != nil {
			t.Fatalf("Failed to insert ready task: %v", err)
		}

		futureQueueID := uuid.New().String()
		futureTaskID := uuid.New().String()
		futureRetryAt := time.Now().UTC().Add(10 * time.Minute)
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, next_retry_at)
			VALUES ($1, $2, 'test-impl-future', 'test-workspace', 'kubernetes', 'pending', $3)
		`, futureQueueID, futureTaskID, futureRetryAt)
		if err != nil {
			t.Fatalf("Failed to insert future task: %v", err)
		}

		// Verify tasks were inserted correctly
		var count int
		err = testDB.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM orchestrated_task_queue WHERE status = 'pending'`).Scan(&count)
		if err != nil || count != 2 {
			t.Fatalf("Expected 2 pending tasks, got %d (err: %v)", count, err)
		}

		// Track which tasks were received
		receivedTasks := make(map[string]bool)
		var mu sync.Mutex

		dispatcher.SetCallback(func(task *OrchestrationTaskNotification) {
			mu.Lock()
			receivedTasks[task.QueueID] = true
			mu.Unlock()
		})

		// Poll pending tasks
		dispatcher.pollPendingTasks(ctx)

		// Verify only the ready task was polled
		mu.Lock()
		defer mu.Unlock()

		if !receivedTasks[readyQueueID] {
			t.Errorf("Expected ready task %s to be polled", readyQueueID)
		}
		if receivedTasks[futureQueueID] {
			t.Errorf("Expected future task %s to NOT be polled (backoff not expired)", futureQueueID)
		}
		if len(receivedTasks) != 1 {
			t.Errorf("Expected exactly 1 task to be polled, got %d: %v", len(receivedTasks), receivedTasks)
		}
	})
}

// TestRetryAuditEvents verifies that audit events are created for retry and DLQ operations
func TestRetryAuditEvents(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return // Skip was called in SetupTestDB
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := tasks.NewTaskStore(testDB.DB)
	// Use isolated metrics registry for tests to avoid conflicts
	testMetrics := NewDispatcherMetricsWithRegistry(prometheus.NewRegistry())
	dispatcher, err := NewNotifyTaskDispatcher(testDB.DB, "", 0, nil, testMetrics)
	if err != nil {
		t.Fatalf("Failed to create dispatcher: %v", err)
	}

	t.Run("UnclaimTask creates retry audit event", func(t *testing.T) {
		// Create a task in the tasks table first (required for foreign key constraint)
		taskID := uuid.New().String()
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		// Create a task in the orchestrated queue
		queueID := uuid.New().String()
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 0, 3)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Unclaim the task to trigger retry
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() error = %v", err)
		}

		// Verify audit event was created
		events, err := taskStore.GetTaskAuditEvents(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to get audit events: %v", err)
		}

		if len(events) != 1 {
			t.Fatalf("Expected 1 audit event, got %d", len(events))
		}

		event := events[0]
		if event.EventType != tasks.EventTypeRetryScheduled {
			t.Errorf("Expected event type %q, got %q", tasks.EventTypeRetryScheduled, event.EventType)
		}
		if event.TaskID != taskID {
			t.Errorf("Expected task_id %q, got %q", taskID, event.TaskID)
		}
		if event.CreatedBy != "dispatcher" {
			t.Errorf("Expected created_by = 'dispatcher', got %q", event.CreatedBy)
		}

		// Verify event data contains expected fields
		if event.EventData == nil {
			t.Fatal("Expected event_data to be non-nil")
		}
		retryCount, hasRetryCount := event.EventData["retry_count"]
		if !hasRetryCount || retryCount != float64(1) {
			t.Errorf("Expected retry_count = 1 in event_data, got %v", retryCount)
		}
		maxRetries, hasMaxRetries := event.EventData["max_retries"]
		if !hasMaxRetries || maxRetries != float64(3) {
			t.Errorf("Expected max_retries = 3 in event_data, got %v", maxRetries)
		}
		queueIDData, hasQueueID := event.EventData["queue_id"]
		if !hasQueueID || queueIDData != queueID {
			t.Errorf("Expected queue_id = %q in event_data, got %v", queueID, queueIDData)
		}
	})

	t.Run("moveToDeadLetter creates DLQ audit event", func(t *testing.T) {
		// Create a task in the tasks table first (required for foreign key constraint)
		taskID := uuid.New().String()
		workspace := "test-workspace"
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', $2, 'test-impl', 'assigned')
		`, taskID, workspace)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		// Create a task in the orchestrated queue at max retries (retry_count=2, max_retries=3)
		queueID := uuid.New().String()
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', $3, 'kubernetes', 'claimed', 2, 3)
		`, queueID, taskID, workspace)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Unclaim triggers DLQ since retry_count(2) >= max_retries(3) - 1
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() error = %v", err)
		}

		// Verify audit event was created
		events, err := taskStore.GetTaskAuditEvents(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to get audit events: %v", err)
		}

		if len(events) != 1 {
			t.Fatalf("Expected 1 audit event, got %d", len(events))
		}

		event := events[0]
		if event.EventType != tasks.EventTypeMovedToDLQ {
			t.Errorf("Expected event type %q, got %q", tasks.EventTypeMovedToDLQ, event.EventType)
		}
		if event.TaskID != taskID {
			t.Errorf("Expected task_id %q, got %q", taskID, event.TaskID)
		}
		if event.CreatedBy != "dispatcher" {
			t.Errorf("Expected created_by = 'dispatcher', got %q", event.CreatedBy)
		}

		// Verify event data contains expected fields
		category := "delivery_failure"
		reason := "Max retries exceeded - failed to deliver to orchestrator"
		if event.EventData == nil {
			t.Fatal("Expected event_data to be non-nil")
		}
		categoryData, hasCategory := event.EventData["category"]
		if !hasCategory || categoryData != category {
			t.Errorf("Expected category = %q in event_data, got %v", category, categoryData)
		}
		reasonData, hasReason := event.EventData["reason"]
		if !hasReason || reasonData != reason {
			t.Errorf("Expected reason = %q in event_data, got %v", reason, reasonData)
		}
		workspaceData, hasWorkspace := event.EventData["workspace"]
		if !hasWorkspace || workspaceData != workspace {
			t.Errorf("Expected workspace = %q in event_data, got %v", workspace, workspaceData)
		}
		retryCount, hasRetryCount := event.EventData["retry_count"]
		if !hasRetryCount || retryCount != float64(2) {
			t.Errorf("Expected retry_count = 2 in event_data, got %v", retryCount)
		}
		attemptCount, hasAttemptCount := event.EventData["attempt_count"]
		if !hasAttemptCount || attemptCount != float64(3) {
			t.Errorf("Expected attempt_count = 3 in event_data, got %v", attemptCount)
		}
	})
}

// TestRecoverStaleClaims verifies that stale claimed tasks are recovered
func TestRecoverStaleClaims(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	testMetrics := NewDispatcherMetricsWithRegistry(prometheus.NewRegistry())
	dispatcher, err := NewNotifyTaskDispatcher(testDB.DB, "", 0, nil, testMetrics)
	if err != nil {
		t.Fatalf("Failed to create dispatcher: %v", err)
	}

	t.Run("recovers tasks claimed longer than threshold", func(t *testing.T) {
		taskID := uuid.New().String()
		queueID := uuid.New().String()

		// Create parent task
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		// Create a claimed task with claimed_at in the past
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, claimed_by, claimed_at, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 'old-gateway', NOW() - INTERVAL '10 minutes', 0, 3)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Recover with a 5-minute threshold — should find the 10-minute-old claim
		recovered, err := dispatcher.RecoverStaleClaims(ctx, 5*time.Minute)
		if err != nil {
			t.Fatalf("RecoverStaleClaims() error = %v", err)
		}
		if recovered != 1 {
			t.Errorf("Expected 1 recovered, got %d", recovered)
		}

		// Verify task is back to pending
		var status string
		var retryCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT status, retry_count FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&status, &retryCount)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}
		if status != "pending" {
			t.Errorf("Expected status = 'pending', got %q", status)
		}
		if retryCount != 1 {
			t.Errorf("Expected retry_count = 1, got %d", retryCount)
		}
	})

	t.Run("does not recover recently claimed tasks", func(t *testing.T) {
		taskID := uuid.New().String()
		queueID := uuid.New().String()

		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		// Create a recently claimed task (claimed just now)
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, claimed_by, claimed_at, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 'active-gateway', NOW(), 0, 3)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		// Recover with a 5-minute threshold — should NOT find the just-claimed task
		recovered, err := dispatcher.RecoverStaleClaims(ctx, 5*time.Minute)
		if err != nil {
			t.Fatalf("RecoverStaleClaims() error = %v", err)
		}
		if recovered != 0 {
			t.Errorf("Expected 0 recovered, got %d", recovered)
		}

		// Verify task is still claimed
		var status string
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT status FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&status)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}
		if status != "claimed" {
			t.Errorf("Expected status = 'claimed', got %q", status)
		}
	})

	t.Run("sends to DLQ when retries exhausted", func(t *testing.T) {
		taskID := uuid.New().String()
		queueID := uuid.New().String()

		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
		`, taskID)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		// Create a stale claimed task at max retries
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, claimed_by, claimed_at, retry_count, max_retries)
			VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 'crashed-gateway', NOW() - INTERVAL '10 minutes', 2, 3)
		`, queueID, taskID)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		recovered, err := dispatcher.RecoverStaleClaims(ctx, 5*time.Minute)
		if err != nil {
			t.Fatalf("RecoverStaleClaims() error = %v", err)
		}
		if recovered != 1 {
			t.Errorf("Expected 1 recovered, got %d", recovered)
		}

		// Verify task was sent to DLQ (failed status)
		var status string
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT status FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&status)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}
		if status != "failed" {
			t.Errorf("Expected status = 'failed', got %q", status)
		}

		// Verify DLQ entry
		var dlqCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM dlq WHERE original_task_id = $1
		`, taskID).Scan(&dlqCount)
		if err != nil {
			t.Fatalf("Failed to query DLQ: %v", err)
		}
		if dlqCount != 1 {
			t.Errorf("Expected 1 DLQ entry, got %d", dlqCount)
		}
	})
}
