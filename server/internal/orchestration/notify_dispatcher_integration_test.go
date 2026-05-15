package orchestration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/tasks"
)

// TestOrchestrationRetryIntegration verifies the complete retry→DLQ flow
// for orchestration tasks, including proper state transitions, exponential
// backoff, and audit event recording.
func TestOrchestrationRetryIntegration(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return // Skip was called in SetupTestDB
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	// Use isolated metrics registry for tests to avoid conflicts
	testMetrics := NewDispatcherMetricsWithRegistry(prometheus.NewRegistry())
	dispatcher, err := NewNotifyTaskDispatcher(taskStore, "", 0, nil, testMetrics)
	if err != nil {
		t.Fatalf("Failed to create dispatcher: %v", err)
	}

	t.Run("Complete retry→DLQ workflow", func(t *testing.T) {
		// Step 1: Create a task in the tasks table (required for foreign key constraint)
		taskID := uuid.New().String()
		workspace := "test-workspace"
		implementation := "python-worker"
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', $2, $3, 'pending')
		`, taskID, workspace, implementation)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		// Step 2: Create an orchestration task entry with max_retries = 3
		queueID := uuid.New().String()
		profile := "kubernetes"
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (
				queue_id, task_id, target_implementation, workspace, profile,
				status, retry_count, max_retries
			)
			VALUES ($1, $2, $3, $4, $5, 'pending', 0, 3)
		`, queueID, taskID, implementation, workspace, profile)
		if err != nil {
			t.Fatalf("Failed to insert orchestration task: %v", err)
		}

		// Step 3: Simulate retry attempts by claiming and unclaiming
		orchestratorID := "test-orchestrator-1"

		// Attempt 1: Claim and unclaim (retry_count: 0 → 1)
		err = dispatcher.ClaimTask(ctx, queueID, orchestratorID)
		if err != nil {
			t.Fatalf("First ClaimTask() failed: %v", err)
		}

		timeBeforeFirstUnclaim := time.Now().UTC()
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("First UnclaimTask() failed: %v", err)
		}

		// Verify retry_count incremented and next_retry_at is set
		var retryCount1 int
		var status1 string
		var nextRetryAt1 sql.NullTime
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT retry_count, status, next_retry_at
			FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&retryCount1, &status1, &nextRetryAt1)
		if err != nil {
			t.Fatalf("Failed to query after first retry: %v", err)
		}

		if retryCount1 != 1 {
			t.Errorf("After first retry: expected retry_count = 1, got %d", retryCount1)
		}
		if status1 != "pending" {
			t.Errorf("After first retry: expected status = 'pending', got %q", status1)
		}
		if !nextRetryAt1.Valid {
			t.Error("After first retry: expected next_retry_at to be set")
		}

		// Verify first retry backoff is approximately 2 seconds (1 << 1 = 2)
		if nextRetryAt1.Valid {
			expectedBackoff1 := 2 * time.Second
			actualBackoff1 := nextRetryAt1.Time.Sub(timeBeforeFirstUnclaim)
			// Allow 500ms tolerance for test execution time
			if actualBackoff1 < expectedBackoff1-500*time.Millisecond || actualBackoff1 > expectedBackoff1+500*time.Millisecond {
				t.Errorf("First retry backoff: expected ~%v, got %v", expectedBackoff1, actualBackoff1)
			}
		}

		// Attempt 2: Clear next_retry_at, claim and unclaim (retry_count: 1 → 2)
		_, err = testDB.DB.ExecContext(ctx, `
			UPDATE orchestrated_task_queue
			SET next_retry_at = NULL
			WHERE queue_id = $1
		`, queueID)
		if err != nil {
			t.Fatalf("Failed to clear next_retry_at: %v", err)
		}

		err = dispatcher.ClaimTask(ctx, queueID, orchestratorID)
		if err != nil {
			t.Fatalf("Second ClaimTask() failed: %v", err)
		}

		timeBeforeSecondUnclaim := time.Now().UTC()
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("Second UnclaimTask() failed: %v", err)
		}

		// Verify retry_count incremented to 2
		var retryCount2 int
		var nextRetryAt2 sql.NullTime
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT retry_count, next_retry_at
			FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&retryCount2, &nextRetryAt2)
		if err != nil {
			t.Fatalf("Failed to query after second retry: %v", err)
		}

		if retryCount2 != 2 {
			t.Errorf("After second retry: expected retry_count = 2, got %d", retryCount2)
		}
		if !nextRetryAt2.Valid {
			t.Error("After second retry: expected next_retry_at to be set")
		}

		// Verify second retry backoff is approximately 4 seconds (1 << 2 = 4)
		if nextRetryAt2.Valid {
			expectedBackoff2 := 4 * time.Second
			actualBackoff2 := nextRetryAt2.Time.Sub(timeBeforeSecondUnclaim)
			// Allow 500ms tolerance for test execution time
			if actualBackoff2 < expectedBackoff2-500*time.Millisecond || actualBackoff2 > expectedBackoff2+500*time.Millisecond {
				t.Errorf("Second retry backoff: expected ~%v, got %v", expectedBackoff2, actualBackoff2)
			}
		}

		// Verify exponential backoff relationship: backoff2 should be 2x backoff1
		if nextRetryAt2.Valid && nextRetryAt1.Valid {
			actualBackoff1 := nextRetryAt1.Time.Sub(timeBeforeFirstUnclaim)
			actualBackoff2 := nextRetryAt2.Time.Sub(timeBeforeSecondUnclaim)
			// Second backoff should be approximately double the first (exponential growth)
			expectedRatio := 2.0
			actualRatio := actualBackoff2.Seconds() / actualBackoff1.Seconds()
			// Allow 20% tolerance for timing variations
			if actualRatio < expectedRatio*0.8 || actualRatio > expectedRatio*1.2 {
				t.Errorf("Exponential backoff ratio: expected ~%.1f, got %.2f (backoff1=%v, backoff2=%v)",
					expectedRatio, actualRatio, actualBackoff1, actualBackoff2)
			}
		}

		// Attempt 3: Clear next_retry_at, claim and unclaim (retry_count: 2 → DLQ)
		_, err = testDB.DB.ExecContext(ctx, `
			UPDATE orchestrated_task_queue
			SET next_retry_at = NULL
			WHERE queue_id = $1
		`, queueID)
		if err != nil {
			t.Fatalf("Failed to clear next_retry_at: %v", err)
		}

		err = dispatcher.ClaimTask(ctx, queueID, orchestratorID)
		if err != nil {
			t.Fatalf("Third ClaimTask() failed: %v", err)
		}

		// This should trigger DLQ movement (retry_count=2, max_retries=3, so 2 >= 3-1)
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("Third UnclaimTask() failed: %v", err)
		}

		// Step 4: Verify task moved to DLQ
		var dlqTaskID, dlqCategory, dlqWorkspace, dlqReason string
		var dlqAttemptCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT original_task_id, category, workspace, failure_reason, attempt_count
			FROM dlq
			WHERE original_task_id = $1
		`, taskID).Scan(&dlqTaskID, &dlqCategory, &dlqWorkspace, &dlqReason, &dlqAttemptCount)
		if err != nil {
			t.Fatalf("Failed to query DLQ: %v", err)
		}

		if dlqTaskID != taskID {
			t.Errorf("Expected DLQ original_task_id = %q, got %q", taskID, dlqTaskID)
		}
		if dlqCategory != "delivery_failure" {
			t.Errorf("Expected DLQ category = 'delivery_failure', got %q", dlqCategory)
		}
		if dlqWorkspace != workspace {
			t.Errorf("Expected DLQ workspace = %q, got %q", workspace, dlqWorkspace)
		}
		if dlqReason != "Max retries exceeded - failed to deliver to orchestrator" {
			t.Errorf("Expected DLQ reason = 'Max retries exceeded...', got %q", dlqReason)
		}
		if dlqAttemptCount != 3 {
			t.Errorf("Expected DLQ attempt_count = 3, got %d", dlqAttemptCount)
		}

		// Step 5: Verify task marked as failed in orchestrated_task_queue
		var finalStatus string
		var errorMessage sql.NullString
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT status, error_message
			FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&finalStatus, &errorMessage)
		if err != nil {
			t.Fatalf("Failed to query final task status: %v", err)
		}

		if finalStatus != "failed" {
			t.Errorf("Expected final status = 'failed', got %q", finalStatus)
		}
		if !errorMessage.Valid || errorMessage.String != "Max retries exceeded - failed to deliver to orchestrator" {
			t.Errorf("Expected error_message = 'Max retries exceeded...', got %q", errorMessage.String)
		}

		// Step 6: Verify audit events were recorded
		auditEvents, err := taskStore.GetTaskAuditEvents(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to get audit events: %v", err)
		}

		// Should have 2 retry scheduled events + 1 DLQ event = 3 total
		// (The third UnclaimTask goes directly to DLQ without creating a retry event)
		if len(auditEvents) != 3 {
			t.Errorf("Expected 3 audit events (2 retries + 1 DLQ), got %d", len(auditEvents))
		}

		// Verify we have the expected event types
		var retryEvents int
		var dlqEvents int
		for _, event := range auditEvents {
			switch event.EventType {
			case tasks.EventTypeRetryScheduled:
				retryEvents++
			case tasks.EventTypeMovedToDLQ:
				dlqEvents++
			}
		}

		if retryEvents != 2 {
			t.Errorf("Expected 2 retry scheduled events, got %d", retryEvents)
		}
		if dlqEvents != 1 {
			t.Errorf("Expected 1 DLQ event, got %d", dlqEvents)
		}
	})

	t.Run("Verify polling respects retry backoff", func(t *testing.T) {
		// Create a task with a future next_retry_at
		taskID := uuid.New().String()
		workspace := "test-workspace"
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', $2, 'test-impl', 'pending')
		`, taskID, workspace)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		queueID := uuid.New().String()
		futureRetryAt := time.Now().UTC().Add(10 * time.Minute)
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (
				queue_id, task_id, target_implementation, workspace, profile,
				status, retry_count, max_retries, next_retry_at
			)
			VALUES ($1, $2, 'test-impl', $3, 'kubernetes', 'pending', 1, 3, $4)
		`, queueID, taskID, workspace, futureRetryAt)
		if err != nil {
			t.Fatalf("Failed to insert orchestration task: %v", err)
		}

		// Track whether task was received
		taskReceived := false
		dispatcher.SetCallback(func(task *OrchestrationTaskNotification) {
			if task.QueueID == queueID {
				taskReceived = true
			}
		})

		// Poll pending tasks
		dispatcher.pollPendingTasks(ctx)

		// Verify task was NOT polled (backoff not expired)
		if taskReceived {
			t.Error("Task with future next_retry_at should not be polled")
		}

		// Now set next_retry_at to the past and poll again
		pastRetryAt := time.Now().UTC().Add(-1 * time.Minute)
		_, err = testDB.DB.ExecContext(ctx, `
			UPDATE orchestrated_task_queue
			SET next_retry_at = $2
			WHERE queue_id = $1
		`, queueID, pastRetryAt)
		if err != nil {
			t.Fatalf("Failed to update next_retry_at: %v", err)
		}

		// Poll again
		dispatcher.pollPendingTasks(ctx)

		// Verify task was polled (backoff expired)
		if !taskReceived {
			t.Error("Task with past next_retry_at should be polled")
		}
	})

	t.Run("Custom max_retries configuration", func(t *testing.T) {
		// Test that custom max_retries values are respected
		taskID := uuid.New().String()
		workspace := "test-workspace"
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', $2, 'test-impl', 'pending')
		`, taskID, workspace)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		queueID := uuid.New().String()
		customMaxRetries := 5
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (
				queue_id, task_id, target_implementation, workspace, profile,
				status, retry_count, max_retries
			)
			VALUES ($1, $2, 'test-impl', $3, 'kubernetes', 'claimed', 3, $4)
		`, queueID, taskID, workspace, customMaxRetries)
		if err != nil {
			t.Fatalf("Failed to insert orchestration task: %v", err)
		}

		// Unclaim (retry_count: 3 → 4, should still retry since 3 < 5-1)
		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() failed: %v", err)
		}

		// Verify task is still pending (not sent to DLQ)
		var status string
		var retryCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT status, retry_count FROM orchestrated_task_queue WHERE queue_id = $1
		`, queueID).Scan(&status, &retryCount)
		if err != nil {
			t.Fatalf("Failed to query task: %v", err)
		}

		if status != "pending" {
			t.Errorf("Expected status = 'pending' (still retrying), got %q", status)
		}
		if retryCount != 4 {
			t.Errorf("Expected retry_count = 4, got %d", retryCount)
		}

		// Now retry one more time (retry_count: 4 → DLQ, since 4 >= 5-1)
		_, err = testDB.DB.ExecContext(ctx, `
			UPDATE orchestrated_task_queue
			SET status = 'claimed', next_retry_at = NULL
			WHERE queue_id = $1
		`, queueID)
		if err != nil {
			t.Fatalf("Failed to set claimed status: %v", err)
		}

		err = dispatcher.UnclaimTask(ctx, queueID)
		if err != nil {
			t.Fatalf("UnclaimTask() failed: %v", err)
		}

		// Verify task sent to DLQ
		var dlqCount int
		err = testDB.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM dlq WHERE original_task_id = $1
		`, taskID).Scan(&dlqCount)
		if err != nil {
			t.Fatalf("Failed to query DLQ: %v", err)
		}

		if dlqCount != 1 {
			t.Errorf("Expected task in DLQ after custom max_retries, found %d entries", dlqCount)
		}
	})

	t.Run("Explicit exponential backoff timing verification", func(t *testing.T) {
		// This test explicitly verifies the exponential backoff formula:
		// retry 1: 1 << 1 = 2 seconds
		// retry 2: 1 << 2 = 4 seconds
		// retry 3: 1 << 3 = 8 seconds
		// retry 4: 1 << 4 = 16 seconds

		taskID := uuid.New().String()
		workspace := "test-workspace"
		_, err := testDB.DB.ExecContext(ctx, `
			INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
			VALUES ($1, 'agent_startup', $2, 'test-impl', 'pending')
		`, taskID, workspace)
		if err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}

		queueID := uuid.New().String()
		_, err = testDB.DB.ExecContext(ctx, `
			INSERT INTO orchestrated_task_queue (
				queue_id, task_id, target_implementation, workspace, profile,
				status, retry_count, max_retries
			)
			VALUES ($1, $2, 'test-impl', $3, 'kubernetes', 'pending', 0, 6)
		`, queueID, taskID, workspace)
		if err != nil {
			t.Fatalf("Failed to insert orchestration task: %v", err)
		}

		orchestratorID := "test-orchestrator-1"

		// Test cases for exponential backoff verification
		testCases := []struct {
			retryAttempt    int
			expectedBackoff time.Duration
		}{
			{1, 2 * time.Second},  // 1 << 1 = 2
			{2, 4 * time.Second},  // 1 << 2 = 4
			{3, 8 * time.Second},  // 1 << 3 = 8
			{4, 16 * time.Second}, // 1 << 4 = 16
		}

		for _, tc := range testCases {
			// Clear next_retry_at to allow retry
			if tc.retryAttempt > 1 {
				_, err = testDB.DB.ExecContext(ctx, `
					UPDATE orchestrated_task_queue
					SET next_retry_at = NULL
					WHERE queue_id = $1
				`, queueID)
				if err != nil {
					t.Fatalf("Failed to clear next_retry_at for retry %d: %v", tc.retryAttempt, err)
				}
			}

			// Claim task
			err = dispatcher.ClaimTask(ctx, queueID, orchestratorID)
			if err != nil {
				t.Fatalf("ClaimTask() failed for retry %d: %v", tc.retryAttempt, err)
			}

			// Capture time and unclaim
			timeBefore := time.Now().UTC()
			err = dispatcher.UnclaimTask(ctx, queueID)
			if err != nil {
				t.Fatalf("UnclaimTask() failed for retry %d: %v", tc.retryAttempt, err)
			}

			// Query next_retry_at
			var nextRetryAt sql.NullTime
			var retryCount int
			err = testDB.DB.QueryRowContext(ctx, `
				SELECT retry_count, next_retry_at
				FROM orchestrated_task_queue WHERE queue_id = $1
			`, queueID).Scan(&retryCount, &nextRetryAt)
			if err != nil {
				t.Fatalf("Failed to query after retry %d: %v", tc.retryAttempt, err)
			}

			// Verify retry count incremented
			if retryCount != tc.retryAttempt {
				t.Errorf("Retry %d: expected retry_count = %d, got %d",
					tc.retryAttempt, tc.retryAttempt, retryCount)
			}

			// Verify backoff timing
			if !nextRetryAt.Valid {
				t.Errorf("Retry %d: expected next_retry_at to be set", tc.retryAttempt)
				continue
			}

			actualBackoff := nextRetryAt.Time.Sub(timeBefore)
			// Allow 500ms tolerance for test execution time
			tolerance := 500 * time.Millisecond
			if actualBackoff < tc.expectedBackoff-tolerance || actualBackoff > tc.expectedBackoff+tolerance {
				t.Errorf("Retry %d: expected backoff ~%v (formula: 1<<%d), got %v",
					tc.retryAttempt, tc.expectedBackoff, tc.retryAttempt, actualBackoff)
			} else {
				t.Logf("Retry %d: backoff verified ✓ (expected: %v, actual: %v, formula: 1<<%d=%ds)",
					tc.retryAttempt, tc.expectedBackoff, actualBackoff, tc.retryAttempt, int(tc.expectedBackoff.Seconds()))
			}
		}
	})
}
