package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/pkg/tasks"
)

// PollingTaskDispatcher is a polling-only implementation of TaskDispatcher.
// It uses the same orchestrated_task_queue SQL table as NotifyTaskDispatcher
// but does NOT open a separate pq.Listener / PostgreSQL NOTIFY connection —
// it relies purely on polling at pollInterval cadence. State is durable in
// SQL (postgres or sqlite); the "polling" qualifier describes the wake-up
// mechanism, not the storage backend. Used in lite mode and other contexts
// where a NOTIFY connection is undesirable or unavailable.
type PollingTaskDispatcher struct {
	db *sql.DB
	// taskStore is the tasks domain Store (internal/storage/tasks).
	taskStore    taskstore.Store
	pollInterval time.Duration

	onTaskReceived func(task *OrchestrationTaskNotification)

	stopCh     chan struct{}
	wg         sync.WaitGroup
	instanceID string

	mu      sync.RWMutex
	running bool
}

// NewPollingTaskDispatcher creates a new PollingTaskDispatcher.
// db must be a valid *sql.DB (PostgreSQL or SQLite via sqlite_compat).
func NewPollingTaskDispatcher(db *sql.DB) *PollingTaskDispatcher {
	return &PollingTaskDispatcher{
		db:           db,
		taskStore:    taskpg.New(db),
		pollInterval: 2 * time.Second,
		stopCh:       make(chan struct{}),
		instanceID:   uuid.New().String()[:8],
	}
}

// SetCallback sets the callback function for handling received task notifications.
func (d *PollingTaskDispatcher) SetCallback(callback func(task *OrchestrationTaskNotification)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTaskReceived = callback
}

// Start begins the polling goroutine that checks for pending orchestration tasks.
func (d *PollingTaskDispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return nil
	}
	d.running = true
	d.mu.Unlock()

	logging.Logger.Info().Str("instance", d.instanceID).Msg("polling dispatcher started (polling only)")

	d.wg.Add(1)
	go d.run(ctx)

	return nil
}

// Stop gracefully shuts down the dispatcher.
func (d *PollingTaskDispatcher) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.running = false
	d.mu.Unlock()

	close(d.stopCh)
	d.wg.Wait()

	logging.Logger.Info().Str("instance", d.instanceID).Msg("polling dispatcher stopped")
}

// run is the main polling loop.
func (d *PollingTaskDispatcher) run(ctx context.Context) {
	defer d.wg.Done()

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.pollPendingTasks(ctx)
		}
	}
}

// pollPendingTasks queries for pending orchestration tasks and delivers them via callback.
func (d *PollingTaskDispatcher) pollPendingTasks(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	query := `
		SELECT queue_id, task_id, profile, workspace, target_implementation
		FROM orchestrated_task_queue
		WHERE status = $1 AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at ASC
		LIMIT 10
	`

	rows, err := d.db.QueryContext(ctx, query, string(QueueStatusPending))
	if err != nil {
		if ctx.Err() == nil {
			logging.Logger.Error().Err(err).Msg("polling dispatcher failed to poll tasks")
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
		var task OrchestrationTaskNotification
		if err := rows.Scan(&task.QueueID, &task.TaskID, &task.Profile, &task.Workspace, &task.TargetImplementation); err != nil {
			logging.Logger.Error().Err(err).Msg("polling dispatcher failed to scan row")
			continue
		}

		logging.Logger.Info().Str("task_id", task.TaskID).Str("profile", task.Profile).Msg("polling dispatcher polled pending task")

		d.mu.RLock()
		callback := d.onTaskReceived
		d.mu.RUnlock()
		if callback != nil {
			callback(&task)
		}
	}
	if err := rows.Err(); err != nil {
		logging.Logger.Error().Err(err).Msg("polling dispatcher error iterating pending tasks")
	}
}

// ClaimTask attempts to claim a task for an orchestrator.
// Returns ErrTaskAlreadyClaimed if another gateway already claimed it.
func (d *PollingTaskDispatcher) ClaimTask(ctx context.Context, queueID, orchestratorID string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $3, claimed_by = $2, claimed_at = NOW()
		WHERE queue_id = $1 AND status = $4
	`

	result, err := d.db.ExecContext(ctx, query, queueID, orchestratorID, string(QueueStatusClaimed), string(QueueStatusPending))
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return ErrTaskAlreadyClaimed
	}

	logging.Logger.Info().Str("queue_id", queueID).Str("orchestrator_id", orchestratorID).Msg("polling dispatcher claimed task")
	return nil
}

// UnclaimTask releases a claimed task back to pending status for retry.
// Implements retry limiting with exponential backoff; moves to DLQ when retries are exhausted.
func (d *PollingTaskDispatcher) UnclaimTask(ctx context.Context, queueID string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var taskID, workspace string
	var retryCount, maxRetries int
	err = tx.QueryRowContext(ctx, `
		SELECT task_id, workspace, retry_count, max_retries
		FROM orchestrated_task_queue
		WHERE queue_id = $1 AND status = $2
	`, queueID, string(QueueStatusClaimed)).Scan(&taskID, &workspace, &retryCount, &maxRetries)
	if err != nil {
		return fmt.Errorf("query task for unclaim: %w", err)
	}

	if retryCount >= maxRetries-1 {
		if err := d.moveToDeadLetterTx(ctx, tx, queueID, taskID, workspace, retryCount); err != nil {
			return err
		}
		return tx.Commit()
	}

	newRetryCount := retryCount + 1
	backoffSeconds := 1 << newRetryCount // 2, 4, 8, 16, ...

	_, err = tx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $4,
		    claimed_by = NULL,
		    claimed_at = NULL,
		    retry_count = $2,
		    next_retry_at = NOW() + ($3 || ' seconds')::interval
		WHERE queue_id = $1
	`, queueID, newRetryCount, fmt.Sprintf("%d", backoffSeconds), string(QueueStatusPending))
	if err != nil {
		return fmt.Errorf("update task for retry: %w", err)
	}

	if err = d.taskStore.RecordAuditEventTx(ctx, tx, &tasks.TaskAuditEvent{
		TaskID:    taskID,
		EventType: tasks.EventTypeRetryScheduled,
		EventData: map[string]interface{}{
			"retry_count":   newRetryCount,
			"max_retries":   maxRetries,
			"queue_id":      queueID,
			"next_retry_at": time.Now().UTC().Add(time.Duration(backoffSeconds) * time.Second).Format(time.RFC3339),
		},
		CreatedBy: "dispatcher",
	}); err != nil {
		return fmt.Errorf("record retry audit event: %w", err)
	}

	return tx.Commit()
}

// moveToDeadLetterTx moves a task to the DLQ within an existing transaction.
func (d *PollingTaskDispatcher) moveToDeadLetterTx(ctx context.Context, tx *sql.Tx, queueID, taskID, workspace string, retryCount int) error {
	reason := "Max retries exceeded - failed to deliver to orchestrator"
	attemptCount := retryCount + 1

	_, err := tx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`, queueID, reason, string(QueueStatusFailed))
	if err != nil {
		return fmt.Errorf("mark task failed: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO dlq (original_task_id, category, workspace, failure_reason, attempt_count, last_attempt_at)
		VALUES ($1, 'delivery_failure', $2, $3, $4, NOW())
	`, taskID, workspace, reason, attemptCount)
	if err != nil {
		return fmt.Errorf("insert into dlq: %w", err)
	}

	if err = d.taskStore.RecordAuditEventTx(ctx, tx, &tasks.TaskAuditEvent{
		TaskID:    taskID,
		EventType: tasks.EventTypeMovedToDLQ,
		EventData: map[string]interface{}{
			"category":      "delivery_failure",
			"reason":        reason,
			"workspace":     workspace,
			"retry_count":   retryCount,
			"attempt_count": attemptCount,
		},
		CreatedBy: "dispatcher",
	}); err != nil {
		return fmt.Errorf("record dlq audit event: %w", err)
	}

	return nil
}

// CompleteTask marks a task as completed.
func (d *PollingTaskDispatcher) CompleteTask(ctx context.Context, queueID string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $2, completed_at = NOW()
		WHERE queue_id = $1
	`
	_, err := d.db.ExecContext(ctx, query, queueID, string(QueueStatusCompleted))
	return err
}

// FailTask marks a task as failed with an error message.
func (d *PollingTaskDispatcher) FailTask(ctx context.Context, queueID, errorMsg string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`
	_, err := d.db.ExecContext(ctx, query, queueID, errorMsg, string(QueueStatusFailed))
	return err
}

// GetTaskDetails retrieves full task details including launch params.
func (d *PollingTaskDispatcher) GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error) {
	query := `
		SELECT task_id, target_implementation, workspace, profile, launch_params
		FROM orchestrated_task_queue
		WHERE queue_id = $1
	`

	var task OrchestratedTaskPayload
	var launchParamsJSON []byte

	err := d.db.QueryRowContext(ctx, query, queueID).Scan(
		&task.TaskID,
		&task.TargetImplementation,
		&task.Workspace,
		&task.Profile,
		&launchParamsJSON,
	)
	if err != nil {
		return nil, err
	}

	if launchParamsJSON != nil {
		if err := json.Unmarshal(launchParamsJSON, &task.LaunchParams); err != nil {
			logging.Logger.Error().Err(err).Msg("polling dispatcher failed to unmarshal launch params")
		}
	}

	return &task, nil
}

// RecoverStaleClaims finds tasks stuck in 'claimed' status longer than the given
// threshold and unclaims them. Returns the number of tasks recovered.
func (d *PollingTaskDispatcher) RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error) {
	query := `
		SELECT queue_id
		FROM orchestrated_task_queue
		WHERE status = $2 AND claimed_at < NOW() - $1::interval
		ORDER BY claimed_at ASC
		LIMIT 50
	`

	rows, err := d.db.QueryContext(ctx, query, fmt.Sprintf("%d seconds", int(threshold.Seconds())), string(QueueStatusClaimed))
	if err != nil {
		return 0, fmt.Errorf("query stale claims: %w", err)
	}
	defer rows.Close()

	var recovered int
	for rows.Next() {
		var queueID string
		if err := rows.Scan(&queueID); err != nil {
			logging.Logger.Error().Err(err).Msg("polling dispatcher failed to scan stale claim")
			continue
		}

		if err := d.UnclaimTask(ctx, queueID); err != nil {
			logging.Logger.Error().Err(err).Str("queue_id", queueID).Msg("polling dispatcher failed to recover stale claim")
			continue
		}

		logging.Logger.Info().Str("queue_id", queueID).Msg("polling dispatcher recovered stale claim")
		recovered++
	}

	return recovered, rows.Err()
}
