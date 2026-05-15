package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
)

// PollingTaskDispatcher is a polling-only implementation of TaskDispatcher.
// It uses the same orchestrated_task_queue SQL table as NotifyTaskDispatcher
// but does NOT open a separate pq.Listener / PostgreSQL NOTIFY connection —
// it relies purely on polling at pollInterval cadence. State is durable in
// SQL (postgres or sqlite); the "polling" qualifier describes the wake-up
// mechanism, not the storage backend. Used in lite mode and other contexts
// where a NOTIFY connection is undesirable or unavailable.
//
// All orchestrated_task_queue reads and writes go through the tasks.Store
// interface — the dispatcher no longer holds a raw *sql.DB.
type PollingTaskDispatcher struct {
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
// taskStore is the tasks domain Store used for all orchestrated_task_queue
// reads and writes. The dispatcher no longer needs a raw *sql.DB handle.
func NewPollingTaskDispatcher(taskStore taskstore.Store) *PollingTaskDispatcher {
	return &PollingTaskDispatcher{
		taskStore:    taskStore,
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

	entries, err := d.taskStore.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		if ctx.Err() == nil {
			logging.Logger.Error().Err(err).Msg("polling dispatcher failed to poll tasks")
		}
		return
	}

	for _, entry := range entries {
		task := &OrchestrationTaskNotification{
			QueueID:              entry.QueueID,
			TaskID:               entry.TaskID,
			Profile:              entry.Profile,
			Workspace:            entry.Workspace,
			TargetImplementation: entry.TargetImplementation,
		}

		logging.Logger.Info().Str("task_id", task.TaskID).Str("profile", task.Profile).Msg("polling dispatcher polled pending task")

		d.mu.RLock()
		callback := d.onTaskReceived
		d.mu.RUnlock()
		if callback != nil {
			callback(task)
		}
	}
}

// ClaimTask attempts to claim a task for an orchestrator.
// Returns ErrTaskAlreadyClaimed if another gateway already claimed it.
func (d *PollingTaskDispatcher) ClaimTask(ctx context.Context, queueID, orchestratorID string) error {
	claimed, err := d.taskStore.ClaimQueueEntry(ctx, queueID, orchestratorID)
	if err != nil {
		return err
	}

	if !claimed {
		return ErrTaskAlreadyClaimed
	}

	logging.Logger.Info().Str("queue_id", queueID).Str("orchestrator_id", orchestratorID).Msg("polling dispatcher claimed task")
	return nil
}

// UnclaimTask releases a claimed task back to pending status for retry.
// Implements retry limiting with exponential backoff; moves to DLQ when retries are exhausted.
//
// All mutations go through tasks.Store transactional methods (Stage 2
// StoreTx discipline) — no direct *sql.Tx usage.
func (d *PollingTaskDispatcher) UnclaimTask(ctx context.Context, queueID string) error {
	tx, err := d.taskStore.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	taskID, workspace, retryCount, maxRetries, err := d.taskStore.QueryQueueEntryForUnclaimTx(ctx, tx, queueID)
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

	if err := d.taskStore.UpdateQueueEntryForRetryTx(ctx, tx, queueID, newRetryCount, backoffSeconds); err != nil {
		return fmt.Errorf("update task for retry: %w", err)
	}

	if err = d.taskStore.RecordAuditEventTx(ctx, tx, &taskstore.TaskAuditEvent{
		TaskID:    taskID,
		EventType: taskstore.EventTypeRetryScheduled,
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

// moveToDeadLetterTx moves a task to the DLQ within an existing StoreTx.
func (d *PollingTaskDispatcher) moveToDeadLetterTx(ctx context.Context, tx taskstore.StoreTx, queueID, taskID, workspace string, retryCount int) error {
	reason := "Max retries exceeded - failed to deliver to orchestrator"
	attemptCount := retryCount + 1

	if err := d.taskStore.MarkQueueEntryFailedTx(ctx, tx, queueID, reason); err != nil {
		return fmt.Errorf("mark task failed: %w", err)
	}

	if err := d.taskStore.InsertDLQEntryTx(ctx, tx, taskID, workspace, reason, attemptCount); err != nil {
		return fmt.Errorf("insert into dlq: %w", err)
	}

	if err := d.taskStore.RecordAuditEventTx(ctx, tx, &taskstore.TaskAuditEvent{
		TaskID:    taskID,
		EventType: taskstore.EventTypeMovedToDLQ,
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
	return d.taskStore.CompleteQueueEntry(ctx, queueID)
}

// FailTask marks a task as failed with an error message.
func (d *PollingTaskDispatcher) FailTask(ctx context.Context, queueID, errorMsg string) error {
	return d.taskStore.FailQueueEntry(ctx, queueID, errorMsg)
}

// GetTaskDetails retrieves full task details including launch params.
func (d *PollingTaskDispatcher) GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error) {
	details, err := d.taskStore.GetQueueEntryDetails(ctx, queueID)
	if err != nil {
		return nil, err
	}

	return &OrchestratedTaskPayload{
		TaskID:               details.TaskID,
		TargetImplementation: details.TargetImplementation,
		Workspace:            details.Workspace,
		Profile:              details.Profile,
		LaunchParams:         details.LaunchParams,
	}, nil
}

// RecoverStaleClaims finds tasks stuck in 'claimed' status longer than the given
// threshold and unclaims them. Returns the number of tasks recovered.
func (d *PollingTaskDispatcher) RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error) {
	staleIDs, err := d.taskStore.ListStaleClaimedQueueEntries(ctx, threshold, 50)
	if err != nil {
		return 0, fmt.Errorf("query stale claims: %w", err)
	}

	var recovered int
	for _, queueID := range staleIDs {
		if err := d.UnclaimTask(ctx, queueID); err != nil {
			logging.Logger.Error().Err(err).Str("queue_id", queueID).Msg("polling dispatcher failed to recover stale claim")
			continue
		}

		logging.Logger.Info().Str("queue_id", queueID).Msg("polling dispatcher recovered stale claim")
		recovered++
	}

	return recovered, nil
}

// CompleteTaskByTaskID marks orchestrated_task_queue row(s) for this taskID as
// completed. Idempotent.
func (d *PollingTaskDispatcher) CompleteTaskByTaskID(ctx context.Context, taskID string) error {
	return d.taskStore.CompleteQueueEntryByTaskID(ctx, taskID)
}

// FailTaskByTaskID marks orchestrated_task_queue row(s) for this taskID as
// failed. Idempotent.
func (d *PollingTaskDispatcher) FailTaskByTaskID(ctx context.Context, taskID, errorMsg string) error {
	return d.taskStore.FailQueueEntryByTaskID(ctx, taskID, errorMsg)
}
