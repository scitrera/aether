package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/pkg/tasks"
)

// NotifyTaskDispatcher listens for orchestration tasks via PostgreSQL NOTIFY
// and delivers them to connected orchestrators via a callback.
// For contexts where a NOTIFY connection is undesirable (lite mode, SQLite),
// use PollingTaskDispatcher instead, which uses the same SQL table but wakes
// up solely on a poll ticker rather than a pq.Listener.
type NotifyTaskDispatcher struct {
	db *sql.DB
	// taskStore is the tasks domain Store (internal/storage/tasks).
	taskStore    taskstore.Store
	listener     *pq.Listener
	pollInterval time.Duration

	// Callback to deliver task to orchestrator
	onTaskReceived func(task *OrchestrationTaskNotification)

	stopCh     chan struct{}
	wg         sync.WaitGroup
	instanceID string

	mu      sync.RWMutex
	running bool

	// Metrics for monitoring dispatcher health
	metrics *DispatcherMetrics
}

// OrchestrationTaskNotification represents a task from PostgreSQL NOTIFY
type OrchestrationTaskNotification struct {
	QueueID              string `json:"queue_id"`
	TaskID               string `json:"task_id"`
	Profile              string `json:"profile"`
	Workspace            string `json:"workspace"`
	TargetImplementation string `json:"target_implementation"`
}

// NewNotifyTaskDispatcher creates a new NotifyTaskDispatcher.
// connStr is the PostgreSQL connection string for pq.Listener.
// When connStr is empty, NOTIFY is disabled and the dispatcher falls back to
// polling only — equivalent to PollingTaskDispatcher but with the same struct.
func NewNotifyTaskDispatcher(
	db *sql.DB,
	connStr string,
	pollInterval time.Duration,
	onTaskReceived func(task *OrchestrationTaskNotification),
	optionalMetrics ...*DispatcherMetrics, // Optional: pass custom metrics for tests
) (*NotifyTaskDispatcher, error) {
	var listener *pq.Listener

	if connStr != "" {
		listener = pq.NewListener(
			connStr,
			10*time.Second,
			time.Minute,
			func(ev pq.ListenerEventType, err error) {
				if err != nil {
					logging.Logger.Error().Err(err).Msg("dispatcher listener error")
				}
			},
		)
	} else {
		logging.Logger.Info().Msg("dispatcher: no connection string, NOTIFY disabled (polling only)")
	}

	// Use provided metrics or create new ones
	var metrics *DispatcherMetrics
	if len(optionalMetrics) > 0 && optionalMetrics[0] != nil {
		metrics = optionalMetrics[0]
	} else {
		metrics = NewDispatcherMetrics()
	}

	return &NotifyTaskDispatcher{
		db:             db,
		taskStore:      taskpg.New(db),
		listener:       listener,
		pollInterval:   pollInterval,
		onTaskReceived: onTaskReceived,
		stopCh:         make(chan struct{}),
		instanceID:     uuid.New().String()[:8],
		metrics:        metrics,
	}, nil
}

// SetCallback sets the callback function for handling received tasks.
// This allows setting the callback after dispatcher creation.
func (d *NotifyTaskDispatcher) SetCallback(callback func(task *OrchestrationTaskNotification)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTaskReceived = callback
}

// Start begins listening for orchestration tasks.
func (d *NotifyTaskDispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return nil
	}
	d.running = true
	d.mu.Unlock()

	// Listen for orchestration task notifications
	if d.listener != nil {
		if err := d.listener.Listen("orchestration_task"); err != nil {
			return err
		}
		logging.Logger.Info().Str("instance", d.instanceID).Msg("dispatcher started with NOTIFY")
	} else {
		logging.Logger.Info().Str("instance", d.instanceID).Msg("dispatcher started (polling only)")
	}

	// Start dispatch loop
	d.wg.Add(1)
	go d.run(ctx)

	return nil
}

// Stop gracefully shuts down the dispatcher.
func (d *NotifyTaskDispatcher) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.running = false
	d.mu.Unlock()

	close(d.stopCh)
	d.wg.Wait()

	if d.listener != nil {
		d.listener.Close()
	}

	logging.Logger.Info().Str("instance", d.instanceID).Msg("dispatcher stopped")
}

// IsRunning returns whether the dispatcher is currently running.
func (d *NotifyTaskDispatcher) IsRunning() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.running
}

// GetQueueDepth returns the current pending queue depth by querying the database directly.
func (d *NotifyTaskDispatcher) GetQueueDepth() float64 {
	ctx := context.Background()
	var count int
	if err := d.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orchestrated_task_queue WHERE status = $1
	`, string(QueueStatusPending)).Scan(&count); err != nil {
		logging.Logger.Error().Err(err).Msg("dispatcher failed to query queue depth")
		return 0
	}
	return float64(count)
}

// GetStats returns current dispatcher statistics including queue depth, rates, and latencies.
// This collects metrics from both Prometheus counters/histograms and direct database queries.
func (d *NotifyTaskDispatcher) GetStats(ctx context.Context) (*DispatcherStats, error) {
	stats := &DispatcherStats{}

	// Queue depth from direct database query
	stats.QueueDepth = d.GetQueueDepth()

	// Collect Prometheus metrics using Write method
	var claimed, failed float64
	var latencySum, latencyCount float64

	// Helper to extract counter value
	getCounterValue := func(counter prometheus.Counter) float64 {
		var m dto.Metric
		if err := counter.Write(&m); err != nil {
			return 0
		}
		if m.Counter != nil {
			return m.Counter.GetValue()
		}
		return 0
	}

	// Helper to extract histogram values
	getHistogramStats := func(hist prometheus.Histogram) (sum, count float64) {
		var m dto.Metric
		if err := hist.Write(&m); err != nil {
			return 0, 0
		}
		if m.Histogram != nil {
			return m.Histogram.GetSampleSum(), float64(m.Histogram.GetSampleCount())
		}
		return 0, 0
	}

	// Collect counter values
	claimed = getCounterValue(d.metrics.TasksClaimed)
	failed = getCounterValue(d.metrics.TasksFailed)

	// Collect histogram values
	latencySum, latencyCount = getHistogramStats(d.metrics.ClaimLatency)

	// Calculate rates (these are cumulative counters, so in practice you'd want
	// to track deltas over time, but for now we return totals)
	total := claimed
	if total > 0 {
		stats.ClaimRate = claimed
		stats.FailureRate = (failed / total) * 100 // percentage
	}

	// Calculate average latency in milliseconds
	if latencyCount > 0 {
		stats.AvgClaimLatencyMs = (latencySum / latencyCount) * 1000 // convert seconds to ms
	}

	return stats, nil
}

// run is the main dispatch loop.
func (d *NotifyTaskDispatcher) run(ctx context.Context) {
	defer d.wg.Done()

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	var notifyCh <-chan *pq.Notification
	if d.listener != nil {
		notifyCh = d.listener.Notify
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case notification := <-notifyCh:
			if notification != nil {
				d.handleNotification(notification)
			}
		case <-ticker.C:
			d.pollPendingTasks(ctx)
		}
	}
}

// handleNotification processes a PostgreSQL NOTIFY event.
func (d *NotifyTaskDispatcher) handleNotification(notification *pq.Notification) {
	if notification == nil || notification.Channel != "orchestration_task" {
		return
	}

	var task OrchestrationTaskNotification
	if err := json.Unmarshal([]byte(notification.Extra), &task); err != nil {
		logging.Logger.Error().Err(err).Msg("dispatcher failed to parse notification")
		return
	}

	logging.Logger.Info().Str("task_id", task.TaskID).Str("profile", task.Profile).Str("workspace", task.Workspace).Msg("dispatcher received NOTIFY")

	d.mu.RLock()
	callback := d.onTaskReceived
	d.mu.RUnlock()
	if callback != nil {
		callback(&task)
	}
}

// pollPendingTasks polls for pending orchestration tasks as a backup.
func (d *NotifyTaskDispatcher) pollPendingTasks(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	// Sample queue depth for metrics
	d.sampleQueueDepth(ctx)

	// Find pending tasks that haven't been claimed
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
			logging.Logger.Error().Err(err).Msg("dispatcher failed to poll tasks")
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
		var task OrchestrationTaskNotification
		if err := rows.Scan(&task.QueueID, &task.TaskID, &task.Profile, &task.Workspace, &task.TargetImplementation); err != nil {
			logging.Logger.Error().Err(err).Msg("dispatcher failed to scan row")
			continue
		}

		logging.Logger.Info().Str("task_id", task.TaskID).Str("profile", task.Profile).Msg("dispatcher polled pending task")

		d.mu.RLock()
		callback := d.onTaskReceived
		d.mu.RUnlock()
		if callback != nil {
			callback(&task)
		}
	}
	if err := rows.Err(); err != nil {
		logging.Logger.Error().Err(err).Msg("error iterating pending tasks")
	}
}

// sampleQueueDepth queries the current queue depth and updates the Prometheus gauge.
// This runs periodically in the dispatcher run loop to track queue buildup.
func (d *NotifyTaskDispatcher) sampleQueueDepth(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	d.metrics.UpdateQueueDepth(d.GetQueueDepth())
}

// ErrTaskAlreadyClaimed is returned when trying to claim a task that's already been claimed
var ErrTaskAlreadyClaimed = fmt.Errorf("task already claimed")

// ClaimTask attempts to claim a task for an orchestrator.
// Returns ErrTaskAlreadyClaimed if another gateway/orchestrator already claimed it.
// This is the key mechanism for ensuring exclusive delivery across multiple gateways.
func (d *NotifyTaskDispatcher) ClaimTask(ctx context.Context, queueID, orchestratorID string) error {
	startTime := time.Now()

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
		// Another gateway or previous attempt already claimed this task
		return ErrTaskAlreadyClaimed
	}

	// Track successful claim with latency
	d.metrics.RecordTaskClaimed(time.Since(startTime).Seconds())

	logging.Logger.Info().Str("queue_id", queueID).Str("orchestrator_id", orchestratorID).Msg("dispatcher claimed task")
	return nil
}

// UnclaimTask releases a claimed task back to pending status for retry.
// Used when delivery fails and we want another orchestrator/gateway to try.
// Implements retry limiting with exponential backoff. If the task has exhausted
// its retries (retry_count >= max_retries - 1), it is moved to the DLQ instead.
func (d *NotifyTaskDispatcher) UnclaimTask(ctx context.Context, queueID string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Fetch current state within the transaction
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

	// Check if retries exhausted: move to DLQ instead of retrying
	if retryCount >= maxRetries-1 {
		if err := d.moveToDeadLetterTx(ctx, tx, queueID, taskID, workspace, retryCount); err != nil {
			return err
		}
		return tx.Commit()
	}

	// Increment retry_count and set exponential backoff
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

	// Record retry_scheduled audit event
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
func (d *NotifyTaskDispatcher) moveToDeadLetterTx(ctx context.Context, tx *sql.Tx, queueID, taskID, workspace string, retryCount int) error {
	reason := "Max retries exceeded - failed to deliver to orchestrator"
	attemptCount := retryCount + 1

	// Mark the queue entry as failed
	_, err := tx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`, queueID, reason, string(QueueStatusFailed))
	if err != nil {
		return fmt.Errorf("mark task failed: %w", err)
	}

	// Insert into DLQ
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dlq (original_task_id, category, workspace, failure_reason, attempt_count, last_attempt_at)
		VALUES ($1, 'delivery_failure', $2, $3, $4, NOW())
	`, taskID, workspace, reason, attemptCount)
	if err != nil {
		return fmt.Errorf("insert into dlq: %w", err)
	}

	// Record moved_to_dlq audit event
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

	// Track task failure metric
	d.metrics.RecordTaskFailed()

	return nil
}

// RecoverStaleClaims finds tasks stuck in 'claimed' status longer than the given
// threshold and unclaims them. This handles gateway crashes that leave tasks in limbo.
// Returns the number of tasks recovered.
func (d *NotifyTaskDispatcher) RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error) {
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
			logging.Logger.Error().Err(err).Msg("dispatcher failed to scan stale claim")
			continue
		}

		if err := d.UnclaimTask(ctx, queueID); err != nil {
			logging.Logger.Error().Err(err).Str("queue_id", queueID).Msg("dispatcher failed to recover stale claim")
			continue
		}

		logging.Logger.Info().Str("queue_id", queueID).Msg("dispatcher recovered stale claim")
		recovered++
	}

	return recovered, rows.Err()
}

// CompleteTask marks a task as completed.
func (d *NotifyTaskDispatcher) CompleteTask(ctx context.Context, queueID string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $2, completed_at = NOW()
		WHERE queue_id = $1
	`

	_, err := d.db.ExecContext(ctx, query, queueID, string(QueueStatusCompleted))
	if err != nil {
		return err
	}

	// Track successful completion
	d.metrics.RecordTaskCompleted()

	return nil
}

// FailTask marks a task as failed.
func (d *NotifyTaskDispatcher) FailTask(ctx context.Context, queueID, errorMsg string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`

	_, err := d.db.ExecContext(ctx, query, queueID, errorMsg, string(QueueStatusFailed))
	if err != nil {
		return err
	}

	// Track task failure
	d.metrics.RecordTaskFailed()

	return nil
}

// CompleteTaskByTaskID marks orchestrated_task_queue row(s) for this taskID as
// completed. Called when a task transitions to a terminal success state (e.g.,
// the agent successfully attached via StartTaskWithAgent) so the dispatcher
// stops re-dispatching it via stale-claim recovery. No-op if no matching row
// exists or the row is already in a terminal state. Idempotent.
func (d *NotifyTaskDispatcher) CompleteTaskByTaskID(ctx context.Context, taskID string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $2, completed_at = NOW()
		WHERE task_id = $1 AND status IN ($3, $4)
	`
	_, err := d.db.ExecContext(ctx, query, taskID, string(QueueStatusCompleted), string(QueueStatusPending), string(QueueStatusClaimed))
	return err
}

// FailTaskByTaskID marks orchestrated_task_queue row(s) for this taskID as
// failed with the given reason. Called when a task itself is failed or
// cancelled so the queue entry is retired rather than recovered by stale-claim
// sweeps. Idempotent.
func (d *NotifyTaskDispatcher) FailTaskByTaskID(ctx context.Context, taskID, errorMsg string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE task_id = $1 AND status IN ($4, $5)
	`
	_, err := d.db.ExecContext(ctx, query, taskID, errorMsg, string(QueueStatusFailed), string(QueueStatusPending), string(QueueStatusClaimed))
	return err
}

// GetTaskDetails retrieves full task details including launch params.
func (d *NotifyTaskDispatcher) GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error) {
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
			logging.Logger.Error().Err(err).Msg("dispatcher failed to unmarshal launch params")
		}
	}

	return &task, nil
}
