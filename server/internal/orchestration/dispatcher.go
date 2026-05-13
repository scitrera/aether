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
	"github.com/scitrera/aether/pkg/tasks"
)

// OrchestratorTaskDispatcher listens for orchestration tasks via PostgreSQL NOTIFY
// and delivers them to connected orchestrators via a callback.
type OrchestratorTaskDispatcher struct {
	db           *sql.DB
	taskStore    *tasks.TaskStore
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

// NewOrchestratorTaskDispatcher creates a new dispatcher.
// connStr is the PostgreSQL connection string for pq.Listener.
func NewOrchestratorTaskDispatcher(
	db *sql.DB,
	connStr string,
	pollInterval time.Duration,
	onTaskReceived func(task *OrchestrationTaskNotification),
	optionalMetrics ...*DispatcherMetrics, // Optional: pass custom metrics for tests
) (*OrchestratorTaskDispatcher, error) {
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

	return &OrchestratorTaskDispatcher{
		db:             db,
		taskStore:      tasks.NewTaskStore(db),
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
func (d *OrchestratorTaskDispatcher) SetCallback(callback func(task *OrchestrationTaskNotification)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTaskReceived = callback
}

// Start begins listening for orchestration tasks.
func (d *OrchestratorTaskDispatcher) Start(ctx context.Context) error {
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
func (d *OrchestratorTaskDispatcher) Stop() {
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
func (d *OrchestratorTaskDispatcher) IsRunning() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.running
}

// GetQueueDepth returns the current pending queue depth by querying the database directly.
func (d *OrchestratorTaskDispatcher) GetQueueDepth() float64 {
	ctx := context.Background()
	var count int
	if err := d.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orchestrated_task_queue WHERE status = 'pending'
	`).Scan(&count); err != nil {
		logging.Logger.Error().Err(err).Msg("dispatcher failed to query queue depth")
		return 0
	}
	return float64(count)
}

// GetStats returns current dispatcher statistics including queue depth, rates, and latencies.
// This collects metrics from both Prometheus counters/histograms and direct database queries.
func (d *OrchestratorTaskDispatcher) GetStats(ctx context.Context) (*DispatcherStats, error) {
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
func (d *OrchestratorTaskDispatcher) run(ctx context.Context) {
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
func (d *OrchestratorTaskDispatcher) handleNotification(notification *pq.Notification) {
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
func (d *OrchestratorTaskDispatcher) pollPendingTasks(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	// Sample queue depth for metrics
	d.sampleQueueDepth(ctx)

	// Find pending tasks that haven't been claimed
	query := `
		SELECT queue_id, task_id, profile, workspace, target_implementation
		FROM orchestrated_task_queue
		WHERE status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at ASC
		LIMIT 10
	`

	rows, err := d.db.QueryContext(ctx, query)
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
func (d *OrchestratorTaskDispatcher) sampleQueueDepth(ctx context.Context) {
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
func (d *OrchestratorTaskDispatcher) ClaimTask(ctx context.Context, queueID, orchestratorID string) error {
	startTime := time.Now()

	query := `
		UPDATE orchestrated_task_queue
		SET status = 'claimed', claimed_by = $2, claimed_at = NOW()
		WHERE queue_id = $1 AND status = 'pending'
	`

	result, err := d.db.ExecContext(ctx, query, queueID, orchestratorID)
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
func (d *OrchestratorTaskDispatcher) UnclaimTask(ctx context.Context, queueID string) error {
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
		WHERE queue_id = $1 AND status = 'claimed'
	`, queueID).Scan(&taskID, &workspace, &retryCount, &maxRetries)
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
		SET status = 'pending',
		    claimed_by = NULL,
		    claimed_at = NULL,
		    retry_count = $2,
		    next_retry_at = NOW() + ($3 || ' seconds')::interval
		WHERE queue_id = $1
	`, queueID, newRetryCount, fmt.Sprintf("%d", backoffSeconds))
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
func (d *OrchestratorTaskDispatcher) moveToDeadLetterTx(ctx context.Context, tx *sql.Tx, queueID, taskID, workspace string, retryCount int) error {
	reason := "Max retries exceeded - failed to deliver to orchestrator"
	attemptCount := retryCount + 1

	// Mark the queue entry as failed
	_, err := tx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'failed', error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`, queueID, reason)
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
func (d *OrchestratorTaskDispatcher) RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error) {
	query := `
		SELECT queue_id
		FROM orchestrated_task_queue
		WHERE status = 'claimed' AND claimed_at < NOW() - $1::interval
		ORDER BY claimed_at ASC
		LIMIT 50
	`

	rows, err := d.db.QueryContext(ctx, query, fmt.Sprintf("%d seconds", int(threshold.Seconds())))
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
func (d *OrchestratorTaskDispatcher) CompleteTask(ctx context.Context, queueID string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = 'completed', completed_at = NOW()
		WHERE queue_id = $1
	`

	_, err := d.db.ExecContext(ctx, query, queueID)
	if err != nil {
		return err
	}

	// Track successful completion
	d.metrics.RecordTaskCompleted()

	return nil
}

// FailTask marks a task as failed.
func (d *OrchestratorTaskDispatcher) FailTask(ctx context.Context, queueID, errorMsg string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = 'failed', error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`

	_, err := d.db.ExecContext(ctx, query, queueID, errorMsg)
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
func (d *OrchestratorTaskDispatcher) CompleteTaskByTaskID(ctx context.Context, taskID string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = 'completed', completed_at = NOW()
		WHERE task_id = $1 AND status IN ('pending', 'claimed')
	`
	_, err := d.db.ExecContext(ctx, query, taskID)
	return err
}

// FailTaskByTaskID marks orchestrated_task_queue row(s) for this taskID as
// failed with the given reason. Called when a task itself is failed or
// cancelled so the queue entry is retired rather than recovered by stale-claim
// sweeps. Idempotent.
func (d *OrchestratorTaskDispatcher) FailTaskByTaskID(ctx context.Context, taskID, errorMsg string) error {
	query := `
		UPDATE orchestrated_task_queue
		SET status = 'failed', error_message = $2, completed_at = NOW()
		WHERE task_id = $1 AND status IN ('pending', 'claimed')
	`
	_, err := d.db.ExecContext(ctx, query, taskID, errorMsg)
	return err
}

// GetTaskDetails retrieves full task details including launch params.
func (d *OrchestratorTaskDispatcher) GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error) {
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
