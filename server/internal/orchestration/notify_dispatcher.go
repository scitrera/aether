package orchestration

import (
	"context"
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
)

// NotifyTaskDispatcher listens for orchestration tasks via PostgreSQL NOTIFY
// and delivers them to connected orchestrators via a callback.
// For contexts where a NOTIFY connection is undesirable (lite mode, SQLite),
// use PollingTaskDispatcher instead, which uses the same SQL table but wakes
// up solely on a poll ticker rather than a pq.Listener.
type NotifyTaskDispatcher struct {
	// taskStore is the tasks domain Store (internal/storage/tasks). All
	// orchestrated_task_queue reads and writes go through this handle —
	// the dispatcher no longer holds a raw *sql.DB.
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
// taskStore is the tasks domain Store used for all orchestrated_task_queue
// reads and writes. The dispatcher no longer needs a raw *sql.DB handle.
func NewNotifyTaskDispatcher(
	taskStore taskstore.Store,
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
		taskStore:      taskStore,
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

// GetQueueDepth returns the current pending queue depth by querying the store.
func (d *NotifyTaskDispatcher) GetQueueDepth() float64 {
	ctx := context.Background()
	count, err := d.taskStore.CountPendingQueueEntries(ctx)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("dispatcher failed to query queue depth")
		return 0
	}
	return float64(count)
}

// GetStats returns current dispatcher statistics including queue depth, rates, and latencies.
// This collects metrics from both Prometheus counters/histograms and direct database queries.
func (d *NotifyTaskDispatcher) GetStats(ctx context.Context) (*DispatcherStats, error) {
	stats := &DispatcherStats{}

	// Queue depth from store query
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

	entries, err := d.taskStore.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		if ctx.Err() == nil {
			logging.Logger.Error().Err(err).Msg("dispatcher failed to poll tasks")
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

		logging.Logger.Info().Str("task_id", task.TaskID).Str("profile", task.Profile).Msg("dispatcher polled pending task")

		d.mu.RLock()
		callback := d.onTaskReceived
		d.mu.RUnlock()
		if callback != nil {
			callback(task)
		}
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

	claimed, err := d.taskStore.ClaimQueueEntry(ctx, queueID, orchestratorID)
	if err != nil {
		return err
	}

	if !claimed {
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
//
// All mutations (queue update, DLQ insert, audit event) go through the
// tasks.Store transactional methods so the underlying *sql.Tx is never
// leaked outside the store — this is the Stage 2 StoreTx discipline.
func (d *NotifyTaskDispatcher) UnclaimTask(ctx context.Context, queueID string) error {
	tx, err := d.taskStore.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Fetch current state within the transaction
	taskID, workspace, retryCount, maxRetries, err := d.taskStore.QueryQueueEntryForUnclaimTx(ctx, tx, queueID)
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

	if err := d.taskStore.UpdateQueueEntryForRetryTx(ctx, tx, queueID, newRetryCount, backoffSeconds); err != nil {
		return fmt.Errorf("update task for retry: %w", err)
	}

	// Record retry_scheduled audit event
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
func (d *NotifyTaskDispatcher) moveToDeadLetterTx(ctx context.Context, tx taskstore.StoreTx, queueID, taskID, workspace string, retryCount int) error {
	reason := "Max retries exceeded - failed to deliver to orchestrator"
	attemptCount := retryCount + 1

	// Mark the queue entry as failed
	if err := d.taskStore.MarkQueueEntryFailedTx(ctx, tx, queueID, reason); err != nil {
		return fmt.Errorf("mark task failed: %w", err)
	}

	// Insert into DLQ
	if err := d.taskStore.InsertDLQEntryTx(ctx, tx, taskID, workspace, reason, attemptCount); err != nil {
		return fmt.Errorf("insert into dlq: %w", err)
	}

	// Record moved_to_dlq audit event
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

	// Track task failure metric
	d.metrics.RecordTaskFailed()

	return nil
}

// RecoverStaleClaims finds tasks stuck in 'claimed' status longer than the given
// threshold and unclaims them. This handles gateway crashes that leave tasks in limbo.
// Returns the number of tasks recovered.
func (d *NotifyTaskDispatcher) RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error) {
	staleIDs, err := d.taskStore.ListStaleClaimedQueueEntries(ctx, threshold, 50)
	if err != nil {
		return 0, fmt.Errorf("query stale claims: %w", err)
	}

	var recovered int
	for _, queueID := range staleIDs {
		if err := d.UnclaimTask(ctx, queueID); err != nil {
			logging.Logger.Error().Err(err).Str("queue_id", queueID).Msg("dispatcher failed to recover stale claim")
			continue
		}

		logging.Logger.Info().Str("queue_id", queueID).Msg("dispatcher recovered stale claim")
		recovered++
	}

	return recovered, nil
}

// CompleteTask marks a task as completed.
func (d *NotifyTaskDispatcher) CompleteTask(ctx context.Context, queueID string) error {
	if err := d.taskStore.CompleteQueueEntry(ctx, queueID); err != nil {
		return err
	}

	// Track successful completion
	d.metrics.RecordTaskCompleted()

	return nil
}

// FailTask marks a task as failed.
func (d *NotifyTaskDispatcher) FailTask(ctx context.Context, queueID, errorMsg string) error {
	if err := d.taskStore.FailQueueEntry(ctx, queueID, errorMsg); err != nil {
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
	return d.taskStore.CompleteQueueEntryByTaskID(ctx, taskID)
}

// FailTaskByTaskID marks orchestrated_task_queue row(s) for this taskID as
// failed with the given reason. Called when a task itself is failed or
// cancelled so the queue entry is retired rather than recovered by stale-claim
// sweeps. Idempotent.
func (d *NotifyTaskDispatcher) FailTaskByTaskID(ctx context.Context, taskID, errorMsg string) error {
	return d.taskStore.FailQueueEntryByTaskID(ctx, taskID, errorMsg)
}

// GetTaskDetails retrieves full task details including launch params.
func (d *NotifyTaskDispatcher) GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error) {
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
