package orchestration

// JetStreamTaskDispatcher is a JetStream work-queue consumer implementation of
// TaskDispatcher. It publishes task notifications to a JetStream stream with
// WorkQueuePolicy (each message is delivered to exactly one consumer and then
// removed after ACK), providing exactly-once-claimed semantics across multiple
// gateways competing for work.
//
// Work-queue topology:
//   - Stream: "tasks_queue", Retention=WorkQueuePolicy, Subjects=["tasks.queue.>"]
//   - One shared durable push consumer (Durable="tasks_queue_dispatcher") shared
//     by all gateway instances. NATS WorkQueuePolicy load-balances delivery across
//     all connected subscribers using the same durable, and removes the message
//     after any one of them ACKs it. This is the standard NATS "competing consumers
//     on a work queue" pattern. WorkQueuePolicy rejects multiple consumers with
//     different durable names (server error 10100).
//
// The JetStream dispatcher does NOT use the SQL orchestrated_task_queue for its
// wake-up path (that is the polling/notify dispatchers' job). It does still
// delegate ClaimTask / UnclaimTask / CompleteTask / FailTask /
// GetTaskDetails / RecoverStaleClaims to the same tasks.Store so that the
// downstream orchestration logic (TaskAssignmentService, admin queries) remains
// identical regardless of which dispatcher backend is active.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
)

// Compile-time interface check.
var _ TaskDispatcher = (*JetStreamTaskDispatcher)(nil)

const (
	// jsStreamName is the JetStream stream name for the work queue.
	jsStreamName = "tasks_queue"

	// jsSubjectPrefix is the subject prefix; individual tasks are published to
	// "tasks.queue.<queueID>".
	jsSubjectPrefix = "tasks.queue."

	// jsSubjectWildcard is the subject filter for the stream / consumer.
	jsSubjectWildcard = "tasks.queue.>"

	// jsAckWait is how long the server waits for an ACK before redelivering.
	jsAckWait = 30 * time.Second

	// jsMaxDeliver is the maximum number of delivery attempts before the
	// server terminates the message (moves to no-ack / drops for work-queue).
	jsMaxDeliver = 5
)

// jsNakBackoffs is the per-attempt NaK delay sequence. The JetStream server
// uses BackOff durations in order; the last value is repeated for all
// subsequent attempts.
var jsNakBackoffs = []time.Duration{
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
}

// jsConsumerName is the shared durable consumer name used by all gateway
// instances. NATS WorkQueuePolicy with a single shared durable delivers each
// message to exactly one of the connected subscribers (load-balanced), and
// removes the message after ACK. This is the standard "competing consumers on a
// work queue" pattern in NATS JetStream. Using different durable names is NOT
// supported on WorkQueuePolicy streams (the server rejects it with error 10100).
const jsConsumerName = "tasks_queue_dispatcher"

// JetStreamTaskDispatcher satisfies TaskDispatcher using NATS JetStream as the
// transport for task notifications. SQL task-state mutations (claim, unclaim,
// complete, fail, retry, DLQ) are still delegated to tasks.Store.
type JetStreamTaskDispatcher struct {
	js        jetstream.JetStream
	gatewayID string
	replicas  int
	taskStore taskstore.Store

	onTaskReceived func(task *OrchestrationTaskNotification)

	mu         sync.RWMutex
	running    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	instanceID string

	// cons is the active push consumer subscription handle, set in Start and
	// stopped in Stop.
	consCtx jetstream.ConsumeContext
}

// NewJetStreamTaskDispatcher creates a JetStreamTaskDispatcher. It
// idempotently creates (or re-opens) the "tasks_queue" stream and the durable
// push consumer named gatewayID.
//
// Parameters:
//   - ctx: used only for the stream / consumer CreateOrUpdate calls; the
//     running goroutine uses its own context derived from Start.
//   - js: JetStream context from the embedded NATS server.
//   - gatewayID: unique name for this gateway instance; becomes the durable
//     consumer name so competing gateways each get their own cursor.
//   - replicas: stream replica count (1 for single-node, 3 for full HA).
//   - taskStore: tasks domain Store for all SQL state mutations.
func NewJetStreamTaskDispatcher(
	ctx context.Context,
	js jetstream.JetStream,
	gatewayID string,
	replicas int,
	taskStore taskstore.Store,
) (*JetStreamTaskDispatcher, error) {
	if js == nil {
		return nil, errors.New("jetstream dispatcher: js is required")
	}
	if gatewayID == "" {
		return nil, errors.New("jetstream dispatcher: gatewayID is required")
	}
	if taskStore == nil {
		return nil, errors.New("jetstream dispatcher: taskStore is required")
	}
	if replicas < 1 {
		replicas = 1
	}

	// Create or update the work-queue stream.
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      jsStreamName,
		Subjects:  []string{jsSubjectWildcard},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		Replicas:  replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream dispatcher: create/update stream: %w", err)
	}

	d := &JetStreamTaskDispatcher{
		js:         js,
		gatewayID:  gatewayID,
		replicas:   replicas,
		taskStore:  taskStore,
		stopCh:     make(chan struct{}),
		instanceID: uuid.New().String()[:8],
	}
	return d, nil
}

// SetCallback sets the callback function invoked when a task notification
// arrives from the JetStream consumer.
func (d *JetStreamTaskDispatcher) SetCallback(callback func(task *OrchestrationTaskNotification)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTaskReceived = callback
}

// Start creates the durable push consumer (idempotent) and begins consuming
// messages from the work-queue stream.
func (d *JetStreamTaskDispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return nil
	}
	d.running = true
	d.mu.Unlock()

	// Fetch (or create) the stream handle.
	stream, err := d.js.Stream(ctx, jsStreamName)
	if err != nil {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
		return fmt.Errorf("jetstream dispatcher: open stream: %w", err)
	}

	// Create or update the shared durable consumer. All gateway instances share
	// the same durable name (jsConsumerName). NATS WorkQueuePolicy load-balances
	// delivery across all connected subscribers and removes the message after one
	// ACK — ensuring exactly-once delivery across the gateway cluster.
	// Using different durable names per gateway is rejected by WorkQueuePolicy
	// (server error 10100: "filtered consumer not unique on workqueue stream").
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       jsConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       jsAckWait,
		MaxDeliver:    jsMaxDeliver,
		BackOff:       jsNakBackoffs,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
		return fmt.Errorf("jetstream dispatcher: create/update consumer: %w", err)
	}

	// Start the push consume loop. Consume returns immediately and delivers
	// messages asynchronously via the handler.
	consCtx, err := cons.Consume(d.handleJSMessage)
	if err != nil {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
		return fmt.Errorf("jetstream dispatcher: consume: %w", err)
	}

	d.mu.Lock()
	d.consCtx = consCtx
	d.mu.Unlock()

	logging.Logger.Info().
		Str("instance", d.instanceID).
		Str("gateway_id", d.gatewayID).
		Msg("jetstream dispatcher started")

	// Watch for stop/ctx cancellation and tear down the consume context.
	d.wg.Add(1)
	go d.watchStop(ctx)

	return nil
}

// watchStop waits for Stop() or ctx cancellation and stops the consumer.
func (d *JetStreamTaskDispatcher) watchStop(ctx context.Context) {
	defer d.wg.Done()
	select {
	case <-ctx.Done():
	case <-d.stopCh:
	}

	d.mu.Lock()
	cc := d.consCtx
	d.mu.Unlock()
	if cc != nil {
		cc.Stop()
	}
}

// Stop gracefully shuts down the dispatcher.
func (d *JetStreamTaskDispatcher) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.running = false
	d.mu.Unlock()

	close(d.stopCh)
	d.wg.Wait()

	logging.Logger.Info().
		Str("instance", d.instanceID).
		Str("gateway_id", d.gatewayID).
		Msg("jetstream dispatcher stopped")
}

// PublishTask publishes a task notification to the JetStream work queue.
// Any gateway that has started a dispatcher and is connected will compete to
// consume the message; WorkQueuePolicy ensures exactly one gateway receives it.
func (d *JetStreamTaskDispatcher) PublishTask(ctx context.Context, task *OrchestrationTaskNotification) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("jetstream dispatcher: marshal task: %w", err)
	}
	subject := jsSubjectPrefix + task.QueueID
	if _, err := d.js.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("jetstream dispatcher: publish task %s: %w", task.QueueID, err)
	}
	return nil
}

// handleJSMessage is the push-consumer handler invoked by the NATS client
// goroutine for each delivered message.
func (d *JetStreamTaskDispatcher) handleJSMessage(msg jetstream.Msg) {
	var task OrchestrationTaskNotification
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		logging.Logger.Error().Err(err).Msg("jetstream dispatcher: failed to decode task notification; terminating message")
		// Permanent failure: bad payload will never be parseable.
		_ = msg.Term()
		return
	}

	logging.Logger.Info().
		Str("queue_id", task.QueueID).
		Str("task_id", task.TaskID).
		Str("profile", task.Profile).
		Msg("jetstream dispatcher received task")

	d.mu.RLock()
	callback := d.onTaskReceived
	d.mu.RUnlock()

	if callback != nil {
		callback(&task)
	}

	// ACK after delivering to callback. The callback is synchronous; any claim
	// contention is handled at the ClaimTask level (SQL CAS), not here.
	if err := msg.Ack(); err != nil {
		logging.Logger.Error().Err(err).Str("queue_id", task.QueueID).Msg("jetstream dispatcher: ack failed")
	}
}

// ---------------------------------------------------------------------------
// TaskDispatcher interface — SQL state mutations delegate to taskStore
// ---------------------------------------------------------------------------

// ClaimTask attempts to claim a task for an orchestrator.
// Returns ErrTaskAlreadyClaimed if another gateway already claimed it.
func (d *JetStreamTaskDispatcher) ClaimTask(ctx context.Context, queueID, orchestratorID string) error {
	claimed, err := d.taskStore.ClaimQueueEntry(ctx, queueID, orchestratorID)
	if err != nil {
		return err
	}
	if !claimed {
		return ErrTaskAlreadyClaimed
	}
	logging.Logger.Info().
		Str("queue_id", queueID).
		Str("orchestrator_id", orchestratorID).
		Msg("jetstream dispatcher claimed task")
	return nil
}

// UnclaimTask releases a claimed task back to pending status for retry.
// Implements the same retry-limit + exponential-backoff + DLQ logic as the
// other dispatchers.
func (d *JetStreamTaskDispatcher) UnclaimTask(ctx context.Context, queueID string) error {
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
func (d *JetStreamTaskDispatcher) moveToDeadLetterTx(ctx context.Context, tx taskstore.StoreTx, queueID, taskID, workspace string, retryCount int) error {
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
func (d *JetStreamTaskDispatcher) CompleteTask(ctx context.Context, queueID string) error {
	return d.taskStore.CompleteQueueEntry(ctx, queueID)
}

// FailTask marks a task as failed with an error message.
func (d *JetStreamTaskDispatcher) FailTask(ctx context.Context, queueID, errorMsg string) error {
	return d.taskStore.FailQueueEntry(ctx, queueID, errorMsg)
}

// GetTaskDetails retrieves full task details including launch params.
func (d *JetStreamTaskDispatcher) GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error) {
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

// RecoverStaleClaims finds tasks stuck in 'claimed' status longer than the
// given threshold and unclaims them. Returns the number of tasks recovered.
func (d *JetStreamTaskDispatcher) RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error) {
	staleIDs, err := d.taskStore.ListStaleClaimedQueueEntries(ctx, threshold, 50)
	if err != nil {
		return 0, fmt.Errorf("query stale claims: %w", err)
	}

	var recovered int
	for _, queueID := range staleIDs {
		if err := d.UnclaimTask(ctx, queueID); err != nil {
			logging.Logger.Error().Err(err).Str("queue_id", queueID).Msg("jetstream dispatcher failed to recover stale claim")
			continue
		}
		logging.Logger.Info().Str("queue_id", queueID).Msg("jetstream dispatcher recovered stale claim")
		recovered++
	}
	return recovered, nil
}
