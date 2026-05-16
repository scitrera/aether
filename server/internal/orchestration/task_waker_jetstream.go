// Phase 4 (Slice 4B): JetStream-driven task waker.
//
// This is the cluster-mode replacement for the polling subset of
// task_waker.go. Instead of scanning postgres on a timer for the two
// event-driven wake paths (authority-resolved + INPUT-message-arrival),
// JetStreamTaskWaker subscribes to two push-based JetStream consumers and
// reacts when the upstream emits an event.
//
// Wake paths covered by THIS file:
//
//  1. Authority-resolved wake:
//     Subscribes to subject filter "authreq.>" on the "authreq" stream
//     (provisioned by JetStreamAuthorityLifecycle in Task #11). Each
//     event is a JSON AuthorityRequestLifecycleEvent. On
//     status_to=approved we call ResumeTask for every task whose
//     WaitSpec.AuthorityRequestID matches; on denied/expired/cancelled we
//     call FailTask.
//
//  2. INPUT-message wake (the deferred-gap closer from §12 of
//     docs/agentic-fabric-protocol-guide.md):
//     Subscribes to subject filter "tk.*.*.input" on the "tk" stream
//     (provisioned by JetStreamRouter). The convention is that when the
//     gateway delivers an INPUT-typed message to a task in
//     waiting_input, the gateway ALSO publishes a copy of that message
//     to the aether topic "tk::{ws}::{task_id}::input" (NATS subject
//     "tk.{ws-esc}.{task_id-esc}.input"). The payload is a JSON
//     TaskInputWakeEvent. We look up the task by id, evaluate
//     WaitSpec.InputMatch against the inbound event's metadata, and
//     ResumeTask when matched.
//
// Wake paths NOT covered (intentionally left on the timer-driven
// task_waker.go scanner):
//
//   - dependency reconciliation
//   - scheduled-timer wake (hibernation)
//   - timeout-to-fail
//   - authority-request sweep (SweepExpiredAuthorityRequests)
//
// Rationale (Option B from the Slice 4B design notes): the dependency
// path is already event-driven via wakeDependents on the
// TaskAssignmentService side, and the remaining timer paths benefit from
// a simple periodic scanner. The JetStream waker COMPOSES with the
// existing TaskWaker — production wiring may run both concurrently with
// no interaction (idempotent state-machine transitions guarantee a
// double-fire is a no-op).
//
// Multi-gateway safety: all wake transitions go through
// TaskAssignmentService methods which are guarded at the storage layer
// (ValidateTransition rejects illegal sources). A given event landing on
// multiple gateways means whichever wins the SQL race performs the
// transition; the others observe "task no longer in waiting_*" and
// silently no-op. No cross-gateway coordination required.

package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/tasks"
)

// JetStream stream names + subject filters used by the waker. The "authreq"
// and "tk" streams are both provisioned upstream (JetStreamAuthorityLifecycle
// + JetStreamRouter respectively); this file references them by name only.
const (
	// authorityRequestStreamName matches acl.AuthorityRequestsStream;
	// duplicated as a local const to avoid pulling the acl package into the
	// hot path beyond the event-type import.
	authorityRequestStreamName       = "authreq"
	authorityRequestEventsFilterAll  = "authreq.>"
	authorityRequestConsumerBaseName = "task_waker_authreq"

	// taskStreamName matches the "tk" stream provisioned by JetStreamRouter.
	taskStreamName = "tk"
	// taskInputFilterAll matches "tk.<ws>.<task_id>.input" — three concrete
	// tokens between the prefix and the leaf-subject "input".
	taskInputFilterAll           = "tk.*.*.input"
	taskInputConsumerBaseName    = "task_waker_tk_input"
	taskInputAetherSubjectSuffix = "input"

	// defaultWakerAckWait bounds JetStream redelivery on unacked messages.
	defaultWakerAckWait = 30 * time.Second
)

// TaskInputWakeEvent is the JSON payload published to
// "tk::{ws}::{task_id}::input" by the gateway when an INPUT-typed message is
// delivered to a task that is parked in waiting_input. The waker decodes
// these and evaluates WaitSpec.InputMatch against the event metadata.
//
// Producers (gateway message-delivery path, future task) MUST stamp at
// least TaskID + Workspace; Metadata is the comparison surface for
// WaitSpec.InputMatch (each k/v in InputMatch must be present in Metadata
// with the same value). MessageType / SenderIdentity are recorded for
// audit/debug visibility but are not consulted by the waker.
type TaskInputWakeEvent struct {
	TaskID          string            `json:"task_id"`
	Workspace       string            `json:"workspace"`
	MessageType     string            `json:"message_type,omitempty"`
	SenderIdentity  string            `json:"sender_identity,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	EmittedAtUnixMs int64             `json:"emitted_at_unix_ms,omitempty"`
}

// taskWakerService is the minimal slice of *TaskAssignmentService the
// JetStream waker consumes. Mirrored as an interface (not a concrete type)
// so tests can substitute a recording fake without standing up the full
// service.
type taskWakerService interface {
	ResumeTask(ctx context.Context, taskID string, to tasks.TaskStatus) error
	FailTask(ctx context.Context, taskID string, errorMsg string) error
}

// Compile-time conformance: the real service satisfies the waker interface.
var _ taskWakerService = (*TaskAssignmentService)(nil)

// JetStreamTaskWaker drives the two event-driven wake paths (authority
// resolution, inbound INPUT message) via push-based JetStream consumers.
// It is independent of the timer-based TaskWaker and may be run alongside
// it in cluster mode.
//
// Lifecycle: NewJetStreamTaskWaker only constructs the value; the consumers
// are created and started inside Run(ctx). Run blocks until ctx is done,
// then tears down both consumers and waits for the dispatcher goroutines
// to drain. The graceful-shutdown shape matches sibling jetstream
// dispatchers in this package.
type JetStreamTaskWaker struct {
	js          jetstream.JetStream
	taskStore   taskstore.Store
	taskService taskWakerService

	// consumerSuffix lets multiple instances (or tests) avoid sharing a
	// durable consumer name. When empty, durable names default to the
	// "task_waker_*" constants above, which is fine for a single-cluster
	// production deployment.
	consumerSuffix string

	// ackWait is the JetStream redelivery interval for unacked events.
	ackWait time.Duration
}

// NewJetStreamTaskWaker constructs a JetStream-driven waker. The returned
// value is dormant until Run(ctx) is invoked. js must be a live JetStream
// context whose streams ("authreq" and "tk") were provisioned by the
// upstream wiring (Task #10 + Task #11). taskStore + taskService are the
// same dependencies the polling TaskWaker takes; consumerSuffix may be
// empty for production and is used by tests to namespace durable consumers
// across parallel runs.
func NewJetStreamTaskWaker(
	js jetstream.JetStream,
	taskStore taskstore.Store,
	taskService taskWakerService,
	consumerSuffix string,
) *JetStreamTaskWaker {
	return &JetStreamTaskWaker{
		js:             js,
		taskStore:      taskStore,
		taskService:    taskService,
		consumerSuffix: consumerSuffix,
		ackWait:        defaultWakerAckWait,
	}
}

// Run blocks until ctx is cancelled. It creates the two push-based
// JetStream consumers and dispatches incoming events through the per-path
// handler. On ctx cancellation each iterator is stopped and the helper
// goroutines drain before Run returns.
func (w *JetStreamTaskWaker) Run(ctx context.Context) {
	if w.js == nil || w.taskStore == nil || w.taskService == nil {
		logging.Logger.Warn().Msg("jetstream task waker: missing js/store/service; not starting")
		return
	}

	// Use Background for consumer iterators — the per-message handlers
	// receive the user-supplied ctx via the wrapper below.
	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startCancel()

	authStop, err := w.startAuthorityConsumer(startCtx, ctx)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("jetstream task waker: failed to start authority consumer")
		return
	}
	defer authStop()

	inputStop, err := w.startInputConsumer(startCtx, ctx)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("jetstream task waker: failed to start input consumer")
		return
	}
	defer inputStop()

	logging.Logger.Info().Msg("jetstream task waker: running (authority + input consumers active)")
	<-ctx.Done()
	logging.Logger.Info().Msg("jetstream task waker: shutting down")
}

// startAuthorityConsumer creates the push consumer on the authreq stream
// and spawns the dispatcher goroutine. Returns a stop func that must be
// invoked exactly once during shutdown.
func (w *JetStreamTaskWaker) startAuthorityConsumer(startCtx context.Context, runCtx context.Context) (func(), error) {
	cfg := jetstream.ConsumerConfig{
		Durable:       w.authorityConsumerName(),
		FilterSubject: authorityRequestEventsFilterAll,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.ackWait,
	}
	cons, err := w.js.CreateOrUpdateConsumer(startCtx, authorityRequestStreamName, cfg)
	if err != nil {
		return nil, fmt.Errorf("authority consumer create: %w", err)
	}
	return w.runConsumer(runCtx, cons, "authority", w.handleAuthorityEvent)
}

// startInputConsumer creates the push consumer on the tk stream filtered
// to "tk.*.*.input" — exactly the leaf subject used by the gateway when it
// echoes an INPUT-typed delivery for a waiting_input task.
func (w *JetStreamTaskWaker) startInputConsumer(startCtx context.Context, runCtx context.Context) (func(), error) {
	cfg := jetstream.ConsumerConfig{
		Durable:       w.inputConsumerName(),
		FilterSubject: taskInputFilterAll,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.ackWait,
	}
	cons, err := w.js.CreateOrUpdateConsumer(startCtx, taskStreamName, cfg)
	if err != nil {
		return nil, fmt.Errorf("input consumer create: %w", err)
	}
	return w.runConsumer(runCtx, cons, "input", w.handleInputEvent)
}

// runConsumer is the shared dispatch loop: open a messages iterator,
// invoke the handler per message, ack, and stop cleanly on ctx
// cancellation. The returned stop func is idempotent.
func (w *JetStreamTaskWaker) runConsumer(
	runCtx context.Context,
	cons jetstream.Consumer,
	label string,
	handle func(ctx context.Context, data []byte),
) (func(), error) {
	msgs, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("%s messages iterator: %w", label, err)
	}

	var (
		stopOnce sync.Once
		doneCh   = make(chan struct{})
	)

	go func() {
		defer close(doneCh)
		// Ensure iterator releases on context cancel from the outside.
		go func() {
			<-runCtx.Done()
			msgs.Stop()
		}()
		for {
			msg, err := msgs.Next()
			if err != nil {
				// Iterator is done (Stop called or stream closed).
				return
			}
			handle(runCtx, msg.Data())
			if ackErr := msg.Ack(); ackErr != nil {
				logging.Logger.Warn().
					Err(ackErr).
					Str("consumer", label).
					Msg("jetstream task waker: ack failed (non-fatal)")
			}
		}
	}()

	stop := func() {
		stopOnce.Do(func() {
			msgs.Stop()
			<-doneCh
		})
	}
	return stop, nil
}

// handleAuthorityEvent processes a single AuthorityRequestLifecycleEvent.
// We only care about transitions to a terminal authority-request status
// (approved/denied/expired/cancelled). Pending or non-resolved events are
// observed but generate no task transitions.
func (w *JetStreamTaskWaker) handleAuthorityEvent(ctx context.Context, data []byte) {
	var evt acl.AuthorityRequestLifecycleEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		logging.Logger.Warn().Err(err).Msg("jetstream task waker: authority event unmarshal failed")
		return
	}
	if evt.RequestID == "" {
		return
	}

	// Map status_to → wake action. Only terminal states trigger work.
	switch evt.StatusTo {
	case acl.AuthorityRequestStatusApproved:
		w.wakeTasksOnAuthorityApproval(ctx, &evt)
	case acl.AuthorityRequestStatusDenied,
		acl.AuthorityRequestStatusExpired,
		acl.AuthorityRequestStatusCancelled:
		w.failTasksOnAuthorityRejection(ctx, &evt)
	default:
		// pending / unknown: nothing to do.
	}
}

// wakeTasksOnAuthorityApproval iterates the current waiting-task batch and
// resumes any whose WaitSpec.AuthorityRequestID matches the approved
// request. Multi-task match is supported (e.g. two parallel callers parked
// on the same delegated approval).
func (w *JetStreamTaskWaker) wakeTasksOnAuthorityApproval(ctx context.Context, evt *acl.AuthorityRequestLifecycleEvent) {
	matches := w.tasksWaitingOnAuthorityRequest(ctx, evt.RequestID)
	for _, t := range matches {
		if err := w.taskService.ResumeTask(ctx, t.TaskID, tasks.TaskStatusRunning); err != nil {
			logging.Logger.Warn().Err(err).
				Str("task_id", t.TaskID).
				Str("authority_request_id", evt.RequestID).
				Msg("jetstream task waker: authority approval resume failed (non-fatal)")
			continue
		}
		logging.Logger.Info().
			Str("task_id", t.TaskID).
			Str("authority_request_id", evt.RequestID).
			Str("grant_id", evt.GrantID).
			Msg("jetstream task waker: resumed task on authority approval")
	}
}

// failTasksOnAuthorityRejection fails each task waiting on the request with
// a reason derived from the event's status_to (and resolution_reason when
// the embedded request payload carries one).
func (w *JetStreamTaskWaker) failTasksOnAuthorityRejection(ctx context.Context, evt *acl.AuthorityRequestLifecycleEvent) {
	reason := authorityEventFailureReason(evt)
	matches := w.tasksWaitingOnAuthorityRequest(ctx, evt.RequestID)
	for _, t := range matches {
		if err := w.taskService.FailTask(ctx, t.TaskID, reason); err != nil {
			logging.Logger.Warn().Err(err).
				Str("task_id", t.TaskID).
				Str("authority_request_id", evt.RequestID).
				Msg("jetstream task waker: authority rejection fail failed (non-fatal)")
			continue
		}
		logging.Logger.Info().
			Str("task_id", t.TaskID).
			Str("authority_request_id", evt.RequestID).
			Str("status", string(evt.StatusTo)).
			Str("reason", reason).
			Msg("jetstream task waker: failed task on authority rejection")
	}
}

// tasksWaitingOnAuthorityRequest scans the waiting-task list (bounded) for
// rows in waiting_authority whose WaitSpec.AuthorityRequestID matches the
// supplied id. We deliberately reuse ListWaitingTasks (already implemented
// across postgres + sqlite) rather than introducing a new index — the
// waiting-task population is small by design (typical: <100) and a linear
// scan is O(n) over a tiny set.
func (w *JetStreamTaskWaker) tasksWaitingOnAuthorityRequest(ctx context.Context, requestID string) []*tasks.Task {
	const listLimit = 500
	rows, err := w.taskStore.ListWaitingTasks(ctx, listLimit)
	if err != nil {
		logging.Logger.Warn().Err(err).Msg("jetstream task waker: list waiting tasks failed (non-fatal)")
		return nil
	}
	var out []*tasks.Task
	for _, t := range rows {
		if t == nil || t.WaitSpec == nil {
			continue
		}
		if t.Status != tasks.TaskStatusWaitingAuthority {
			continue
		}
		if t.WaitSpec.Reason != tasks.WaitReasonAuthority {
			continue
		}
		if t.WaitSpec.AuthorityRequestID != requestID {
			continue
		}
		out = append(out, t)
	}
	return out
}

// authorityEventFailureReason formats the FailTask reason from a lifecycle
// event. Mirrors authorityRequestFailureReason in task_waker.go but works
// against the event payload directly (the embedded Request may be nil if
// the producer trimmed it for size).
func authorityEventFailureReason(evt *acl.AuthorityRequestLifecycleEvent) string {
	if evt == nil {
		return "authority request resolved without approval"
	}
	if evt.Request != nil && evt.Request.ResolutionReason != "" {
		return fmt.Sprintf("authority request %s: %s", evt.StatusTo, evt.Request.ResolutionReason)
	}
	return fmt.Sprintf("authority request %s", evt.StatusTo)
}

// handleInputEvent processes a single TaskInputWakeEvent. The waker looks
// up the named task, verifies it is still in waiting_input, evaluates
// WaitSpec.InputMatch against the inbound metadata, and resumes when
// matched.
func (w *JetStreamTaskWaker) handleInputEvent(ctx context.Context, data []byte) {
	var evt TaskInputWakeEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		logging.Logger.Warn().Err(err).Msg("jetstream task waker: input event unmarshal failed")
		return
	}
	if evt.TaskID == "" {
		return
	}

	task, err := w.taskStore.GetTask(ctx, evt.TaskID)
	if err != nil {
		logging.Logger.Warn().Err(err).
			Str("task_id", evt.TaskID).
			Msg("jetstream task waker: input event task lookup failed (non-fatal)")
		return
	}
	if task == nil {
		// Task no longer exists (terminal cleanup raced our event); silent.
		return
	}
	if task.Status != tasks.TaskStatusWaitingInput {
		// Concurrent transition (e.g. timeout fired first); nothing to do.
		return
	}
	if task.WaitSpec == nil || task.WaitSpec.Reason != tasks.WaitReasonInput {
		// Defensive: should not happen if status==waiting_input, but guard
		// against malformed rows rather than panicking on the nil deref.
		return
	}

	if !inputMatchesWaitSpec(task.WaitSpec.InputMatch, evt.Metadata) {
		logging.Logger.Debug().
			Str("task_id", evt.TaskID).
			Msg("jetstream task waker: input event metadata did not match WaitSpec.InputMatch")
		return
	}

	if err := w.taskService.ResumeTask(ctx, task.TaskID, tasks.TaskStatusRunning); err != nil {
		logging.Logger.Warn().Err(err).
			Str("task_id", task.TaskID).
			Msg("jetstream task waker: input resume failed (non-fatal)")
		return
	}
	logging.Logger.Info().
		Str("task_id", task.TaskID).
		Str("message_type", evt.MessageType).
		Str("sender", evt.SenderIdentity).
		Msg("jetstream task waker: resumed task on inbound input message")
}

// inputMatchesWaitSpec returns true when every k/v in matchSpec is also
// present (with the same value) in eventMeta. An empty matchSpec means
// "wake on any input message" — consistent with the WaitSpec docstring
// (InputMatch is optional). A nil eventMeta against a non-empty matchSpec
// can never match.
func inputMatchesWaitSpec(matchSpec, eventMeta map[string]string) bool {
	if len(matchSpec) == 0 {
		return true
	}
	if eventMeta == nil {
		return false
	}
	for k, v := range matchSpec {
		got, ok := eventMeta[k]
		if !ok || got != v {
			return false
		}
	}
	return true
}

// authorityConsumerName / inputConsumerName build durable consumer names,
// optionally suffixed for test isolation.
func (w *JetStreamTaskWaker) authorityConsumerName() string {
	return suffixedName(authorityRequestConsumerBaseName, w.consumerSuffix)
}

func (w *JetStreamTaskWaker) inputConsumerName() string {
	return suffixedName(taskInputConsumerBaseName, w.consumerSuffix)
}

func suffixedName(base, suffix string) string {
	if suffix == "" {
		return base
	}
	return base + "_" + strings.TrimSpace(suffix)
}
