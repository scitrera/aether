// Phase 4 Stage B: task subscription primitive.
//
// This file handles UpstreamMessage.task_subscription_op (wire tag 31). Clients
// SUBSCRIBE to a per-task event stream and the gateway forwards TaskEvent
// messages from the tk::{workspace}::{task_id}::events topic. UNSUBSCRIBE tears
// down the per-task router subscription.
//
// Decisions (recorded in commit message + Stage B report):
//
//  1. Publish point: emit from service-layer methods (TaskAssignmentService
//     lifecycle hooks, handleProgressReport, acl request lifecycle relays).
//     The store stays pure CRUD. This is option (b) per the Stage B prompt.
//
//  2. Recursive subscription: snapshot-at-subscribe. We walk descendants via
//     ListTasksPage(IncludeDescendants=true) at SUBSCRIBE time and subscribe
//     to each known descendant's task-events topic. Children born AFTER
//     SUBSCRIBE are NOT picked up automatically — they are surfaced indirectly
//     via the parent's child_lifecycle events. Documented as a Stage B limit.
//
//  3. Re-authorization: subscribe-time only. The caller's identity and the
//     task's workspace are validated when SUBSCRIBE runs; later events flow
//     without re-checking auth per event (would be expensive and the
//     workspace-tenancy boundary doesn't change for the lifetime of the
//     subscription).
//
//  4. Terminal auto-close: NO. The subscription stays active until the client
//     unsubscribes or disconnects. Final transitions are delivered first.
//
// Authorization mirrors handleTaskOp (authorizeTaskOp). Info-hiding: missing
// or unauthorized tasks return ErrTaskNotFoundOrUnauthorized.
//
// Subscription bookkeeping: each ClientSession owns a taskSubscriptions map
// (subscription_id -> cancellation closure). UnsubscribeAll on session close
// already nils out the parent .subscriptions topics map; the cancellation
// closures we register here live in that same map, keyed by an opaque topic
// alias built from subscription_id, so they are GCed alongside other topic
// subs on disconnect.

package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
	"google.golang.org/protobuf/proto"
)

// maxRecursiveSubscriptionFanout caps how many descendant subscriptions a
// single SUBSCRIBE request may open. Keeps a runaway recursive subscribe from
// exhausting per-session subscription slots; in practice subscribers asking
// for a deep tree should be rare and bounded.
const maxRecursiveSubscriptionFanout = 256

// taskSubscriptionMarkerPrefix is the per-session subscription-map key prefix
// used to store the cancellation closure for a task-events subscription. The
// closure key is unique per subscription (subscription_id-based) so multiple
// SUBSCRIBE calls against the same task don't collide.
const taskSubscriptionMarkerPrefix = "__task_sub__"

// handleTaskSubscriptionOp routes SUBSCRIBE / UNSUBSCRIBE for the per-task
// event stream. The bidi-stream model means we own the response path: write
// a TaskSubscriptionOperationResponse to the originating session, then keep
// pushing TaskEvent deliveries until cancel.
func (s *GatewayServer) handleTaskSubscriptionOp(ctx context.Context, client *ClientSession, op *pb.TaskSubscriptionOperation) {
	if op == nil {
		sendTaskSubscriptionError(client, "", "", "task_subscription_op is required")
		return
	}
	switch op.GetOp() {
	case pb.TaskSubscriptionOperation_SUBSCRIBE:
		s.handleTaskSubscribe(ctx, client, op)
	case pb.TaskSubscriptionOperation_UNSUBSCRIBE:
		s.handleTaskUnsubscribe(ctx, client, op)
	default:
		sendTaskSubscriptionError(client, op.GetClientRequestId(), op.GetTaskId(), "unknown task subscription operation")
	}
}

func (s *GatewayServer) handleTaskSubscribe(ctx context.Context, client *ClientSession, op *pb.TaskSubscriptionOperation) {
	taskID := op.GetTaskId()
	if taskID == "" {
		sendTaskSubscriptionError(client, op.GetClientRequestId(), "", "task_id is required")
		return
	}
	if s.taskStore == nil {
		sendTaskSubscriptionError(client, op.GetClientRequestId(), taskID, "task store not configured")
		return
	}

	// Authorization: same auth surface as handleTaskOp. Info-hiding on miss.
	task, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil || task == nil {
		sendTaskSubscriptionError(client, op.GetClientRequestId(), taskID, ErrTaskNotFoundOrUnauthorized)
		return
	}
	if !s.authorizeTaskOp(ctx, client, task) {
		sendTaskSubscriptionError(client, op.GetClientRequestId(), taskID, ErrTaskNotFoundOrUnauthorized)
		return
	}

	subscriptionID := uuid.NewString()

	// Topic for the primary subscribed task.
	primaryTopic, err := models.TaskEventsTopic(task.Workspace, taskID)
	if err != nil {
		sendTaskSubscriptionError(client, op.GetClientRequestId(), taskID, fmt.Sprintf("invalid task-events topic: %v", err))
		return
	}

	// Collect all topics this subscription registers against. Always
	// includes the primary; recursive subscriptions append descendants
	// (snapshot-at-subscribe). Each topic gets its own router subscription
	// + cancel; we aggregate cancels into a single closure keyed by the
	// subscription id so UNSUBSCRIBE tears them all down at once.
	topics := []string{primaryTopic}
	if op.GetRecursive() {
		descendants, derr := s.collectDescendantTopics(ctx, task)
		if derr != nil {
			logging.Logger.Warn().Err(derr).Str("task_id", taskID).Msg("handleTaskSubscribe: failed to collect descendants for recursive subscription (continuing with primary only)")
		} else {
			topics = append(topics, descendants...)
		}
	}

	cancels := make([]func(), 0, len(topics))
	handler := s.createTaskEventHandler(client, subscriptionID)

	// Subscribe each topic. If ANY individual subscribe fails we roll back
	// the ones we already opened to avoid orphaned consumers.
	for _, topic := range topics {
		var cancel func()
		var subErr error
		if op.GetStartTimestampUnixMs() > 0 {
			// Use timestamp-hint replay when caller asked for cold-start
			// replay. ConsumerName is the subscription id so each subscribe
			// gets its own offset namespace.
			cancel, subErr = s.router.SubscribeExclusiveFromTimestamp(topic, subscriptionID, op.GetStartTimestampUnixMs(), handler)
		} else {
			cancel, subErr = s.router.Subscribe(topic, handler)
		}
		if subErr != nil {
			for _, c := range cancels {
				c()
			}
			sendTaskSubscriptionError(client, op.GetClientRequestId(), taskID, fmt.Sprintf("router subscribe failed: %v", subErr))
			return
		}
		cancels = append(cancels, cancel)
	}

	// Register a single composite cancel under a per-subscription key on the
	// session's subscription map. UnsubscribeAll on disconnect calls it.
	markerKey := taskSubscriptionMarkerPrefix + subscriptionID
	client.AddSubscription(markerKey, func() {
		for _, c := range cancels {
			c()
		}
		topicSubscriptions.Sub(float64(len(cancels)))
	})
	topicSubscriptions.Add(float64(len(cancels)))

	// Ack success.
	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskSubscriptionResponse{
			TaskSubscriptionResponse: &pb.TaskSubscriptionOperationResponse{
				Success:         true,
				ClientRequestId: op.GetClientRequestId(),
				TaskId:          taskID,
				SubscriptionId:  subscriptionID,
			},
		},
	}); err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Str("subscription_id", subscriptionID).Msg("handleTaskSubscribe: failed to send ack response")
	}
}

func (s *GatewayServer) handleTaskUnsubscribe(_ context.Context, client *ClientSession, op *pb.TaskSubscriptionOperation) {
	subscriptionID := op.GetSubscriptionId()
	if subscriptionID == "" {
		sendTaskSubscriptionError(client, op.GetClientRequestId(), op.GetTaskId(), "subscription_id is required for UNSUBSCRIBE")
		return
	}
	markerKey := taskSubscriptionMarkerPrefix + subscriptionID
	// RemoveSubscription on a missing key is a no-op; we surface a friendly
	// success either way so clients can be idempotent on cleanup.
	client.RemoveSubscription(markerKey)
	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskSubscriptionResponse{
			TaskSubscriptionResponse: &pb.TaskSubscriptionOperationResponse{
				Success:         true,
				ClientRequestId: op.GetClientRequestId(),
				TaskId:          op.GetTaskId(),
				SubscriptionId:  subscriptionID,
			},
		},
	}); err != nil {
		logging.Logger.Warn().Err(err).Str("subscription_id", subscriptionID).Msg("handleTaskUnsubscribe: failed to send ack response")
	}
}

// collectDescendantTopics walks the descendants of `parent` via
// ListTasksPage(IncludeDescendants=true) and returns the task-events topic
// for each. Hard-capped at maxRecursiveSubscriptionFanout; over the cap the
// extra rows are dropped and a warning is logged.
func (s *GatewayServer) collectDescendantTopics(ctx context.Context, parent *tasks.Task) ([]string, error) {
	if parent == nil || parent.TaskID == "" || parent.Workspace == "" {
		return nil, nil
	}
	filter := &tasks.TaskFilter{
		Workspace:          parent.Workspace,
		ParentTaskID:       parent.TaskID,
		IncludeDescendants: true,
		Limit:              maxRecursiveSubscriptionFanout + 1,
	}
	rows, _, err := s.taskStore.ListTasksPage(ctx, filter)
	if err != nil {
		return nil, err
	}
	if len(rows) > maxRecursiveSubscriptionFanout {
		logging.Logger.Warn().
			Str("task_id", parent.TaskID).
			Int("descendant_count", len(rows)).
			Int("cap", maxRecursiveSubscriptionFanout).
			Msg("collectDescendantTopics: truncating recursive subscription fan-out")
		rows = rows[:maxRecursiveSubscriptionFanout]
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if row == nil || row.TaskID == parent.TaskID {
			continue
		}
		t, err := models.TaskEventsTopic(row.Workspace, row.TaskID)
		if err != nil {
			continue
		}
		out = append(out, t)
	}
	return out, nil
}

// createTaskEventHandler returns the per-subscription handler the router
// invokes for each TaskEvent message published on a subscribed topic.
// The handler unmarshals the bytes, stamps the subscription_id, and delivers
// the event downstream via the session's deliveryCh. Self-echo, dedup, etc.
// are not required for Stage B — the router-side consumer already handles
// at-least-once delivery semantics.
func (s *GatewayServer) createTaskEventHandler(client *ClientSession, subscriptionID string) func([]byte) {
	client.identityMu.RLock()
	identStr := client.Identity.String()
	client.identityMu.RUnlock()
	return func(payload []byte) {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("identity", identStr).Str("subscription_id", subscriptionID).Msg("panic in task-event handler")
			}
		}()
		var evt pb.TaskEvent
		if err := proto.Unmarshal(payload, &evt); err != nil {
			logging.Logger.Warn().Err(err).Str("identity", identStr).Str("subscription_id", subscriptionID).Msg("createTaskEventHandler: failed to unmarshal TaskEvent")
			return
		}
		// Stamp the subscription id so multi-subscribe clients can demux on
		// receive. The payload itself was published without it (one publish
		// fans out to N subscribers via the shared topic).
		evt.SubscriptionId = subscriptionID
		client.Deliver(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TaskEvent{
				TaskEvent: &evt,
			},
		})
	}
}

// publishTaskEventBytes serializes evt and forwards to the router for the
// canonical tk::{workspace}::{task_id}::events topic. Best-effort: any failure
// is logged and swallowed so the originating lifecycle / progress / authority
// transition never blocks on publish failure.
func (s *GatewayServer) publishTaskEventBytes(ctx context.Context, workspace, taskID string, evt *pb.TaskEvent) error {
	if s.router == nil || workspace == "" || taskID == "" || evt == nil {
		return nil
	}
	topic, err := models.TaskEventsTopic(workspace, taskID)
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(evt)
	if err != nil {
		return err
	}
	// Use the publish circuit breaker when available; otherwise call the
	// router directly. Test scaffolding constructs gateways without a
	// breaker, so a nil guard keeps unit tests working.
	if s.publishBreaker != nil {
		return s.publishBreaker.Execute(func() error {
			return s.router.Publish(ctx, topic, payload)
		})
	}
	return s.router.Publish(ctx, topic, payload)
}

// PublishTaskEvent satisfies orchestration.TaskEventPublisher so the gateway
// can wire itself into TaskAssignmentService transitions. The interface lives
// in the orchestration package; the method body in this file makes the
// gateway-side router/topic conversion concrete.
func (s *GatewayServer) PublishTaskEvent(ctx context.Context, workspace, taskID string, event *pb.TaskEvent) error {
	return s.publishTaskEventBytes(ctx, workspace, taskID, event)
}

// publishProgressTaskEvent emits a TaskProgressEvent on the task-events topic
// for the given task_id when handleProgressReport fans out a progress report
// that targets a known task. Best-effort: callers should not propagate the
// error.
func (s *GatewayServer) publishProgressTaskEvent(ctx context.Context, taskID, workspace string, report *pb.ProgressReport) {
	if taskID == "" || workspace == "" || report == nil {
		return
	}
	evt := &pb.TaskEvent{
		TaskId:          taskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       workspace,
		Event: &pb.TaskEvent_Progress{
			Progress: &pb.TaskProgressEvent{
				State:    report.GetState(),
				Progress: report.GetCompletion(),
				Message:  report.GetSummary(),
				Metadata: report.GetMetadata(),
			},
		},
	}
	if err := s.publishTaskEventBytes(ctx, workspace, taskID, evt); err != nil {
		logging.Logger.Debug().Err(err).Str("task_id", taskID).Msg("publishProgressTaskEvent: publish failed (non-fatal)")
	}
}

// publishAuthorityRequestTaskEvent emits a TaskAuthorityRequestEventRelay on
// the task-events topic of the request's bound task. Called from the
// authority-request lifecycle path AFTER the gateway emits the per-session
// AuthorityRequestEvent to the originator. Best-effort.
func (s *GatewayServer) publishAuthorityRequestTaskEvent(ctx context.Context, taskID, workspace string, inner *pb.AuthorityRequestEvent) {
	if taskID == "" || workspace == "" || inner == nil {
		return
	}
	evt := &pb.TaskEvent{
		TaskId:          taskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       workspace,
		Event: &pb.TaskEvent_AuthorityRequest{
			AuthorityRequest: &pb.TaskAuthorityRequestEventRelay{
				Event: inner,
			},
		},
	}
	if err := s.publishTaskEventBytes(ctx, workspace, taskID, evt); err != nil {
		logging.Logger.Debug().Err(err).Str("task_id", taskID).Msg("publishAuthorityRequestTaskEvent: publish failed (non-fatal)")
	}
}

// sendTaskSubscriptionError pushes an error response on the originating
// session. Shared by SUBSCRIBE / UNSUBSCRIBE for consistent shape.
func sendTaskSubscriptionError(client *ClientSession, clientRequestID, taskID, errMsg string) {
	if client == nil {
		return
	}
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskSubscriptionResponse{
			TaskSubscriptionResponse: &pb.TaskSubscriptionOperationResponse{
				Success:         false,
				Error:           errMsg,
				ClientRequestId: clientRequestID,
				TaskId:          taskID,
			},
		},
	})
}
