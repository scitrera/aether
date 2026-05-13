package gateway

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

// envelopeOnce holds a lazily-parsed MessageEnvelope protected by sync.Once.
// A pointer to this struct is stored in envelopeSyncMap keyed by (ptr, len) of
// the raw bytes so all fan-out handlers for the same message share one parse.
type envelopeOnce struct {
	once sync.Once
	env  *pb.MessageEnvelope
	err  error
}

// parse runs proto.Unmarshal exactly once and stores the result.
func (e *envelopeOnce) parse(b []byte) {
	e.once.Do(func() {
		var envelope pb.MessageEnvelope
		if err := proto.Unmarshal(b, &envelope); err != nil {
			e.err = err
			return
		}
		e.env = &envelope
	})
}

// envelopeCacheKey returns a comparable key for a []byte that is stable for
// the lifetime of a single fan-out call (pointer + length).
func envelopeCacheKey(b []byte) [2]uintptr {
	if len(b) == 0 {
		return [2]uintptr{}
	}
	return [2]uintptr{uintptr(unsafe.Pointer(&b[0])), uintptr(len(b))}
}

// subscribeClientToTopic subscribes a client to a shared topic (broadcasts).
// Messages are forwarded to the client's gRPC stream.
// For unique identity topics that need offset tracking, use subscribeClientToTopicExclusive.
func (s *GatewayServer) subscribeClientToTopic(client *ClientSession, topic string) error {
	if client.HasSubscription(topic) {
		return nil // Already subscribed
	}

	cancel, err := s.router.Subscribe(topic, s.createMessageHandler(client))
	if err != nil {
		return err
	}

	client.AddSubscription(topic, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
	return nil
}

// subscribeClientToTopicExclusive subscribes a client to a topic with offset tracking.
// This creates a dedicated consumer that can resume from the last committed offset.
// Use this for unique identity topics (agents, tasks, users) where message replay matters.
func (s *GatewayServer) subscribeClientToTopicExclusive(client *ClientSession, topic string) error {
	if client.HasSubscription(topic) {
		return nil // Already subscribed
	}

	// Use identity string as consumer name for offset tracking
	client.identityMu.RLock()
	consumerName := client.Identity.String()
	client.identityMu.RUnlock()
	cancel, err := s.router.SubscribeExclusive(topic, consumerName, s.createMessageHandler(client))
	if err != nil {
		return err
	}

	client.AddSubscription(topic, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
	return nil
}

// lookupTriggerTimestampMs retrieves the trigger_timestamp_ms from the task metadata for
// the given taskID. Returns 0 if the task is not found, has no such key, or the value
// cannot be parsed. Errors are non-fatal and only logged at debug level.
func (s *GatewayServer) lookupTriggerTimestampMs(ctx context.Context, taskID string) int64 {
	if taskID == "" || s.taskStore == nil {
		return 0
	}
	t, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil || t == nil {
		return 0
	}
	raw, ok := t.Metadata["trigger_timestamp_ms"]
	if !ok {
		return 0
	}
	switch v := raw.(type) {
	case string:
		if parsed, perr := strconv.ParseInt(v, 10, 64); perr == nil && parsed > 0 {
			return parsed
		}
	case float64: // JSON numbers round-trip as float64
		if int64(v) > 0 {
			return int64(v)
		}
	case int64:
		if v > 0 {
			return v
		}
	}
	return 0
}

// subscribeClientToTopicExclusiveFromTimestamp subscribes a client to a topic with offset
// tracking, using a timestamp hint for cold-start replay when no stored offset exists.
// When startTimestampMs is 0, falls back to the standard SubscribeExclusive path.
func (s *GatewayServer) subscribeClientToTopicExclusiveFromTimestamp(client *ClientSession, topic string, startTimestampMs int64) error {
	if client.HasSubscription(topic) {
		return nil // Already subscribed
	}

	client.identityMu.RLock()
	consumerName := client.Identity.String()
	client.identityMu.RUnlock()

	var cancel func()
	var err error
	if startTimestampMs > 0 {
		cancel, err = s.router.SubscribeExclusiveFromTimestamp(topic, consumerName, startTimestampMs, s.createMessageHandler(client))
	} else {
		cancel, err = s.router.SubscribeExclusive(topic, consumerName, s.createMessageHandler(client))
	}
	if err != nil {
		return err
	}

	client.AddSubscription(topic, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
	return nil
}

// createMessageHandler creates a message handler that forwards messages to a client's gRPC stream.
// The identity string is captured at closure creation time to avoid data races with SwitchWorkspace.
//
// Design notes:
//   - Uses client.Deliver (non-blocking enqueue) instead of client.SafeSend to avoid blocking
//     the shared RabbitMQ consumer fan-out goroutine when a single client is slow (Finding #1).
//   - Caches the proto.Unmarshal result in envelopeSyncMap so that when N clients share the
//     same broadcast topic the envelope bytes are parsed only once per fan-out cycle (Finding #8).
//   - Recovers from panics so a misbehaving message cannot crash the shared consumer (Finding #24).
func (s *GatewayServer) createMessageHandler(client *ClientSession) func([]byte) {
	client.identityMu.RLock()
	identityStr := client.Identity.String()
	client.identityMu.RUnlock()

	return func(envelopeBytes []byte) {
		// Finding #24: recover from panics so a bad message cannot crash the shared consumer.
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("identity", identityStr).Msg("panic in message handler")
			}
		}()

		_, span := tracing.Tracer.Start(context.Background(), "aether.deliver_message")
		defer span.End()
		span.SetAttributes(attribute.String("aether.target_identity", identityStr))

		// Finding #8: unmarshal once per fan-out cycle. All handlers for the same broadcast
		// message share the same underlying []byte pointer, so we key on (ptr, len).
		// envelopeOnce.parse is guarded by sync.Once so concurrent handlers are safe.
		key := envelopeCacheKey(envelopeBytes)
		val, loaded := s.envelopeSyncMap.LoadOrStore(key, &envelopeOnce{})
		parsed := val.(*envelopeOnce)
		if !loaded {
			// We inserted the entry; schedule its removal after this handler returns
			// so subsequent fan-out cycles don't see a stale entry.
			defer s.envelopeSyncMap.Delete(key)
		}
		parsed.parse(envelopeBytes)

		if parsed.err != nil {
			logging.Logger.Error().Err(parsed.err).Str("identity", identityStr).Msg("failed to unmarshal message envelope")
			return
		}

		// Proxy/tunnel control-plane envelopes (ProxyHttpRequest/Response,
		// TunnelOpen/Close, ProxyError, …) are pre-marshalled DownstreamMessages
		// wrapped in a MessageEnvelope by ``publishProxyEnvelope``. Detect via
		// the sentinel Source, unwrap, and Deliver the inner DownstreamMessage
		// as-is so the SDK's typed dispatch (OnProxyHttpRequest etc.) fires
		// correctly. Without this fast path the consumer would re-wrap proxy
		// frames in IncomingMessage and route them to the generic OnMessage
		// handler, where they're silently dropped.
		if parsed.env.GetSource() == proxyFrameSourceMarker {
			var inner pb.DownstreamMessage
			if err := proto.Unmarshal(parsed.env.GetPayload(), &inner); err != nil {
				logging.Logger.Error().Err(err).Str("identity", identityStr).Msg("failed to unmarshal proxy frame inner DownstreamMessage")
				return
			}
			client.Deliver(&inner)
			return
		}

		// Finding #1: use Deliver (non-blocking) instead of SafeSend to avoid stalling
		// the shared consumer goroutine when this client's delivery buffer is full.
		client.Deliver(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Msg{
				Msg: &pb.IncomingMessage{
					SourceTopic: parsed.env.Source,
					Payload:     parsed.env.Payload,
					MessageType: parsed.env.MessageType,
				},
			},
		})
	}
}

// setupClientSubscriptions sets up the appropriate topic subscriptions for a client
// based on their principal type.
//
// Identity topics (unique per client) use exclusive subscriptions with offset tracking,
// allowing message replay on reconnection. Broadcast topics use shared subscriptions.
func (s *GatewayServer) setupClientSubscriptions(client *ClientSession) error {
	identity := client.Identity

	// For agent and task principals, look up the trigger timestamp from the associated task
	// so that cold-started pool-dispatched agents can replay from the message that triggered
	// their startup rather than missing it entirely (Fix B).
	var triggerTimestampMs int64
	if (identity.Type == models.PrincipalAgent || identity.Type == models.PrincipalTask) && client.AssociatedTaskID != "" {
		triggerTimestampMs = s.lookupTriggerTimestampMs(context.Background(), client.AssociatedTaskID)
	}

	switch identity.Type {
	case models.PrincipalAgent:
		// Agents subscribe to their identity topic (exclusive, with offset tracking).
		// Use timestamp hint for cold-start replay when no stored offset exists.
		idTopic := identity.ToTopic()
		if idTopic != "" {
			if err := s.subscribeClientToTopicExclusiveFromTimestamp(client, idTopic, triggerTimestampMs); err != nil {
				return fmt.Errorf("failed to subscribe to identity topic: %w", err)
			}
		}
		// Subscribe to global agent broadcasts for workspace (shared, no offset tracking)
		gaTopic, err := models.GlobalAgentTopic(identity.Workspace)
		if err != nil {
			return fmt.Errorf("invalid workspace for global agent topic: %w", err)
		}
		if err := s.subscribeClientToTopic(client, gaTopic); err != nil {
			logging.Logger.Warn().Err(err).Str("topic", gaTopic).Msg("failed to subscribe to global agent topic")
		}
		// Subscribe to workspace progress stream (shared, with server-side filtering)
		if err := s.subscribeClientToProgress(client, identity.Workspace); err != nil {
			logging.Logger.Warn().Err(err).Str("workspace", identity.Workspace).Msg("failed to subscribe to progress topic")
		}

	case models.PrincipalTask:
		// Tasks subscribe to their identity topic (exclusive, with offset tracking).
		// Use timestamp hint for cold-start replay when no stored offset exists.
		idTopic := identity.ToTopic()
		if idTopic != "" {
			if err := s.subscribeClientToTopicExclusiveFromTimestamp(client, idTopic, triggerTimestampMs); err != nil {
				return fmt.Errorf("failed to subscribe to identity topic: %w", err)
			}
		}
		// Non-unique tasks also subscribe to the broadcast topic for load balancing (shared)
		if identity.Specifier == "" && identity.ID != "" {
			tbTopic, err := models.TaskBroadcastTopic(identity.Workspace, identity.Implementation)
			if err != nil {
				return fmt.Errorf("invalid task broadcast topic: %w", err)
			}
			if err := s.subscribeClientToTopic(client, tbTopic); err != nil {
				logging.Logger.Warn().Err(err).Str("topic", tbTopic).Msg("failed to subscribe to task broadcast topic")
			}
		}

	case models.PrincipalUser:
		// Users subscribe to their window topic (exclusive, with offset tracking)
		idTopic := identity.ToTopic()
		if idTopic != "" {
			if err := s.subscribeClientToTopicExclusive(client, idTopic); err != nil {
				return fmt.Errorf("failed to subscribe to identity topic: %w", err)
			}
		}
		// Subscribe to the per-user progress topic for targeted (cross-workspace)
		// progress delivery. Agents publishing chat/app-kind progress set
		// ProgressReport.recipient to either us::{user}::{window} (window-
		// specific) or us::{user} (all windows); both publish to pg.us.{user}.
		// Each window's filter handler decides whether to deliver based on the
		// recipient form. Shared subscription within the gateway — local
		// fan-out dispatches each Rabbit message to every subscribed window
		// handler on this gateway, so a single Rabbit consumer per user
		// suffices regardless of window count.
		if identity.ID != "" {
			upgTopic, err := models.UserProgressTopic(identity.ID)
			if err != nil {
				return fmt.Errorf("invalid user-progress topic: %w", err)
			}
			if err := s.subscribeClientToUserProgress(client, upgTopic); err != nil {
				logging.Logger.Warn().Err(err).Str("topic", upgTopic).Msg("failed to subscribe to user-progress topic")
			}
		}
		// Subscribe to workspace-scoped topics if workspace is set (shared, no offset tracking)
		// This includes gu.{workspace}, uw.{user}.{workspace}, and pg.{workspace}
		if identity.Workspace != "" {
			if err := s.subscribeUserToWorkspaceTopics(client, identity.Workspace); err != nil {
				logging.Logger.Warn().Err(err).Msg("failed to subscribe to workspace topics")
			}
		}

	case models.PrincipalWorkflowEngine:
		// Workflow engine subscribes to event.* topic (exclusive, with offset tracking)
		// Events may need replay to ensure none are missed
		idTopic := identity.ToTopic()
		if idTopic != "" {
			if err := s.subscribeClientToTopicExclusive(client, idTopic); err != nil {
				return fmt.Errorf("failed to subscribe to events topic: %w", err)
			}
		}

	case models.PrincipalMetricsBridge:
		// Metrics bridge subscribes to the metric::receiver{N} fan-in shard
		// exclusively (with offset tracking) so a reconnecting bridge can
		// replay any metrics published while it was down. Multiple bridge
		// instances would collide on the identity-lock anyway (one bridge
		// per shard), so an exclusive subscription matches the deployment
		// invariant. Sharing was acceptable when bridges were
		// per-workspace; now that the receiver is a singleton fan-in, we
		// need delivery durability.
		idTopic := identity.ToTopic()
		if idTopic != "" {
			if err := s.subscribeClientToTopicExclusive(client, idTopic); err != nil {
				return fmt.Errorf("failed to subscribe to metrics topic: %w", err)
			}
		}

	case models.PrincipalBridge, models.PrincipalService:
		// Bridges/services subscribe to their identity topic (exclusive, with offset tracking)
		// Messages to these workspace-less intermediaries must not be lost.
		idTopic := identity.ToTopic()
		if idTopic != "" {
			if err := s.subscribeClientToTopicExclusive(client, idTopic); err != nil {
				return fmt.Errorf("failed to subscribe to identity topic: %w", err)
			}
		}

	case models.PrincipalOrchestrator:
		// Orchestrators manage infrastructure (spin up/down agents) and don't need
		// task-level progress. They receive notifications via direct gRPC stream messages.
		logging.Logger.Info().Str("session_id", client.ID).Str("identity", identity.String()).Msg("no topic subscription (orchestrator)")
	}

	return nil
}

// subscribeUserToWorkspaceTopics subscribes a user to workspace-scoped topics
func (s *GatewayServer) subscribeUserToWorkspaceTopics(client *ClientSession, workspace string) error {
	// Global user broadcast for workspace
	guTopic, err := models.GlobalUserTopic(workspace)
	if err != nil {
		return fmt.Errorf("invalid global user topic: %w", err)
	}
	if err := s.subscribeClientToTopic(client, guTopic); err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", guTopic, err)
	}

	// User-workspace scoped topic
	uwTopic, err := models.UserWorkspaceTopic(client.Identity.ID, workspace)
	if err != nil {
		return fmt.Errorf("invalid user-workspace topic: %w", err)
	}
	if err := s.subscribeClientToTopic(client, uwTopic); err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", uwTopic, err)
	}

	// Progress stream for workspace (shared, with server-side filtering)
	if err := s.subscribeClientToProgress(client, workspace); err != nil {
		return fmt.Errorf("failed to subscribe to progress for %s: %w", workspace, err)
	}

	return nil
}

// unsubscribeUserFromWorkspaceTopics removes workspace-scoped topic subscriptions
func (s *GatewayServer) unsubscribeUserFromWorkspaceTopics(client *ClientSession, workspace string) {
	if guTopic, err := models.GlobalUserTopic(workspace); err == nil {
		client.RemoveSubscription(guTopic)
	}

	if uwTopic, err := models.UserWorkspaceTopic(client.Identity.ID, workspace); err == nil {
		client.RemoveSubscription(uwTopic)
	}

	if pgTopic, err := models.ProgressTopic(workspace); err == nil {
		client.RemoveSubscription(pgTopic)
	}
}
