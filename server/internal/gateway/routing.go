package gateway

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/metering"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/sharding"
	"github.com/scitrera/aether/pkg/tasks"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/proto"
)

// offlineCacheTTL is how long a negative "target is offline" result is cached
// before re-checking Redis via sessions.IsActive.
const offlineCacheTTL = 5 * time.Second

// validTopicPrefixSet is a map for O(1) topic prefix validation.
var validTopicPrefixSet = map[string]bool{
	"ag": true, "tu": true, "ta": true, "tb": true,
	"us": true, "uw": true, "ga": true, "gu": true,
	"pg": true, "event": true, "metric": true, "br": true, "sv": true,
}

func validateTopicFormat(topic string) error {
	if len(topic) == 0 || len(topic) > 256 {
		return fmt.Errorf("topic length must be 1-256 characters")
	}
	sep := strings.Index(topic, models.IdentitySep)
	if sep <= 0 {
		return fmt.Errorf("invalid topic prefix")
	}
	if validTopicPrefixSet[topic[:sep]] {
		return nil
	}
	return fmt.Errorf("invalid topic prefix")
}

// extractWorkspaceFromTopic is an alias for workspaceFromTopic used by authority checking code.
func extractWorkspaceFromTopic(topic string) string { return workspaceFromTopic(topic) }

// workspaceFromTopic extracts the workspace component from a topic string.
// Returns empty string for topics with no workspace component (us::, br::, sv::).
func workspaceFromTopic(topic string) string {
	parts := strings.Split(topic, models.IdentitySep)
	if len(parts) < 2 {
		return ""
	}
	switch parts[0] {
	case "metric", "event":
		// metric::receiver* / event::receiver* topics are workspace-agnostic
		// fan-in shards, not workspaces — workspace attribution lives in
		// the message payload (Metric proto metadata for metrics, the
		// declared event_workspace / app_workspace for events). Treating
		// these as workspace-less avoids ACL checks that would otherwise
		// gate publishes against a non-existent workspace named
		// "receiver0".
		if strings.HasPrefix(parts[1], "receiver") {
			return ""
		}
		return parts[1]
	case "ag", "tu", "ta", "tb", "ga", "gu", "pg":
		// workspace is the next component: prefix::{workspace}[::rest]
		return parts[1]
	case "uw":
		// uw::{user_id}::{workspace} — workspace is the third component
		if len(parts) < 3 {
			return ""
		}
		return parts[2]
	}
	// us, br, sv — no workspace component
	return ""
}

func (s *GatewayServer) routeMessage(ctx context.Context, client *ClientSession, msg *pb.SendMessage) {
	routeStart := time.Now()
	ctx, span := tracing.Tracer.Start(ctx, "gateway.RouteMessage")
	defer span.End()
	span.SetAttributes(
		attribute.String("sender", client.Identity.String()),
		attribute.String("target_topic", msg.TargetTopic),
		attribute.String("message.type", msg.MessageType.String()),
		attribute.Int("message.payload_size", len(msg.Payload)),
	)

	// CRIT-1: Snapshot identity under RLock to avoid data race with handleSwitchWorkspace
	client.identityMu.RLock()
	sender := client.Identity // value copy, not pointer
	client.identityMu.RUnlock()

	// M-8: Use pre-parsed SessionUUID stored on client (avoid uuid.Parse on every message)
	sessionUUID := client.SessionUUID

	// Observe routing latency when done
	defer func() {
		messageRoutingLatency.WithLabelValues(sender.Workspace).Observe(time.Since(routeStart).Seconds())
	}()

	// H-4: Validate target topic format before any processing
	if err := validateTopicFormat(msg.TargetTopic); err != nil {
		logging.Logger.Warn().Str("identity", sender.String()).Str("topic", msg.TargetTopic).Err(err).Msg("invalid target topic format")
		sendClientError(client, "ERR_INVALID_TOPIC", fmt.Sprintf("invalid target topic: %s", err))
		return
	}

	// Rewrite event::/metric:: targets onto their fan-in shard. SDKs send to
	// either the literal wildcard "{event,metric}::*" (legacy convention) or
	// a workspace-scoped form "{event,metric}::{ws}". Both rewrite to a
	// single sharded receiver topic — today shard 0 — so workflow engines
	// and metrics bridges subscribe to one consolidated fan-in stream
	// regardless of how many workspaces are publishing.
	//
	// We capture the DECLARED workspace before rewrite so it can flow into
	// the downstream cross-workspace ACL check and the per-message
	// IncomingMessage.workspace field. The MessageEnvelope.Source still
	// records the sender's identity-topic; the declared workspace is
	// separately preserved via msg.AppWorkspace below.
	declaredEventMetricWorkspace := ""
	if strings.HasPrefix(msg.TargetTopic, "metric"+models.IdentitySep) {
		if !sharding.IsReceiverTopic(msg.TargetTopic) {
			// metric::*  → sender.Workspace controls the fan-in shard
			// metric::ws → ws controls the fan-in shard
			ws := strings.TrimPrefix(msg.TargetTopic, "metric"+models.IdentitySep)
			if ws == "*" || ws == "" {
				declaredEventMetricWorkspace = sender.Workspace
			} else {
				declaredEventMetricWorkspace = ws
			}
			msg.TargetTopic = sharding.ReceiverTopic("metric", sharding.ShardForWorkspace(declaredEventMetricWorkspace, sharding.TotalShards()))
		}
		// Receiver-shard targets passed in by the SDK already point at the
		// fan-in topic; leave them alone (no declared workspace context to
		// capture — the sender knew exactly what they were targeting).
	} else if strings.HasPrefix(msg.TargetTopic, "event"+models.IdentitySep) {
		if !sharding.IsReceiverTopic(msg.TargetTopic) {
			// event::*  → sender.Workspace controls the fan-in shard
			// event::ws → ws controls the fan-in shard
			ws := strings.TrimPrefix(msg.TargetTopic, "event"+models.IdentitySep)
			if ws == "*" || ws == "" {
				declaredEventMetricWorkspace = sender.Workspace
			} else {
				declaredEventMetricWorkspace = ws
			}
			msg.TargetTopic = sharding.ReceiverTopic("event", sharding.ShardForWorkspace(declaredEventMetricWorkspace, sharding.TotalShards()))
		}
	}
	// Stash the declared workspace into msg.AppWorkspace when it isn't
	// already set, so the envelope metadata downstream and the future
	// IncomingMessage.workspace field can recover it. AppWorkspace already
	// flows through routing as the canonical "workspace context" channel
	// (see triggerOrchestration); event/metric declared workspaces feed
	// into it identically.
	if declaredEventMetricWorkspace != "" && msg.AppWorkspace == "" {
		msg.AppWorkspace = declaredEventMetricWorkspace
	}

	// WS-P2J: Resolve sv::{impl} wildcard on the regular message path.
	// ParseSendTarget accepts two forms:
	//   sv::{impl}           — bare wildcard, isWildcard=true
	//   sv::{impl}::{spec}   — concrete, returned as-is
	// Non-sv:: targets (ag::, tu::, us::, …) are rejected by ParseSendTarget
	// and pass through unchanged. Resolution mirrors resolveProxyTarget and
	// MUST happen before ACL/audit so both layers see the concrete address.
	if _, _, isWC, parseErr := models.ParseSendTarget(msg.TargetTopic); parseErr == nil && isWC {
		concrete, resolveErr := s.resolveProxyTarget(ctx, msg.TargetTopic)
		if resolveErr != nil {
			logging.Logger.Warn().
				Str("from", sender.ToTopic()).
				Str("wildcard", msg.TargetTopic).
				Err(resolveErr).
				Msg("sv:: wildcard resolution failed on regular routing path")
			messageErrors.WithLabelValues(sender.Workspace, "sv_wildcard_unavailable").Inc()
			event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageRouteFailed, msg.TargetTopic, sender.Workspace, sessionUUID, false, resolveErr.Error(), map[string]interface{}{
				"from":          sender.ToTopic(),
				"to":            msg.TargetTopic,
				"denied_reason": "sv_wildcard_unavailable",
			})
			s.auditLog(ctx, event)
			sendClientError(client, "ERR_SV_UNAVAILABLE", resolveErr.Error(), withRetryable(true))
			return
		}
		logging.Logger.Debug().
			Str("from", sender.ToTopic()).
			Str("wildcard", msg.TargetTopic).
			Str("resolved", concrete).
			Msg("sv:: wildcard resolved on regular routing path")
		msg.TargetTopic = concrete
	}

	// 0. Message payload size limit
	maxPayload := s.quotaEnforcer.getMaxMessagePayloadSize()
	if len(msg.Payload) > maxPayload {
		logging.Logger.Warn().Str("identity", sender.String()).Int("size", len(msg.Payload)).Int("max", maxPayload).Msg("message payload too large")
		messageErrors.WithLabelValues(sender.Workspace, "payload_too_large").Inc()
		sendClientError(client, "ERR_PAYLOAD_TOO_LARGE", fmt.Sprintf("message payload size %d exceeds maximum of %d bytes", len(msg.Payload), maxPayload))
		return
	}

	// 0a. Per-client rate limiting
	if client.rateLimiter != nil && !client.rateLimiter.Allow() {
		logging.Logger.Warn().Str("identity", sender.String()).Msg("message rate limit exceeded")
		messageErrors.WithLabelValues(sender.Workspace, "rate_limited").Inc()
		retryAfterMs := int64(1000 / s.quotaEnforcer.messageRateLimit) // approximate ms until next token
		if retryAfterMs < 10 {
			retryAfterMs = 10
		}
		sendClientError(client, "ERR_RATE_LIMITED", "message rate limit exceeded", withRetryable(true), withRetryAfter(retryAfterMs))
		return
	}

	// 0b-pre. Workspace-level token-bucket rate limiting (in-memory, low latency)
	if s.quotaEnforcer.workspaceRateLimiter != nil {
		client.identityMu.RLock()
		workspace := client.Identity.Workspace
		client.identityMu.RUnlock()
		if workspace != "" && !s.quotaEnforcer.workspaceRateLimiter.Allow(workspace) {
			logging.Logger.Warn().Str("identity", sender.String()).Str("workspace", workspace).Msg("workspace message rate limit exceeded")
			messageErrors.WithLabelValues(workspace, "workspace_rate_limited").Inc()
			sendClientError(client, "ERR_WORKSPACE_RATE_LIMITED", fmt.Sprintf("workspace %s message rate limit exceeded", workspace), withRetryable(true), withRetryAfter(100))
			return
		}
	}

	// 0.5 Workspace-level message rate quota (multi-tenant)
	if s.quotaEnforcer.quotaManager != nil {
		if err := s.quotaEnforcer.quotaManager.CheckMessageQuota(ctx, sender.Workspace, sender.String()); err != nil {
			logging.Logger.Warn().Str("identity", sender.String()).Str("workspace", sender.Workspace).Msg("workspace message rate quota exceeded")
			sendClientError(client, "ERR_QUOTA_001", err.Error(), withRetryable(true), withRetryAfter(1000))
			return
		}
	}

	// 0a-pre. Cross-workspace event/metric broadcast ACL check. The
	// receiver-shard rewrite above has already collapsed event::/metric::
	// targets onto a workspace-agnostic fan-in topic, so the downstream
	// checkMessageSend can no longer see the declared target workspace.
	// Enforce the capability gate here using the workspace we captured
	// pre-rewrite.
	//
	// Implicit grant when declared == sender.Workspace (same-workspace
	// broadcast — no extra permission beyond the regular topic-send ACL).
	// Cross-workspace requires capability/event_broadcast or
	// capability/metric_broadcast against the TARGET workspace.
	if declaredEventMetricWorkspace != "" && declaredEventMetricWorkspace != sender.Workspace {
		if err := s.checkCrossWorkspaceBroadcast(ctx, sender, msg.TargetTopic, declaredEventMetricWorkspace, sessionUUID); err != nil {
			logging.Logger.Warn().Str("from", sender.ToTopic()).Str("to", msg.TargetTopic).Str("declared_workspace", declaredEventMetricWorkspace).Err(err).Msg("cross-workspace event/metric broadcast denied")
			messageErrors.WithLabelValues(sender.Workspace, "cross_workspace_broadcast_denied").Inc()
			event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageRouteFailed, msg.TargetTopic, sender.Workspace, sessionUUID, false, err.Error(), map[string]interface{}{
				"from":               sender.ToTopic(),
				"to":                 msg.TargetTopic,
				"message_type":       msg.MessageType.String(),
				"declared_workspace": declaredEventMetricWorkspace,
				"denied_reason":      "cross_workspace_broadcast_denied",
			})
			s.auditLog(ctx, event)
			sendClientError(client, "ERR_PERMISSION_DENIED", fmt.Sprintf("cross-workspace broadcast to %s denied: %s", msg.TargetTopic, err.Error()))
			return
		}
	}

	// 0a. Permission matrix enforcement (spec Section 3.2.2)
	if err := enforceTopicPermissions(sender, msg.TargetTopic); err != nil {
		logging.Logger.Warn().Str("from", sender.ToTopic()).Str("to", msg.TargetTopic).Err(err).Msg("message denied by permission matrix")
		messageErrors.WithLabelValues(sender.Workspace, "permission_denied").Inc()
		sendClientError(client, "ERR_PERMISSION_DENIED", fmt.Sprintf("not authorized to send to topic %s", msg.TargetTopic))
		return
	}

	// 0a-bis. Metric payload SHAPE validation (structured Metric proto).
	// Done early — fail fast on garbage input before any ACL roundtrip.
	// Negative-delta authorization is checked AFTER authority resolution
	// below so on-behalf-of grants can supply the credit capability.
	var parsedMetric *pb.Metric
	if msg.MessageType == pb.MessageType_METRIC {
		m, shapeErr := validateMetricShape(msg.Payload)
		if shapeErr != nil {
			s.rejectMetric(ctx, client, sender, msg, sessionUUID, nil, shapeErr)
			return
		}
		parsedMetric = m
	}

	resolvedAuthority, err := s.resolveAuthorizationContext(ctx, client, sender, msg.GetAuthorization())
	if err != nil {
		logging.Logger.Warn().Str("from", sender.ToTopic()).Str("to", msg.TargetTopic).Err(err).Msg("message denied by authorization context")
		messageErrors.WithLabelValues(sender.Workspace, "permission_denied").Inc()

		event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageRouteFailed, msg.TargetTopic, sender.Workspace, sessionUUID, false, err.Error(), map[string]interface{}{
			"from":          sender.ToTopic(),
			"to":            msg.TargetTopic,
			"message_type":  msg.MessageType.String(),
			"denied_reason": "invalid_authorization_context",
		})
		s.auditLog(ctx, event)

		sendClientError(client, "ERR_PERMISSION_DENIED", fmt.Sprintf("not authorized to send to topic %s", msg.TargetTopic))
		return
	}

	// Auto-resolve session-bound task authority for agent/task principals that
	// didn't attach an explicit OBO context on this message. Enables send_to_user
	// and similar task-scoped operations without requiring callers to manually
	// thread grant IDs on every send. Scoped to agent/task principals only so
	// this doesn't change behavior for user/service/etc. senders.
	if resolvedAuthority == nil && client != nil && client.AssociatedTaskID != "" {
		if sender.Type == models.PrincipalAgent || sender.Type == models.PrincipalTask {
			if autoAuth, autoErr := s.loadCallerMessageAuthority(ctx, client, sender); autoErr == nil && autoAuth != nil {
				resolvedAuthority = autoAuth
			}
		}
	}
	associatedTaskID := ""
	if client != nil {
		associatedTaskID = client.AssociatedTaskID
	}
	logging.Logger.Debug().
		Str("from", sender.ToTopic()).
		Str("to", msg.TargetTopic).
		Bool("explicit_obo", msg.GetAuthorization() != nil).
		Bool("resolved", resolvedAuthority != nil).
		Str("associated_task_id", associatedTaskID).
		Msg("message send authority resolution outcome")

	// 0b. ACL Check - verify message send permission
	if resolvedAuthority != nil {
		err = s.checkMessageSendWithAuthority(ctx, sender, msg.TargetTopic, sessionUUID, resolvedAuthority)
	} else {
		err = s.checkMessageSend(ctx, sender, msg.TargetTopic)
	}
	if err != nil {
		logging.Logger.Warn().Str("from", sender.ToTopic()).Str("to", msg.TargetTopic).Err(err).Msg("message denied by ACL")

		// Audit: Message routing failed (ACL denied)
		event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageRouteFailed, msg.TargetTopic, sender.Workspace, sessionUUID, false, err.Error(), map[string]interface{}{
			"from":          sender.ToTopic(),
			"to":            msg.TargetTopic,
			"message_type":  msg.MessageType.String(),
			"denied_reason": "acl_check_failed",
		})
		applyResolvedAuthorityToAuditEvent(event, resolvedAuthority)
		s.auditLog(ctx, event)

		sendClientError(client, "ERR_PERMISSION_DENIED", fmt.Sprintf("not authorized to send to topic %s", msg.TargetTopic))
		return
	}

	// 0c. Metric negative-delta authorization. Runs after authority resolution
	// so on-behalf-of grants (subject's capability/metric_credit) are honored, and
	// so the rejection audit row carries full authority lineage.
	if parsedMetric != nil && metricHasNegative(parsedMetric) {
		if creditErr := s.checkMetricCredit(ctx, sender, msg.TargetTopic, sessionUUID, resolvedAuthority); creditErr != nil {
			s.rejectMetric(ctx, client, sender, msg, sessionUUID, resolvedAuthority, creditErr)
			return
		}
	}

	// Audit: Message received from client (successful ACL check)
	if s.auditLogger != nil {
		msgMetadata := map[string]interface{}{
			"from":         sender.ToTopic(),
			"to":           msg.TargetTopic,
			"message_type": msg.MessageType.String(),
		}

		// Add message metadata based on verbosity level
		verbosity := s.auditLogger.GetConfig().VerbosityLevel
		if audit.ShouldIncludeMessageMetadata(verbosity) {
			if len(msg.Payload) > 0 {
				msgMetadata["message_size"] = len(msg.Payload)
			}
		}
		if audit.ShouldIncludeMessageContent(verbosity) {
			if len(msg.Payload) > 0 {
				if msg.MessageType == pb.MessageType_OPAQUE {
					msgMetadata["message_content_format"] = "opaque"
				} else {
					maxLen := 1024
					if len(msg.Payload) > maxLen {
						msgMetadata["message_content"] = string(msg.Payload[:maxLen]) + "... (truncated)"
						msgMetadata["content_truncated"] = true
					} else {
						msgMetadata["message_content"] = string(msg.Payload)
					}
				}
			}
		}

		event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageReceived, msg.TargetTopic, sender.Workspace, sessionUUID, true, "", msgMetadata)
		applyResolvedAuthorityToAuditEvent(event, resolvedAuthority)
		s.auditLog(ctx, event)
	}

	now := time.Now()

	// Check if target is online (for ag. and tu. topics)
	// First check local identityIndex (O(1) sync.Map) before falling back to Redis.
	// Finding #12: use a negative cache to avoid a Redis round-trip on every message
	// to an offline target. Cache entries expire after offlineCacheTTL (5s).
	if s.shouldOrchestrate(msg.TargetTopic) {
		_, locallyConnected := s.identityIndex.Load(msg.TargetTopic)
		if !locallyConnected {
			// Check negative cache before hitting Redis
			needsRedisCheck := true
			if cached, ok := s.offlineTopicCache.Load(msg.TargetTopic); ok {
				if time.Since(cached.(time.Time)) < offlineCacheTTL {
					needsRedisCheck = false
				}
			}

			isOffline := false
			if needsRedisCheck {
				active, _ := s.sessions.IsActive(ctx, msg.TargetTopic)
				if !active {
					isOffline = true
					s.offlineTopicCache.Store(msg.TargetTopic, time.Now())
				}
				// Target came online: evict any stale negative-cache entry
				if active {
					s.offlineTopicCache.Delete(msg.TargetTopic)
				}
			} else {
				isOffline = true
			}

			if isOffline {
				s.triggerOrchestration(ctx, sender, msg.TargetTopic, now.UnixMilli(), msg.GetAppWorkspace())
			}
		}
	}

	// Wrap payload in MessageEnvelope with server-verified source.
	//
	// effectiveWorkspace captures the workspace context for this message in
	// priority order:
	//   1. SendMessage.app_workspace (already set on the request, or
	//      stashed by the event/metric rewrite above with the declared ws)
	//   2. Workspace component of the pre-rewrite target topic
	//      (event::{ws}, metric::{ws}, uw::{user}::{ws}, ag::{ws}::…)
	//   3. Sender's identity workspace
	//   4. Empty string if none apply (bridges, services)
	//
	// This is propagated through MessageEnvelope.Metadata so subscribers on
	// workspace-agnostic fan-in shards (event::receiver{N},
	// metric::receiver{N}) can recover the originating workspace at
	// delivery time. Once IncomingMessage.workspace lands (proto regen), the
	// subscription path will surface it as a first-class field.
	effectiveWorkspace := msg.GetAppWorkspace()
	if effectiveWorkspace == "" {
		effectiveWorkspace = declaredEventMetricWorkspace
	}
	if effectiveWorkspace == "" {
		effectiveWorkspace = sender.Workspace
	}
	envelope := &pb.MessageEnvelope{
		Source:      sender.ToTopic(),
		Payload:     msg.Payload,
		MessageType: msg.MessageType,
		TimestampMs: now.UnixMilli(),
	}
	if effectiveWorkspace != "" {
		// Always allocate the map only when we have data — avoids inflating
		// the envelope size for the common no-workspace bridge case.
		envelope.Metadata = map[string]string{"workspace": effectiveWorkspace}
	}

	envelopeBytes, err := proto.Marshal(envelope)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("failed to marshal message envelope")
		return
	}

	err = s.publishBreaker.Execute(func() error {
		return s.router.Publish(ctx, msg.TargetTopic, envelopeBytes)
	})
	if err != nil {
		messageErrors.WithLabelValues(sender.Workspace, "publish_failed").Inc()

		var errCode, errMsg string
		if err == circuitbreaker.ErrCircuitOpen {
			logging.Logger.Warn().Str("identity", sender.String()).Str("topic", msg.TargetTopic).Msg("publish circuit breaker open, rejecting message")
			errCode = "ERR_CIRCUIT_OPEN"
			errMsg = "message broker temporarily unavailable, please retry"
		} else {
			logging.Logger.Error().Err(err).Msg("failed to publish message")
			errCode = "ERR_PUBLISH_FAILED"
			errMsg = "message delivery failed"
		}

		// Audit: Message routing failed (publish error)
		event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageRouteFailed, msg.TargetTopic, sender.Workspace, sessionUUID, false, err.Error(), map[string]interface{}{
			"from":         sender.ToTopic(),
			"to":           msg.TargetTopic,
			"message_type": msg.MessageType.String(),
		})
		applyResolvedAuthorityToAuditEvent(event, resolvedAuthority)
		s.auditLog(ctx, event)

		// Notify the client that delivery failed so it can retry or handle the error
		sendClientError(client, errCode, errMsg, withRetryable(true))
	} else {
		messagesRouted.WithLabelValues(sender.Workspace, msg.MessageType.String()).Inc()
		metering.MessagesRouted.WithLabelValues(sender.Workspace, msg.MessageType.String()).Inc()
		metering.BytesRouted.WithLabelValues(sender.Workspace).Add(float64(len(msg.Payload)))
		// Audit: Message successfully routed to topic
		event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpMessageRouted, msg.TargetTopic, sender.Workspace, sessionUUID, true, "", map[string]interface{}{
			"from":         sender.ToTopic(),
			"to":           msg.TargetTopic,
			"message_type": msg.MessageType.String(),
		})
		applyResolvedAuthorityToAuditEvent(event, resolvedAuthority)
		s.auditLog(ctx, event)
	}
}

// checkMessageSend checks whether a sender is allowed to publish to a target topic via the ACL
// service. It preserves the workspace-derivation logic: bridges/services with no home workspace
// check ACL against the target workspace; workspace-scoped senders check against the target
// workspace when it differs from their own (cross-workspace), or their own workspace otherwise.
func (s *GatewayServer) checkMessageSend(ctx context.Context, sender models.Identity, targetTopic string) error {
	if s.acl == nil {
		return nil // ACL not enabled
	}

	// System principals with no workspace (bridges/services) check ACL against the target workspace.
	if sender.Workspace == "" {
		targetWorkspace := workspaceFromTopic(targetTopic)
		if targetWorkspace != "" {
			decision, err := s.acl.CanSendMessage(ctx, sender, acl.ResourceTypeWorkspace, targetWorkspace, targetWorkspace, uuid.Nil)
			if err != nil {
				return fmt.Errorf("ACL check failed: %w", err)
			}
			if decision.Denied() {
				logging.Logger.Info().
					Str("sender", sender.ToTopic()).
					Str("target", targetTopic).
					Str("workspace_checked", targetWorkspace).
					Str("reason", decision.Reason).
					Msg("checkMessageSend denial")
				return fmt.Errorf("access denied: %s", decision.Reason)
			}
		}
		return nil
	}

	// Workspace-scoped senders: gate on TARGET workspace when it differs
	// from sender's home, sender's own workspace otherwise.
	checkWorkspace := sender.Workspace
	targetWorkspace := workspaceFromTopic(targetTopic)
	if targetWorkspace != "" && targetWorkspace != sender.Workspace {
		checkWorkspace = targetWorkspace
	}

	decision, err := s.acl.CanSendMessage(ctx, sender, acl.ResourceTypeWorkspace, checkWorkspace, checkWorkspace, uuid.Nil)
	if err != nil {
		return fmt.Errorf("ACL check failed: %w", err)
	}
	if decision.Denied() {
		logging.Logger.Info().
			Str("sender", sender.ToTopic()).
			Str("target", targetTopic).
			Str("workspace_checked", checkWorkspace).
			Str("reason", decision.Reason).
			Msg("checkMessageSend denial")
		return fmt.Errorf("access denied: %s", decision.Reason)
	}
	return nil
}

// checkCrossWorkspaceBroadcast enforces the cross-workspace capability gate
// for event::/metric:: publishes. Same-workspace publishes are implicit (the
// regular checkMessageSend path handles them via the per-workspace topic-send
// ACL). Cross-workspace publishes require capability/event_broadcast or
// capability/metric_broadcast against the TARGET workspace.
//
// targetTopic here is the POST-rewrite topic — i.e. always event::receiver{N}
// or metric::receiver{N}. The DECLARED workspace is provided separately by
// the caller (captured before rewrite).
//
// Returns nil when ACL is disabled (dev mode): the cross-workspace gate is
// best-effort layered on top of the regular ACL; if ACL isn't configured,
// there's no ceiling to enforce.
func (s *GatewayServer) checkCrossWorkspaceBroadcast(ctx context.Context, sender models.Identity, targetTopic, declaredWorkspace string, sessionUUID uuid.UUID) error {
	if s.acl == nil {
		return nil
	}
	permission := acl.PermissionEventBroadcast
	resourceLabel := "event_broadcast"
	if strings.HasPrefix(targetTopic, "metric"+models.IdentitySep) {
		permission = acl.PermissionMetricBroadcast
		resourceLabel = "metric_broadcast"
	}
	if hasCrossWorkspaceBroadcastPermission(ctx, s, sender, permission, resourceLabel, declaredWorkspace, sessionUUID) {
		return nil
	}
	return fmt.Errorf("cross-workspace broadcast requires %s on workspace %q", permission, declaredWorkspace)
}

// hasCrossWorkspaceBroadcastPermission is the production ACL check for the
// cross-workspace event/metric capability gate. Exposed as a package-level
// variable so tests can substitute a deterministic stub without standing up
// a full *acl.Service. NOTE: tests overriding this var must not call
// t.Parallel() — the substitution is process-global.
var hasCrossWorkspaceBroadcastPermission = func(ctx context.Context, s *GatewayServer, sender models.Identity, permission, resourceLabel, declaredWorkspace string, sessionUUID uuid.UUID) bool {
	if s == nil || s.acl == nil {
		return false
	}
	decision, err := s.acl.CheckAccess(
		ctx, sender,
		acl.ResourceTypeCapability, permission,
		resourceLabel, declaredWorkspace, sessionUUID, acl.AccessReadWrite,
	)
	return err == nil && decision != nil && decision.Allowed
}

// enforceTopicPermissions checks spec Section 3.2.2 permission matrix.
// Returns an error if the sender's principal type is not allowed to publish to the target topic.
func enforceTopicPermissions(sender models.Identity, targetTopic string) error {
	switch sender.Type {
	case models.PrincipalUser:
		// Users cannot send to event.*, metric.*, or pg.* topics
		if strings.HasPrefix(targetTopic, "event"+models.IdentitySep) || strings.HasPrefix(targetTopic, "metric"+models.IdentitySep) || strings.HasPrefix(targetTopic, "pg"+models.IdentitySep) {
			return fmt.Errorf("users cannot publish to %s topics", strings.SplitN(targetTopic, ".", 2)[0])
		}
	case models.PrincipalMetricsBridge:
		// MetricsBridge is receive-only — cannot send to any topic
		return fmt.Errorf("metrics bridge principals are receive-only")
	case models.PrincipalOrchestrator:
		// Orchestrators can only send to agent/task topics (status updates)
		if !strings.HasPrefix(targetTopic, "ag"+models.IdentitySep) && !strings.HasPrefix(targetTopic, "tu"+models.IdentitySep) &&
			!strings.HasPrefix(targetTopic, "ta"+models.IdentitySep) && !strings.HasPrefix(targetTopic, "tb"+models.IdentitySep) {
			return fmt.Errorf("orchestrators can only send status updates to agent/task topics")
		}
	}

	// Cross-workspace sends are NOT denied here at the transport layer.
	// They were unconditionally rejected pre-Solution-A-followups, but the
	// rule was over-broad: it blocked legitimate operator-coordination
	// patterns (e.g., a CoworkAgent in `_apps` pushing per-sandbox
	// proxy_config to its spawned sidecar in `_sandbox`) even when the
	// caller had explicit ACL authority on the target workspace.
	//
	// New rule (enforced downstream in checkMessageSendWith{Authority,
	// Delegation}): if target workspace differs from sender's home
	// workspace, the caller must hold the appropriate ACL grant
	// (directly OR via an OBO subject) on the TARGET workspace.
	// Same-workspace sends preserve the implicit "you can talk inside
	// the workspace you authenticated into" semantic via the same
	// downstream ACL path. workspaceFromTopic returns "" for user,
	// bridge, and similar identity-shaped topics — those continue to be
	// exempt from any workspace check.
	return nil
}

// startOfflineCacheEvictor runs a background goroutine that periodically evicts
// stale entries from s.offlineTopicCache to prevent unbounded growth.
func (s *GatewayServer) startOfflineCacheEvictor(ctx context.Context) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Msg("recovered from panic in offline cache evictor")
			}
		}()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				s.offlineTopicCache.Range(func(key, value any) bool {
					if now.Sub(value.(time.Time)) > 2*offlineCacheTTL {
						s.offlineTopicCache.Delete(key)
					}
					return true
				})
			}
		}
	}()
}

func (s *GatewayServer) shouldOrchestrate(topic string) bool {
	// ag:: and tu:: topics trigger orchestration
	return strings.HasPrefix(topic, "ag"+models.IdentitySep) || strings.HasPrefix(topic, "tu"+models.IdentitySep)
}

// triggerOrchestration creates an agent_startup task to spin up a pool-dispatched
// agent for the given offline topic. sender is the principal whose message
// caused this call (propagated into CreateTaskRequest.SubjectIdentity so the
// resulting startup task's Authority lineage records the originating user —
// this is Fix AA, upstream of Fix A). triggerTimestampMs is the unix-millisecond
// timestamp when the gateway accepted the user message that triggered this call;
// it is recorded on the task's Metadata map so the agent's broker subscription can
// start from that point and replay the trigger message (see Fix B in the plan at
// two-things-we-should-encapsulated-matsumoto.md). A value of 0 means "no hint".
// senderAppWorkspace is the user's active app workspace (e.g. "default"), stamped
// by the ws-server on SendMessage.app_workspace so the minted task-authority grant
// includes it in WorkspaceScope; this allows spawned agents to create dependent
// resources (sandbox leases, KV writes) in the user's workspace on their behalf.
//
// Currently, task authority is minted with app_workspace explicitly set in WorkspaceScope.
// Future: once server/cmd/auth-proxy fronts the gateway for cowork users and populates
// client.RootGrantID, prefer establishTaskAuthorityGrant with the user's root grant as
// parent — it inherits the broader WorkspaceScope automatically, making app_workspace
// merely a hint / observability field.
func (s *GatewayServer) triggerOrchestration(ctx context.Context, sender models.Identity, targetTopic string, triggerTimestampMs int64, senderAppWorkspace string) {
	ctx, span := tracing.Tracer.Start(ctx, "aether.orchestration.trigger")
	defer span.End()
	span.SetAttributes(attribute.String("aether.target_topic", targetTopic))

	// Check if orchestration services are available
	if s.orchestration == nil || s.orchestration.TaskService == nil {
		logging.Logger.Warn().Str("topic", targetTopic).Msg("orchestration not enabled, cannot start agent")
		return
	}

	// Parse identity from topic
	// ag.{workspace}.{impl}.{spec}
	// tu.{workspace}.{impl}.{unique_spec}
	identity, err := models.ParseIdentity(targetTopic)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logging.Logger.Error().Err(err).Str("topic", targetTopic).Msg("failed to parse identity from topic")
		return
	}

	// Only orchestrate agents and unique tasks
	if identity.Type != models.PrincipalAgent && identity.Type != models.PrincipalTask {
		logging.Logger.Debug().Str("topic", targetTopic).Msg("topic is not an agent or task, skipping orchestration")
		return
	}
	span.SetAttributes(
		attribute.String("aether.target_implementation", identity.Implementation),
		attribute.String("aether.workspace", identity.Workspace),
	)

	// Check if agent implementation exists in registry
	exists, err := s.orchestration.Registry.Exists(ctx, identity.Implementation)
	if err != nil {
		logging.Logger.Error().Err(err).Str("implementation", identity.Implementation).Msg("failed to check agent registry")
		return
	}
	if !exists {
		logging.Logger.Warn().Str("implementation", identity.Implementation).Msg("agent implementation not in registry, cannot orchestrate")
		return
	}

	// Finding #13: skip task creation if an active startup task already exists
	// for this (implementation, workspace, specifier) tuple to avoid duplicate
	// orchestration tasks. Specifier is included so per-user singleton agents
	// (e.g. CoworkAgent) don't collide across users sharing the home workspace.
	if s.taskStore != nil {
		hasActive, _, err := s.taskStore.HasActiveStartupTask(ctx, identity.Implementation, identity.Workspace, identity.Specifier)
		if err == nil && hasActive {
			logging.Logger.Debug().Str("topic", targetTopic).Str("impl", identity.Implementation).Str("workspace", identity.Workspace).Str("specifier", identity.Specifier).Msg("active startup task already exists, skipping orchestration trigger")
			return
		}
	}

	// Create an agent_startup task directly. We route with TaskType=
	// "agent_startup" so the TaskService short-circuits to
	// createOrchestratedStartupTask and produces exactly one task row (see
	// task_assignment.go::handleTargeted, line ~263). Previously this path
	// also created a companion "message_delivery" task, but that task was
	// metadata-only (no payload — the user's actual message is published to
	// the RabbitMQ stream at routeMessage L340 and reaches the agent via
	// ordinary subscription replay), had no agent-side handler, and existed
	// only as dead weight with a redundant grant. Dropping it halves the
	// tasks per cold-start trigger and collapses the grant lineage.
	metadata := map[string]interface{}{
		"trigger":      "message_routing",
		"target_topic": targetTopic,
	}
	// Record the trigger timestamp so the agent's first-connect subscription
	// can resume from just before the message that woke it up (Fix B).
	// Stored as a string for stable Postgres JSONB round-trip.
	if triggerTimestampMs > 0 {
		metadata["trigger_timestamp_ms"] = strconv.FormatInt(triggerTimestampMs, 10)
	}
	req := &orchestration.CreateTaskRequest{
		TaskType:       "agent_startup",
		Workspace:      identity.Workspace,
		AssignmentMode: "targeted",
		TargetAgentID:  targetTopic,
		Metadata:       metadata,
		CreatorIdentity: models.Identity{
			Type: models.PrincipalAgent,
			ID:   s.gatewayID,
		},
		// Fix AA: propagate the originating sender as the OBO subject so the
		// startup task carries Authority.SubjectType/SubjectID. Without this,
		// buildTaskContext cannot populate task_context["user"] and the
		// resulting agent tries to send_to_user with user_id=None.
		SubjectIdentity: sender,
	}

	response, err := s.orchestration.TaskService.CreateTask(ctx, req)
	if err != nil {
		logging.Logger.Error().Err(err).Str("topic", targetTopic).Msg("failed to trigger orchestration")
		return
	}

	// Fix AAA: mint a root-level task authority grant so the spawned agent
	// inherits authority to act on the user-sender's behalf (specifically so
	// send_to_user on the ag.→us. edge passes ACL, sandbox_lease creation in
	// the user's workspace passes ACL, etc.). Grant minting failures are
	// logged but do not fail the trigger; the task still exists — ACL will
	// simply deny like before, matching prior behavior.
	if sender.Type == models.PrincipalUser && sender.ID != "" && response.StartupTaskID != "" {
		if targetIdentity, parseErr := models.ParseIdentity(targetTopic); parseErr == nil {
			if _, gErr := s.mintTaskGrantForSender(ctx, sender, response.StartupTaskID, identity.Workspace, targetIdentity, fmt.Sprintf("trigger_orchestration:agent_startup:%s", response.StartupTaskID), senderAppWorkspace); gErr != nil {
				logging.Logger.Warn().Err(gErr).Str("task_id", response.StartupTaskID).Msg("failed to mint agent_startup task grant")
			}
		} else {
			logging.Logger.Warn().Err(parseErr).Str("topic", targetTopic).Msg("failed to parse target identity; skipping agent_startup grant mint")
		}
	}

	orchestrationTriggers.WithLabelValues(identity.Workspace).Inc()
	logging.Logger.Info().Str("topic", targetTopic).Str("task_id", response.TaskID).Str("status", string(response.Status)).Msg("triggered orchestration")
}

func (s *GatewayServer) handleSwitchWorkspace(ctx context.Context, client *ClientSession, sw *pb.SwitchWorkspace) {
	// Snapshot identity under RLock before type check
	client.identityMu.RLock()
	identType := client.Identity.Type
	oldWorkspace := client.Identity.Workspace
	userID := client.Identity.ID
	client.identityMu.RUnlock()

	if identType != models.PrincipalUser {
		sendClientError(client, "ERR_INVALID_PRINCIPAL", "only users can switch workspaces")
		return
	}

	newWorkspace := sw.NewWorkspaceId

	if oldWorkspace == newWorkspace {
		return // No change
	}

	// H-2: ACL check before allowing workspace switch
	if s.acl != nil {
		decision, err := s.acl.CanConnect(ctx, client.Identity, acl.ResourceTypeWorkspace, newWorkspace, newWorkspace, uuid.Nil)
		if err != nil {
			logging.Logger.Warn().Err(err).Str("user_id", userID).Str("workspace", newWorkspace).Msg("workspace switch ACL check failed")
			sendClientError(client, "ERR_WORKSPACE_SWITCH_FAILED", fmt.Sprintf("workspace switch ACL check failed: %v", err))
			return
		}
		if decision.Denied() {
			logging.Logger.Warn().Str("user_id", userID).Str("workspace", newWorkspace).Str("reason", decision.Reason).Msg("workspace switch denied by ACL")
			sendClientError(client, "ERR_PERMISSION_DENIED", fmt.Sprintf("not authorized to access workspace %s", newWorkspace))
			return
		}
	}

	logging.Logger.Info().Str("user_id", userID).Str("old_workspace", oldWorkspace).Str("new_workspace", newWorkspace).Msg("user switching workspace")

	// Unsubscribe from old workspace topics
	if oldWorkspace != "" {
		s.unsubscribeUserFromWorkspaceTopics(client, oldWorkspace)
	}

	// Update identity workspace under write lock
	client.identityMu.Lock()
	client.Identity.Workspace = newWorkspace
	client.identityMu.Unlock()

	// Subscribe to new workspace topics
	if newWorkspace != "" {
		if err := s.subscribeUserToWorkspaceTopics(client, newWorkspace); err != nil {
			logging.Logger.Error().Err(err).Msg("error subscribing to new workspace topics")
			sendClientError(client, "ERR_WORKSPACE_SWITCH_FAILED", fmt.Sprintf("failed to subscribe to workspace topics: %v", err))
		}
	}
}

// isUserBoundKVScope reports whether a KV scope is bound to a specific user
// identity. Used by handleKVOp to decide whether auto-OBO authority resolution
// (task/message grant → user subject) should run: it only makes sense for
// user-bound scopes. For scopes where the caller's own identity is
// authoritative (global, workspace, and their exclusive variants) we let
// ACL evaluate against the raw caller identity.
//
// Both the per-agent USER/USER_WORKSPACE scopes AND the cross-agent shared
// USER_SHARED/USER_WORKSPACE_SHARED scopes are user-bound: in the shared
// case the storage rendezvous still belongs to a specific user, and any
// cross-agent access still needs to demonstrate authority over that user.
func isUserBoundKVScope(s pb.KVOperation_Scope) bool {
	switch s {
	case pb.KVOperation_USER,
		pb.KVOperation_USER_WORKSPACE,
		pb.KVOperation_USER_SHARED,
		pb.KVOperation_USER_WORKSPACE_SHARED:
		return true
	}
	return false
}

func (s *GatewayServer) handleKVOp(ctx context.Context, client *ClientSession, op *pb.KVOperation) {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.KVOperation")
	defer span.End()
	span.SetAttributes(
		attribute.String("operation", op.GetOp().String()),
		attribute.String("scope", op.GetScope().String()),
		attribute.String("key", op.GetKey()),
	)

	// Snapshot identity under RLock to avoid data race with handleSwitchWorkspace
	client.identityMu.RLock()
	ident := client.Identity
	client.identityMu.RUnlock()

	// KV quota checks (value size for PUT operations)
	if s.quotaEnforcer.quotaManager != nil && op.GetOp() == pb.KVOperation_PUT {
		workspace := op.Workspace
		if workspace == "" {
			workspace = ident.Workspace
		}
		if err := s.quotaEnforcer.quotaManager.CheckKVValueSize(ctx, workspace, len(op.Value)); err != nil {
			sendClientError(client, "ERR_QUOTA_001", err.Error(), withRequestID(op.GetRequestId()))
			return
		}
	}

	// Use cached KV handler (stateless, created once at server init)
	handler := s.kvHandler

	// Use pre-parsed SessionUUID from client (avoids uuid.Parse on every KV op)
	sessionUUID := client.SessionUUID

	// Send response back to stream (mutex-protected)
	sendResponse := func(msg *pb.DownstreamMessage) {
		if err := client.SafeSend(msg); err != nil {
			logging.Logger.Error().Err(err).Msg("failed to send KV response")
		}
	}

	resolvedAuthority, err := s.resolveAuthorizationContext(ctx, client, ident, op.GetAuthorization())
	if err != nil {
		sendClientError(client, "ERR_PERMISSION_DENIED", "invalid authorization context", withRequestID(op.GetRequestId()))
		return
	}

	// Auto-derive authority from the caller's associated task when the client
	// didn't attach an explicit AuthorizationContext, so KV ops from agents
	// acting on a user's behalf don't require callers to thread OBO context
	// manually. Mirrors the fallback chain used by handleCreateTask (see
	// orchestration_integration.go:395).
	//
	// Scope-aware: auto-promotion only runs for user-bound scopes (user,
	// user-workspace) where the caller is semantically operating on a
	// specific user's data. For non-user-bound scopes (global, workspace)
	// the caller's own identity is authoritative — e.g. an agent reading
	// its own global model config is an agent-identity action, not an
	// on-behalf-of-user action. Callers that genuinely need cross-principal
	// grant-based access to global/workspace KV can still attach an
	// explicit AuthorizationContext (authority_mode="on_behalf_of") to
	// bypass this guard.
	if resolvedAuthority == nil && isUserBoundKVScope(op.GetScope()) {
		inherited, inheritedErr := s.loadCallerTaskAuthority(ctx, client, ident)
		if inheritedErr != nil {
			logging.Logger.Warn().Err(inheritedErr).Str("identity", ident.String()).Msg("failed to load caller task authority for KVOp")
		}
		if inherited != nil {
			resolvedAuthority = inherited
		} else {
			// Fall back to message-scoped authority — the task grant as USED
			// by the caller (not delegated from). This lets an agent use the
			// grant attached to its associated task for KV reads/writes in
			// the user's workspace, matching how message-send authority is
			// resolved.
			msgAuth, msgErr := s.loadCallerMessageAuthority(ctx, client, ident)
			if msgErr != nil {
				logging.Logger.Warn().Err(msgErr).Str("identity", ident.String()).Msg("failed to load caller message authority for KVOp")
			}
			if msgAuth != nil {
				resolvedAuthority = msgAuth
			}
		}
	}

	err = handler.HandleKVOperation(ctx, ident, sessionUUID, resolvedAuthority, op, sendResponse)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("KV operation failed")
		// Send generic error to avoid leaking internal details (e.g. Redis internals) to clients.
		sendClientError(client, "KV_ERROR", "internal error processing KV operation", withRequestID(op.GetRequestId()))
	} else {
		workspace := op.Workspace
		if workspace == "" {
			workspace = ident.Workspace
		}
		metering.KVOperations.WithLabelValues(workspace, op.GetOp().String()).Inc()
	}
}

func (s *GatewayServer) handleCheckpointOp(ctx context.Context, client *ClientSession, op *pb.CheckpointOperation) {
	// Snapshot identity under RLock to avoid data race with handleSwitchWorkspace
	client.identityMu.RLock()
	ident := client.Identity
	client.identityMu.RUnlock()
	requestID := op.GetRequestId()

	// Check if checkpoint store is available
	if s.checkpoints == nil {
		if sendErr := client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Checkpoint{
				Checkpoint: &pb.CheckpointResponse{
					Success:   false,
					Error:     "checkpoint store not configured",
					RequestId: requestID,
				},
			},
		}); sendErr != nil {
			logging.Logger.Error().Err(sendErr).Msg("failed to send checkpoint not configured response")
		}
		return
	}

	var response *pb.CheckpointResponse

	switch op.Op {
	case pb.CheckpointOperation_SAVE:
		// Handle TTL:
		//   -1 = use server default TTL
		//    0 = no expiration
		//   >0 = specific TTL in seconds
		var ttl time.Duration
		if op.Ttl < 0 {
			ttl = s.checkpointDefaultTTL // Use server default (may be 0 for no expiration)
		} else {
			ttl = time.Duration(op.Ttl) * time.Second
		}
		err := s.checkpoints.Save(ctx, ident, op.Key, op.Data, ttl)
		if err != nil {
			response = &pb.CheckpointResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			response = &pb.CheckpointResponse{
				Success: true,
				SavedAt: time.Now().Unix(),
			}
			logging.Logger.Debug().Str("identity", ident.String()).Str("key", op.Key).Int("size", len(op.Data)).Msg("checkpoint saved")
		}

	case pb.CheckpointOperation_LOAD:
		cp, err := s.checkpoints.Load(ctx, ident, op.Key)
		if err != nil {
			response = &pb.CheckpointResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else if cp == nil {
			response = &pb.CheckpointResponse{
				Success: true,
				Data:    nil, // Not found, but not an error
			}
		} else {
			response = &pb.CheckpointResponse{
				Success: true,
				Data:    cp.Data,
				SavedAt: cp.SavedAt.Unix(),
			}
			logging.Logger.Debug().Str("identity", ident.String()).Str("key", op.Key).Int("size", len(cp.Data)).Msg("checkpoint loaded")
		}

	case pb.CheckpointOperation_DELETE:
		err := s.checkpoints.Delete(ctx, ident, op.Key)
		if err != nil {
			response = &pb.CheckpointResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			response = &pb.CheckpointResponse{
				Success: true,
			}
			logging.Logger.Debug().Str("identity", ident.String()).Str("key", op.Key).Msg("checkpoint deleted")
		}

	case pb.CheckpointOperation_LIST:
		keys, err := s.checkpoints.List(ctx, ident)
		if err != nil {
			response = &pb.CheckpointResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			response = &pb.CheckpointResponse{
				Success: true,
				Keys:    keys,
			}
		}

	default:
		response = &pb.CheckpointResponse{
			Success: false,
			Error:   "unknown checkpoint operation",
		}
	}

	// Copy request_id for correlation
	response.RequestId = requestID

	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Checkpoint{
			Checkpoint: response,
		},
	}); err != nil {
		logging.Logger.Error().Err(err).Str("identity", ident.String()).Msg("failed to send checkpoint response")
	}

	if response.Success {
		metering.CheckpointOperations.WithLabelValues(ident.Workspace, op.Op.String()).Inc()
	}
}

func (s *GatewayServer) sendBaselineConfig(ctx context.Context, client *ClientSession) {
	// CRIT-1: Snapshot identity under RLock before use
	client.identityMu.RLock()
	identity := client.Identity // value copy
	client.identityMu.RUnlock()

	// Pushes per-agent (EXCLUSIVE) KV baseline to Agents/Tasks. Shared
	// workspace/global data is queryable on demand and intentionally
	// excluded from the baseline snapshot — those scopes are unbounded
	// across all agents and should not be eagerly hydrated. Per-agent
	// exclusive scopes ARE bounded (one agent's own data) and useful
	// to ship at connect time so handlers can run synchronously.
	if identity.Type != models.PrincipalAgent && identity.Type != models.PrincipalTask {
		return
	}

	// Per-agent workspace-scoped KV (workspace-exclusive).
	workspaceExclusiveKV, err := s.kv.List(ctx, identity, kv.ScopeWorkspaceExclusive, "", identity.Workspace)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("failed to list workspace-exclusive KV")
		workspaceExclusiveKV = nil
	}

	// Per-agent global-scoped KV (global-exclusive).
	globalExclusiveKV, err := s.kv.List(ctx, identity, kv.ScopeGlobalExclusive, "", "")
	if err != nil {
		logging.Logger.Error().Err(err).Msg("failed to list global-exclusive KV")
		globalExclusiveKV = nil
	}

	// Build task_context from pending TaskAssignment (if any)
	taskContext := s.buildTaskContext(ctx, client, identity)

	// Coerce map[string]string → map[string][]byte at the gRPC boundary.
	workspaceExclusiveBytes := make(map[string][]byte, len(workspaceExclusiveKV))
	for k, v := range workspaceExclusiveKV {
		workspaceExclusiveBytes[k] = []byte(v)
	}
	globalExclusiveBytes := make(map[string][]byte, len(globalExclusiveKV))
	for k, v := range globalExclusiveKV {
		globalExclusiveBytes[k] = []byte(v)
	}

	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Config{
			Config: &pb.ConfigSnapshot{
				WorkspaceExclusiveKv: workspaceExclusiveBytes,
				GlobalExclusiveKv:    globalExclusiveBytes,
				TaskContext:          taskContext,
				// Legacy Kv/GlobalKv fields are intentionally not populated;
				// they are deprecated and old SDK clients will see empty maps.
			},
		},
	}); err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("failed to send baseline config")
	}
}

// buildTaskContext looks up the active startup task for the connecting agent
// and returns a map of task context fields. Returns nil if no task is found
// or if orchestration is not configured.
// If client is non-nil and client.AssociatedTaskID is currently empty, the
// discovered task ID is stamped onto the session so that subsequent calls to
// loadCallerMessageAuthority (which bail early when AssociatedTaskID=="") can
// resolve the task-bound grant for outbound messages.
func (s *GatewayServer) buildTaskContext(ctx context.Context, client *ClientSession, identity models.Identity) map[string]string {
	if s.taskStore == nil {
		return nil
	}

	// Only agents get task context from startup tasks
	if identity.Type != models.PrincipalAgent {
		return nil
	}

	// Look up the active startup task for this agent's (implementation,
	// workspace, specifier). Specifier is included so a per-user singleton
	// agent's connect resolves to its own startup task, not a sibling user's.
	hasActive, taskID, err := s.taskStore.HasActiveStartupTask(ctx, identity.Implementation, identity.Workspace, identity.Specifier)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("identity", identity.String()).Msg("failed to check for active startup task")
		return nil
	}
	if !hasActive {
		return nil
	}

	// Stamp the discovered task ID onto the session so that
	// loadCallerMessageAuthority can resolve the task-bound grant for
	// outbound messages. Only set when currently empty — never overwrite an
	// ID that was supplied via task-token auth in authenticateCredentials.
	// AssociatedTaskID has no dedicated mutex; existing writers use plain
	// assignment, and this path is single-goroutine during initial config
	// delivery before the session is live for concurrent message handling.
	if client != nil && client.AssociatedTaskID == "" {
		client.AssociatedTaskID = taskID
		logging.Logger.Info().
			Str("identity", identity.String()).
			Str("task_id", taskID).
			Msg("stamped AssociatedTaskID onto session from discovered startup task")
	}

	// Get full task details
	task, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to get startup task details for task_context")
		return nil
	}

	// Build task_context map
	tc := make(map[string]string)
	tc["task_id"] = task.TaskID
	tc["workspace"] = task.Workspace
	tc["implementation"] = task.TargetImplementation
	if task.Specifier != "" {
		tc["specifier"] = task.Specifier
	} else if identity.Specifier != "" {
		tc["specifier"] = identity.Specifier
	}

	// Populate user fields from OBO authority so pool-dispatched agents can
	// identify the originating user and root user without additional lookups.
	applyAuthorityToTaskContext(tc, task.Authority)

	// Extract profile from launch params if available
	if profile, ok := task.LaunchParams["profile"].(string); ok && profile != "" {
		tc["profile"] = profile
	}

	// Merge all metadata entries directly
	for k, v := range task.Metadata {
		if str, ok := v.(string); ok {
			tc[k] = str
		}
	}

	// Merge all launch params with "lp." prefix
	for k, v := range task.LaunchParams {
		if str, ok := v.(string); ok {
			tc["lp."+k] = str
		}
	}

	logging.Logger.Info().Str("task_id", taskID).Str("identity", identity.String()).Int("fields", len(tc)).Msg("populated task_context from startup task")
	return tc
}

// applyAuthorityToTaskContext writes "user" and/or "root_user" into tc based
// on the OBO authority lineage attached to the task. Only subject/root entries
// whose type is exactly "user" are propagated; service and task principals are
// intentionally excluded so callers never see a non-user identity in those keys.
func applyAuthorityToTaskContext(tc map[string]string, auth tasks.TaskAuthorityInfo) {
	if auth.SubjectType == "user" && auth.SubjectID != "" {
		tc["user"] = auth.SubjectID
	}
	if auth.RootSubjectType == "user" && auth.RootSubjectID != "" {
		tc["root_user"] = auth.RootSubjectID
	}
}

// taskStatusToProto converts a tasks.TaskStatus to a pb.TaskStatus enum.
func taskStatusToProto(s tasks.TaskStatus) pb.TaskStatus {
	switch s {
	case tasks.TaskStatusPending, tasks.TaskStatusAssigned, tasks.TaskStatusStarting:
		return pb.TaskStatus_TASK_STATUS_QUEUED
	case tasks.TaskStatusRunning:
		return pb.TaskStatus_TASK_STATUS_RUNNING
	case tasks.TaskStatusCompleted:
		return pb.TaskStatus_TASK_STATUS_COMPLETED
	case tasks.TaskStatusFailed, tasks.TaskStatusDLQ:
		return pb.TaskStatus_TASK_STATUS_FAILED
	case tasks.TaskStatusCancelled:
		return pb.TaskStatus_TASK_STATUS_CANCELLED
	// Phase 1: A2A-aligned paused states.
	case tasks.TaskStatusWaitingInput:
		return pb.TaskStatus_TASK_STATUS_WAITING_INPUT
	case tasks.TaskStatusWaitingAuthority:
		return pb.TaskStatus_TASK_STATUS_WAITING_AUTHORITY
	case tasks.TaskStatusWaitingDependency:
		return pb.TaskStatus_TASK_STATUS_WAITING_DEPENDENCY
	case tasks.TaskStatusHibernated:
		return pb.TaskStatus_TASK_STATUS_HIBERNATED
	case tasks.TaskStatusRejected:
		return pb.TaskStatus_TASK_STATUS_REJECTED
	default:
		return pb.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

// protoTaskStatusToTasks converts a pb.TaskStatus enum to a tasks.TaskStatus.
func protoTaskStatusToTasks(s pb.TaskStatus) tasks.TaskStatus {
	switch s {
	case pb.TaskStatus_TASK_STATUS_QUEUED:
		return tasks.TaskStatusPending
	case pb.TaskStatus_TASK_STATUS_RUNNING:
		return tasks.TaskStatusRunning
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		return tasks.TaskStatusCompleted
	case pb.TaskStatus_TASK_STATUS_FAILED:
		return tasks.TaskStatusFailed
	case pb.TaskStatus_TASK_STATUS_CANCELLED:
		return tasks.TaskStatusCancelled
	// Phase 1: A2A-aligned paused states.
	case pb.TaskStatus_TASK_STATUS_WAITING_INPUT:
		return tasks.TaskStatusWaitingInput
	case pb.TaskStatus_TASK_STATUS_WAITING_AUTHORITY:
		return tasks.TaskStatusWaitingAuthority
	case pb.TaskStatus_TASK_STATUS_WAITING_DEPENDENCY:
		return tasks.TaskStatusWaitingDependency
	case pb.TaskStatus_TASK_STATUS_HIBERNATED:
		return tasks.TaskStatusHibernated
	case pb.TaskStatus_TASK_STATUS_REJECTED:
		return tasks.TaskStatusRejected
	default:
		return tasks.TaskStatusPending
	}
}

// unixOrZero returns the unix-seconds timestamp of t, or 0 when t is nil.
// Used in proto conversions where 0 sentinels "absent" for time fields.
func unixOrZero(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.Unix()
}

// taskToProto converts a tasks.Task to a protobuf TaskInfo message.
func taskToProto(t *tasks.Task) *pb.TaskInfo {
	info := &pb.TaskInfo{
		TaskId:                 t.TaskID,
		TaskType:               t.TaskType,
		TaskClass:              pb.TaskClass(t.TaskClass),
		DisconnectedAt:         unixOrZero(t.DisconnectedAt),
		GraceWindowMs:          t.GraceWindowMs,
		Status:                 taskStatusToProto(t.Status),
		Workspace:              t.Workspace,
		TargetTopic:            t.TargetTopic,
		AssignedTo:             t.AssignedTo,
		CreatedAt:              t.CreatedAt.Unix(),
		Attempt:                int32(t.RetryCount),
		MaxAttempts:            int32(t.MaxRetries),
		Error:                  t.ErrorMessage,
		AuthorityMode:          t.Authority.Mode,
		SubjectType:            t.Authority.SubjectType,
		SubjectId:              t.Authority.SubjectID,
		RootSubjectType:        t.Authority.RootSubjectType,
		RootSubjectId:          t.Authority.RootSubjectID,
		AuthorityGrantId:       t.Authority.AuthorityGrantID,
		RootAuthorityGrantId:   t.Authority.RootAuthorityGrantID,
		ParentAuthorityGrantId: t.Authority.ParentAuthorityGrantID,
		CreatorActorId:         t.ParentAgentID,
		ParentTaskId:           t.ParentTaskID,
	}
	if t.StartedAt != nil {
		info.StartedAt = t.StartedAt.Unix()
	}
	if t.CompletedAt != nil {
		info.CompletedAt = t.CompletedAt.Unix()
	}
	if t.Metadata != nil {
		info.Metadata = make(map[string]string, len(t.Metadata))
		for k, v := range t.Metadata {
			if str, ok := v.(string); ok {
				info.Metadata[k] = str
			}
		}
	}
	// Phase 1: A2A paused-state fields.
	if t.WaitSpec != nil {
		ws := &pb.WaitSpec{
			ExpectedPrincipal:   t.WaitSpec.ExpectedPrincipal,
			InputMatch:          t.WaitSpec.InputMatch,
			AuthorityRequestId:  t.WaitSpec.AuthorityRequestID,
			DependsOn:           t.WaitSpec.DependsOn,
			WakeOnAny:           t.WaitSpec.WakeOnAny,
			TimeoutMs:           t.WaitSpec.TimeoutMs,
			ScheduledWakeUnixMs: t.WaitSpec.ScheduledWakeUnixMs,
		}
		switch t.WaitSpec.Reason {
		case tasks.WaitReasonInput:
			ws.Reason = pb.WaitReason_WAIT_REASON_INPUT
		case tasks.WaitReasonAuthority:
			ws.Reason = pb.WaitReason_WAIT_REASON_AUTHORITY
		case tasks.WaitReasonDependency:
			ws.Reason = pb.WaitReason_WAIT_REASON_DEPENDENCY
		case tasks.WaitReasonHibernation:
			ws.Reason = pb.WaitReason_WAIT_REASON_HIBERNATION
		}
		if hib := t.WaitSpec.Hibernation; hib != nil {
			ws.Hibernation = &pb.HibernationDescriptor{
				CheckpointKey:    hib.CheckpointKey,
				ResumeSessionId:  hib.ResumeSessionID,
				WakeEventTypes:   hib.WakeEventTypes,
				EscalationPolicy: hib.EscalationPolicy,
			}
		}
		info.WaitSpec = ws
	}
	if len(t.DependsOn) > 0 {
		info.DependsOn = t.DependsOn
	}
	info.ContextId = t.ContextID
	info.PausedAt = unixOrZero(t.PausedAt)
	return info
}

func (s *GatewayServer) handleTaskQuery(ctx context.Context, client *ClientSession, query *pb.TaskQuery) {
	requestID := query.GetRequestId()

	if s.taskStore == nil {
		if sendErr := client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TaskQuery{
				TaskQuery: &pb.TaskQueryResponse{
					Success:   false,
					Error:     "task store not configured",
					RequestId: requestID,
				},
			},
		}); sendErr != nil {
			logging.Logger.Error().Err(sendErr).Msg("failed to send task query not configured response")
		}
		return
	}

	var response *pb.TaskQueryResponse

	switch query.Op {
	case pb.TaskQuery_GET:
		task, err := s.taskStore.GetTask(ctx, query.TaskId)
		if err != nil {
			response = &pb.TaskQueryResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else if task == nil {
			response = &pb.TaskQueryResponse{
				Success: false,
				Error:   "task not found",
			}
		} else {
			response = &pb.TaskQueryResponse{
				Success: true,
				Task:    taskToProto(task),
			}
		}

	case pb.TaskQuery_LIST:
		filter := &tasks.TaskFilter{}
		if query.Filter != nil {
			// Prefer repeated statuses over singular status
			if len(query.Filter.Statuses) > 0 {
				for _, s := range query.Filter.Statuses {
					if s != pb.TaskStatus_TASK_STATUS_UNSPECIFIED {
						filter.Statuses = append(filter.Statuses, protoTaskStatusToTasks(s))
					}
				}
			} else if query.Filter.Status != pb.TaskStatus_TASK_STATUS_UNSPECIFIED {
				status := protoTaskStatusToTasks(query.Filter.Status)
				filter.Status = &status
			}
			filter.Workspace = query.Filter.Workspace
			filter.TaskType = query.Filter.TaskType
			filter.TaskClass = int32(query.Filter.TaskClass)
			if len(query.Filter.ExcludeTaskClasses) > 0 {
				filter.ExcludeTaskClasses = make([]int32, 0, len(query.Filter.ExcludeTaskClasses))
				for _, c := range query.Filter.ExcludeTaskClasses {
					filter.ExcludeTaskClasses = append(filter.ExcludeTaskClasses, int32(c))
				}
			}
			filter.SubjectType = query.Filter.SubjectType
			filter.SubjectID = query.Filter.SubjectId
			filter.AuthorityMode = query.Filter.AuthorityMode
			filter.AuthorityGrantID = query.Filter.AuthorityGrantId
			filter.RootAuthorityGrantID = query.Filter.RootAuthorityGrantId
			filter.ParentTaskID = query.Filter.ParentTaskId
			// Phase 1: A2A filter fields.
			filter.ContextID = query.Filter.ContextId
			if len(query.Filter.ExcludeStatuses) > 0 {
				filter.ExcludeStatuses = make([]tasks.TaskStatus, 0, len(query.Filter.ExcludeStatuses))
				for _, s := range query.Filter.ExcludeStatuses {
					filter.ExcludeStatuses = append(filter.ExcludeStatuses, protoTaskStatusToTasks(s))
				}
			}
			if query.Filter.Limit > 0 {
				filter.Limit = int(query.Filter.Limit)
				if filter.Limit > 1000 {
					filter.Limit = 1000
				}
			}
			filter.Offset = int(query.Filter.Offset)
		}

		taskList, err := s.taskStore.ListTasks(ctx, filter)
		if err != nil {
			response = &pb.TaskQueryResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			protoTasks := make([]*pb.TaskInfo, len(taskList))
			for i, t := range taskList {
				protoTasks[i] = taskToProto(t)
			}
			response = &pb.TaskQueryResponse{
				Success:    true,
				Tasks:      protoTasks,
				TotalCount: int32(len(protoTasks)),
			}
		}

	default:
		response = &pb.TaskQueryResponse{
			Success: false,
			Error:   "unknown task query operation",
		}
	}

	// Copy request_id for correlation
	response.RequestId = requestID

	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskQuery{
			TaskQuery: response,
		},
	}); err != nil {
		client.identityMu.RLock()
		identStr := client.Identity.String()
		client.identityMu.RUnlock()
		logging.Logger.Error().Err(err).Str("identity", identStr).Msg("failed to send task query response")
	}
}

func (s *GatewayServer) handleTaskOp(ctx context.Context, client *ClientSession, op *pb.TaskOperation) {
	requestID := op.GetRequestId()

	if s.taskStore == nil {
		if sendErr := client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TaskOp{
				TaskOp: &pb.TaskOperationResponse{
					Success:   false,
					Error:     "task store not configured",
					RequestId: requestID,
				},
			},
		}); sendErr != nil {
			logging.Logger.Error().Err(sendErr).Msg("failed to send task op not configured response")
		}
		return
	}

	var response *pb.TaskOperationResponse

	switch op.Op {
	case pb.TaskOperation_CANCEL:
		// Authorization: verify caller's workspace matches the task's workspace
		cancelTask, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || cancelTask == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerWorkspaceCancel := client.Identity.Workspace
		client.identityMu.RUnlock()
		if callerWorkspaceCancel != "" && cancelTask.Workspace != callerWorkspaceCancel {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task belongs to a different workspace",
			}
			break
		}
		cancelFn := s.taskStore.CancelTask
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			cancelFn = s.orchestration.TaskService.CancelTask
		}
		if err := cancelFn(ctx, op.TaskId); err != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			// Fetch the updated task to return
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task cancelled",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, "cancelled", "")
		}

	case pb.TaskOperation_RETRY:
		// Authorization: verify caller's workspace matches the task's workspace
		retryTask, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || retryTask == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerWorkspaceRetry := client.Identity.Workspace
		client.identityMu.RUnlock()
		if callerWorkspaceRetry != "" && retryTask.Workspace != callerWorkspaceRetry {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task belongs to a different workspace",
			}
			break
		}
		if err := s.taskStore.RetryTask(ctx, op.TaskId); err != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			// Fetch the updated task to return
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task queued for retry",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, "pending", "")
		}

	case pb.TaskOperation_COMPLETE:
		// Authorization: only the assigned agent may complete a task
		task, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || task == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerTopic := client.Identity.ToTopic()
		client.identityMu.RUnlock()
		if task.AssignedTo != "" && task.AssignedTo != callerTopic {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task is assigned to a different agent",
			}
			break
		}
		completeFn := s.taskStore.CompleteTask
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			completeFn = s.orchestration.TaskService.CompleteTask
		}
		if err := completeFn(ctx, op.TaskId); err != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			// Re-fetch to get updated state
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task completed",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, "completed", "")
		}

	case pb.TaskOperation_FAIL:
		errMsg := op.Reason
		if errMsg == "" {
			errMsg = "task failed"
		}
		// Authorization: only the assigned agent may fail a task
		task, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || task == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerTopic := client.Identity.ToTopic()
		client.identityMu.RUnlock()
		if task.AssignedTo != "" && task.AssignedTo != callerTopic {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task is assigned to a different agent",
			}
			break
		}
		failFn := s.taskStore.FailTask
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			failFn = s.orchestration.TaskService.FailTask
		}
		if err := failFn(ctx, op.TaskId, errMsg); err != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   err.Error(),
			}
		} else {
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task marked as failed",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, "failed", errMsg)
		}

	case pb.TaskOperation_PAUSE:
		// PAUSE: transition running -> WAITING_* with a typed wait reason.
		// Validation: wait_spec required (it carries the WaitReason that
		// determines the target waiting status).
		if op.WaitSpec == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "wait_spec required for PAUSE",
			}
			break
		}
		pauseTask, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || pauseTask == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerWorkspacePause := client.Identity.Workspace
		client.identityMu.RUnlock()
		if callerWorkspacePause != "" && pauseTask.Workspace != callerWorkspacePause {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task belongs to a different workspace",
			}
			break
		}
		toStatus := waitReasonToStatus(op.WaitSpec.GetReason())
		spec := protoWaitSpecToTasks(op.WaitSpec)
		// Phase 3: HIBERNATE precondition. Workers must SAVE a checkpoint
		// before requesting hibernation (so the rehydrated worker on wake can
		// LOAD their prior state). Reject the transition without state change
		// if the descriptor is missing or the named checkpoint does not exist
		// for the task's assignee identity. Checkpoints are scoped per identity
		// in the checkpoint store.
		if op.WaitSpec.GetReason() == pb.WaitReason_WAIT_REASON_HIBERNATION {
			if precondErr := s.validateHibernationPrecondition(ctx, pauseTask, spec); precondErr != nil {
				response = &pb.TaskOperationResponse{
					Success: false,
					Error:   precondErr.Error(),
				}
				break
			}
		}
		var pauseErr error
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			pauseErr = s.orchestration.TaskService.PauseTask(ctx, op.TaskId, toStatus, spec)
		} else {
			pauseErr = s.taskStore.PauseTask(ctx, op.TaskId, toStatus, spec)
		}
		if pauseErr != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   pauseErr.Error(),
			}
		} else {
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task paused",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, string(toStatus), "")
			// Stage B: signal the worker assigned to a hibernating task that
			// it should disconnect cleanly. The disconnect_reaper skips
			// HIBERNATED tasks, so an unanswered TaskHibernated is safe —
			// workers that don't disconnect themselves are reaped via
			// normal session-expiry semantics.
			if op.WaitSpec.GetReason() == pb.WaitReason_WAIT_REASON_HIBERNATION {
				notifyTask := updated
				if notifyTask == nil {
					notifyTask = pauseTask
				}
				s.notifyTaskHibernated(ctx, notifyTask, spec)
			}
		}

	case pb.TaskOperation_WAIT_FOR:
		// WAIT_FOR: specialization of PAUSE for spontaneous dependencies.
		// Validation: WaitSpec.depends_on must be non-empty; reason defaults
		// to DEPENDENCY (and is forced to DEPENDENCY for routing). All
		// referenced dependency task ids must exist in the caller's
		// workspace (info-hiding: missing/foreign tasks reported as
		// "dependency not found").
		if op.WaitSpec == nil || len(op.WaitSpec.GetDependsOn()) == 0 {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "wait_spec.depends_on required for WAIT_FOR",
			}
			break
		}
		waitTask, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || waitTask == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerWorkspaceWait := client.Identity.Workspace
		client.identityMu.RUnlock()
		if callerWorkspaceWait != "" && waitTask.Workspace != callerWorkspaceWait {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task belongs to a different workspace",
			}
			break
		}
		// Validate every referenced dependency is in the same workspace.
		depValid := true
		var depErr string
		for _, depID := range op.WaitSpec.GetDependsOn() {
			if depID == "" {
				continue
			}
			dep, err := s.taskStore.GetTask(ctx, depID)
			if err != nil || dep == nil {
				depValid = false
				depErr = "dependency not found: " + depID
				break
			}
			if dep.Workspace != waitTask.Workspace {
				depValid = false
				depErr = "dependency not found: " + depID
				break
			}
		}
		if !depValid {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   depErr,
			}
			break
		}
		spec := protoWaitSpecToTasks(op.WaitSpec)
		// Force the reason to DEPENDENCY regardless of caller-supplied value;
		// WAIT_FOR is the dependency op.
		spec.Reason = tasks.WaitReasonDependency
		toStatus := tasks.TaskStatusWaitingDependency
		var waitErr error
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			waitErr = s.orchestration.TaskService.PauseTask(ctx, op.TaskId, toStatus, spec)
		} else {
			waitErr = s.taskStore.PauseTask(ctx, op.TaskId, toStatus, spec)
		}
		if waitErr != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   waitErr.Error(),
			}
		} else {
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task waiting on dependencies",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, string(toStatus), "")
		}

	case pb.TaskOperation_RESUME:
		// RESUME: force-resume a paused task back to running. Normally the
		// task_waker handles this; this op is the admin/manual override.
		resumeTask, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || resumeTask == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerWorkspaceResume := client.Identity.Workspace
		client.identityMu.RUnlock()
		if callerWorkspaceResume != "" && resumeTask.Workspace != callerWorkspaceResume {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task belongs to a different workspace",
			}
			break
		}
		toStatus := tasks.TaskStatusRunning
		var resumeErr error
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			resumeErr = s.orchestration.TaskService.ResumeTask(ctx, op.TaskId, toStatus)
		} else {
			resumeErr = s.taskStore.ResumeTask(ctx, op.TaskId, toStatus)
		}
		if resumeErr != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   resumeErr.Error(),
			}
		} else {
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task resumed",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, string(toStatus), "")
		}

	case pb.TaskOperation_REJECT:
		// REJECT: terminal — agent declines before processing. Reuses the
		// CancelTask cleanup pattern (revoke tokens + authority grant,
		// retire orchestrated_task_queue row, fire dependency wake).
		rejectTask, err := s.taskStore.GetTask(ctx, op.TaskId)
		if err != nil || rejectTask == nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "task not found",
			}
			break
		}
		client.identityMu.RLock()
		callerWorkspaceReject := client.Identity.Workspace
		client.identityMu.RUnlock()
		if callerWorkspaceReject != "" && rejectTask.Workspace != callerWorkspaceReject {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   "not authorized: task belongs to a different workspace",
			}
			break
		}
		reason := op.Reason
		if reason == "" {
			reason = "rejected"
		}
		var rejectErr error
		if s.orchestration != nil && s.orchestration.TaskService != nil {
			rejectErr = s.orchestration.TaskService.RejectTask(ctx, op.TaskId, reason)
		} else {
			rejectErr = s.taskStore.RejectTask(ctx, op.TaskId, reason)
		}
		if rejectErr != nil {
			response = &pb.TaskOperationResponse{
				Success: false,
				Error:   rejectErr.Error(),
			}
		} else {
			updated, _ := s.taskStore.GetTask(ctx, op.TaskId)
			response = &pb.TaskOperationResponse{
				Success: true,
				Message: "task rejected",
			}
			if updated != nil {
				response.Task = taskToProto(updated)
			}
			s.notifyTaskStatusChangeFromTaskID(ctx, op.TaskId, string(tasks.TaskStatusRejected), reason)
		}

	default:
		response = &pb.TaskOperationResponse{
			Success: false,
			Error:   "unknown task operation",
		}
	}

	// Copy request_id for correlation
	response.RequestId = requestID

	if response.Success {
		client.identityMu.RLock()
		workspace := client.Identity.Workspace
		client.identityMu.RUnlock()
		metering.TaskOperations.WithLabelValues(workspace, op.Op.String()).Inc()
	}

	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskOp{
			TaskOp: response,
		},
	}); err != nil {
		client.identityMu.RLock()
		identStr := client.Identity.String()
		client.identityMu.RUnlock()
		logging.Logger.Error().Err(err).Str("identity", identStr).Msg("failed to send task operation response")
	}
}
