package gateway

import (
	"context"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// Foreign audit-event error codes returned via SubmitAuditEventResponse.
const (
	errAuditTypeForbidden = "ERR_AUDIT_TYPE_FORBIDDEN"
	errAuditPermDenied    = "ERR_PERMISSION_DENIED"
	errAuditRateLimited   = "ERR_AUDIT_RATE_LIMITED"
	errAuditDisabled      = "ERR_AUDIT_DISABLED"
)

// handleSubmitAuditEvent processes a SubmitAuditEventRequest from a connected
// principal. Identity is stamped from the authenticated session;
// client-supplied actor fields are ignored. Metadata is unconditionally
// sanitized for credential patterns before persistence.
//
// Rejected event types (gateway-truth categories that an agent must not be
// able to forge): connection, auth, admin, acl. Accepted: message, kv, task,
// custom.
//
// Cross-workspace submissions (where req.Workspace != client.Identity.Workspace
// and is non-empty) require capability/audit_submit on the requested workspace.
//
// Per-principal rate limit is enforced via QuotaEnforcer.foreignAuditRateLimiter
// when present. On limit breach the handler returns ERR_AUDIT_RATE_LIMITED.
func (s *GatewayServer) handleSubmitAuditEvent(ctx context.Context, client *ClientSession, req *pb.SubmitAuditEventRequest) {
	requestID := req.GetClientRequestId()

	if s.auditLogger == nil {
		sendSubmitAuditError(client, requestID, errAuditDisabled, "audit logging not enabled")
		return
	}

	// 1. Validate event type against whitelist.
	if !isAllowedForeignAuditEventType(req.GetEventType()) {
		sendSubmitAuditError(client, requestID, errAuditTypeForbidden,
			"event type not permitted for principal-submitted events: "+req.GetEventType())
		return
	}

	// Snapshot authenticated identity under the client's identity lock.
	client.identityMu.RLock()
	identity := client.Identity
	client.identityMu.RUnlock()

	actorIdentity := identity.String()

	// 2. Determine effective workspace and ACL-gate cross-workspace submissions.
	effectiveWorkspace := identity.Workspace
	requestedWorkspace := req.GetWorkspace()
	if requestedWorkspace != "" && requestedWorkspace != identity.Workspace {
		if s.acl == nil {
			sendSubmitAuditError(client, requestID, errAuditPermDenied,
				"cross-workspace audit submission requires capability/audit_submit but ACL service is unavailable")
			return
		}
		decision, err := s.acl.CheckAccess(
			ctx, identity,
			acl.ResourceTypeCapability, acl.PermissionAuditSubmit,
			"audit_submit", requestedWorkspace, client.SessionUUID, acl.AccessRead,
		)
		if err != nil || decision == nil || !decision.Allowed {
			sendSubmitAuditError(client, requestID, errAuditPermDenied,
				"cross-workspace audit submission requires capability/audit_submit on the target workspace")
			return
		}
		effectiveWorkspace = requestedWorkspace
	}

	// 3. Per-principal rate limit.
	if rl := s.quotaEnforcer.foreignAuditRateLimiter; rl != nil {
		if !rl.Allow(actorIdentity) {
			sendSubmitAuditError(client, requestID, errAuditRateLimited,
				"foreign audit submission rate limit exceeded")
			return
		}
	}

	// 4. Build AuditEvent. ActorType/ActorID/SessionID/GatewayID are stamped
	//    from the authenticated session; client-supplied actor metadata is
	//    preserved (under the metadata map) but never overrides the trusted
	//    fields.
	metadata := convertStringMapToInterface(req.GetMetadata())
	metadata = audit.SanitizeMetadata(metadata)

	event := &audit.AuditEvent{
		EventType:    req.GetEventType(),
		ActorType:    actorPrincipalTypeString(identity.Type),
		ActorID:      actorIdentity,
		Operation:    req.GetOperation(),
		ResourceType: req.GetResourceType(),
		ResourceID:   req.GetResourceId(),
		Workspace:    effectiveWorkspace,
		SessionID:    client.SessionUUID,
		GatewayID:    s.gatewayID,
		Success:      req.GetSuccess(),
		ErrorMessage: req.GetErrorMessage(),
		Metadata:     metadata,
		Source:       audit.SourcePrincipal,
	}

	// 5. Submit asynchronously (drop-on-overflow) and acknowledge.
	s.auditLogger.LogEvent(ctx, event)

	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_SubmitAuditEventResponse{
			SubmitAuditEventResponse: &pb.SubmitAuditEventResponse{
				ClientRequestId: requestID,
				Success:         true,
			},
		},
	})
}

// isAllowedForeignAuditEventType reports whether a principal-submitted event
// of the given type is permitted. Gateway-truth categories (connection / auth
// / admin / acl) are reserved for server emission only.
func isAllowedForeignAuditEventType(eventType string) bool {
	switch eventType {
	case audit.EventTypeMessage, audit.EventTypeKV, audit.EventTypeTask, audit.EventTypeCustom:
		return true
	default:
		return false
	}
}

// actorPrincipalTypeString maps a models.PrincipalType to the lowercase form
// used in audit_log.actor_type. Mirrors audit.NormalizePrincipalTypeCase but
// works from the typed PrincipalType enum.
func actorPrincipalTypeString(pt models.PrincipalType) string {
	return audit.NormalizePrincipalTypeCase(string(pt))
}

// convertStringMapToInterface converts a map[string]string (proto map type)
// into the map[string]interface{} expected by AuditEvent.Metadata. Returns a
// non-nil empty map for nil or empty input so prepareEvent doesn't need to
// initialize it again.
func convertStringMapToInterface(m map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// sendSubmitAuditError sends a SubmitAuditEventResponse with success=false.
func sendSubmitAuditError(client *ClientSession, requestID, code, message string) {
	logging.Logger.Debug().Str("identity", client.Identity.String()).Str("code", code).Str("message", message).Msg("foreign audit submit rejected")
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_SubmitAuditEventResponse{
			SubmitAuditEventResponse: &pb.SubmitAuditEventResponse{
				ClientRequestId: requestID,
				Success:         false,
				ErrorCode:       code,
				ErrorMessage:    message,
			},
		},
	})
}
