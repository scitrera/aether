package gateway

import (
	"context"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// handleSessionOp processes a SessionOperation from a connected client.
// Mirrors handleAgentOp: a single op enum drives reads (LIST/GET) and
// mutations (DISCONNECT) over the streaming API. The REST equivalents in
// admin_connections.go remain the source of truth for the underlying state
// access; this handler just wraps the same provider methods.
//
// ACL gating runs inside the handler (not in the connect.go dispatch loop)
// so the call can resolve an optional OBO authority context from the proto
// — admin dashboards run on behalf of an authenticated user, not the
// service principal that opened the gRPC connection.
func (s *GatewayServer) handleSessionOp(ctx context.Context, client *ClientSession, identity models.Identity, op *pb.SessionOperation) {
	if s.adminProvider == nil {
		sendSessionError(client, op.RequestId, "admin provider not configured")
		return
	}

	if !s.isAllowedSessionOp(ctx, client, identity, op) {
		return // isAllowedSessionOp sends the error response
	}

	switch op.Op {
	case pb.SessionOperation_LIST:
		filter := protoConnectionFilterToAdmin(op.Filter)
		conns, err := s.adminProvider.GetConnections(ctx, filter)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleSessionOp: list connections failed")
			sendSessionError(client, op.RequestId, err.Error())
			return
		}
		protoConns := make([]*pb.ConnectionInfo, 0, len(conns))
		for _, c := range conns {
			protoConns = append(protoConns, adminConnectionToProto(c))
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_SessionResponse{
				SessionResponse: &pb.SessionOperationResponse{
					Success:     true,
					Connections: protoConns,
					TotalCount:  int32(len(protoConns)),
					RequestId:   op.RequestId,
				},
			},
		})

	case pb.SessionOperation_GET:
		if op.SessionId == "" {
			sendSessionError(client, op.RequestId, "session_id required for GET")
			return
		}
		conn, err := s.adminProvider.GetConnectionByID(ctx, op.SessionId)
		if err != nil {
			logging.Logger.Error().Err(err).Str("session_id", op.SessionId).Msg("handleSessionOp: get connection failed")
			sendSessionError(client, op.RequestId, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_SessionResponse{
				SessionResponse: &pb.SessionOperationResponse{
					Success:    true,
					Connection: adminConnectionToProto(conn),
					RequestId:  op.RequestId,
				},
			},
		})

	case pb.SessionOperation_DISCONNECT:
		if op.SessionId == "" {
			sendSessionError(client, op.RequestId, "session_id required for DISCONNECT")
			return
		}
		if err := s.adminProvider.DisconnectSession(ctx, op.SessionId); err != nil {
			logging.Logger.Error().Err(err).Str("session_id", op.SessionId).Msg("handleSessionOp: disconnect failed")
			sendSessionError(client, op.RequestId, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_SessionResponse{
				SessionResponse: &pb.SessionOperationResponse{
					Success:   true,
					Message:   "session disconnected",
					RequestId: op.RequestId,
				},
			},
		})

	default:
		sendSessionError(client, op.RequestId, "unknown session operation")
	}
}

// isAllowedSessionOp gates SessionOperation. System principals
// (WorkflowEngine/Orchestrator) pass through. Otherwise the caller needs
// either admin/* (umbrella) or admin/sessions (category-specific) at
// AccessAdmin. When op.Authorization is set, the check runs against the
// OBO subject (the user) so the service principal doesn't need the perm
// itself.
func (s *GatewayServer) isAllowedSessionOp(ctx context.Context, client *ClientSession, identity models.Identity, op *pb.SessionOperation) bool {
	resolvedAuthority, err := s.resolveAuthorizationContext(ctx, client, identity, op.GetAuthorization())
	if err != nil {
		sendSessionError(client, op.RequestId, "invalid authorization context: "+err.Error())
		return false
	}

	// System principals are implicitly allowed for direct (non-OBO) calls.
	if resolvedAuthority == nil {
		switch identity.Type {
		case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
			return true
		}
	}

	if s.acl == nil {
		sendSessionError(client, op.RequestId, "admin sessions operations require system-level privileges")
		return false
	}

	check := func(resourceID, operation string) (*acl.ACLDecision, error) {
		if resolvedAuthority != nil {
			return s.acl.CheckAccessWithAuthority(ctx, identity, resolvedAuthority,
				acl.ResourceTypeAdmin, resourceID, operation,
				identity.Workspace, client.SessionUUID, acl.AccessAdmin)
		}
		return s.acl.CheckAccess(ctx, identity,
			acl.ResourceTypeAdmin, resourceID, operation,
			identity.Workspace, client.SessionUUID, acl.AccessAdmin)
	}

	// Single check against admin/sessions; admin/* umbrella glob-matches via Casbin.
	if decision, err := check("admin/sessions", "session_op_sessions"); err == nil && decision != nil && decision.Allowed {
		return true
	}

	sendSessionError(client, op.RequestId, "admin sessions operations require system-level privileges or an explicit ACL grant")
	return false
}

func sendSessionError(client *ClientSession, requestID, msg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_SessionResponse{
			SessionResponse: &pb.SessionOperationResponse{
				Success:   false,
				Error:     msg,
				RequestId: requestID,
			},
		},
	})
}

// protoConnectionFilterToAdmin converts a proto ConnectionFilter to the
// admin layer's string-typed equivalent. Returns nil when the proto filter
// is nil so callers pass through to the provider's default behavior.
func protoConnectionFilterToAdmin(f *pb.ConnectionFilter) *admin.ConnectionFilter {
	if f == nil {
		return nil
	}
	return &admin.ConnectionFilter{
		Type:      protoPrincipalTypeToModelString(f.Type),
		Workspace: f.Workspace,
		Limit:     int(f.Limit),
		Offset:    int(f.Offset),
	}
}

// protoPrincipalTypeToModelString converts the proto enum to the canonical
// string used by models.PrincipalType ("Agent", "Task", ...). Empty string
// for UNSPECIFIED so the admin filter treats it as "no type filter".
func protoPrincipalTypeToModelString(t pb.PrincipalType) string {
	switch t {
	case pb.PrincipalType_PRINCIPAL_AGENT:
		return string(models.PrincipalAgent)
	case pb.PrincipalType_PRINCIPAL_TASK:
		return string(models.PrincipalTask)
	case pb.PrincipalType_PRINCIPAL_USER:
		return string(models.PrincipalUser)
	case pb.PrincipalType_PRINCIPAL_ORCHESTRATOR:
		return string(models.PrincipalOrchestrator)
	case pb.PrincipalType_PRINCIPAL_WORKFLOW_ENGINE:
		return string(models.PrincipalWorkflowEngine)
	case pb.PrincipalType_PRINCIPAL_METRICS_BRIDGE:
		return string(models.PrincipalMetricsBridge)
	case pb.PrincipalType_PRINCIPAL_BRIDGE:
		return string(models.PrincipalBridge)
	case pb.PrincipalType_PRINCIPAL_SERVICE:
		return string(models.PrincipalService)
	default:
		return ""
	}
}

// adminConnectionToProto converts admin.ConnectionInfo to pb.ConnectionInfo.
// Time fields become unix-second int64 (matches proto schema). Empty input
// returns nil so callers can short-circuit cleanly.
func adminConnectionToProto(c *admin.ConnectionInfo) *pb.ConnectionInfo {
	if c == nil {
		return nil
	}
	return &pb.ConnectionInfo{
		SessionId:      c.SessionID,
		Type:           modelPrincipalStringToProto(c.Type),
		Identity:       c.Identity,
		Workspace:      c.Workspace,
		Implementation: c.Implementation,
		Specifier:      c.Specifier,
		ConnectedAt:    c.ConnectedAt.Unix(),
		Duration:       c.Duration,
		RemoteAddr:     c.RemoteAddr,
		LastActivity:   c.LastActivity.Unix(),
	}
}

// modelPrincipalStringToProto converts a canonical models.PrincipalType
// string ("Agent", "Task", ...) to the proto enum. Distinct from
// principalTypeStringToProto in workspace_handler.go which accepts the
// lowercase REST API form.
func modelPrincipalStringToProto(t string) pb.PrincipalType {
	switch models.PrincipalType(t) {
	case models.PrincipalAgent:
		return pb.PrincipalType_PRINCIPAL_AGENT
	case models.PrincipalTask:
		return pb.PrincipalType_PRINCIPAL_TASK
	case models.PrincipalUser:
		return pb.PrincipalType_PRINCIPAL_USER
	case models.PrincipalOrchestrator:
		return pb.PrincipalType_PRINCIPAL_ORCHESTRATOR
	case models.PrincipalWorkflowEngine:
		return pb.PrincipalType_PRINCIPAL_WORKFLOW_ENGINE
	case models.PrincipalMetricsBridge:
		return pb.PrincipalType_PRINCIPAL_METRICS_BRIDGE
	case models.PrincipalBridge:
		return pb.PrincipalType_PRINCIPAL_BRIDGE
	case models.PrincipalService:
		return pb.PrincipalType_PRINCIPAL_SERVICE
	default:
		return pb.PrincipalType_PRINCIPAL_TYPE_UNSPECIFIED
	}
}
