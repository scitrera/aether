package gateway

import (
	"context"
	"encoding/json"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// handleAuditQuery processes a comprehensive audit log query from a connected client.
// Requires system-level admin access or workspace-scoped read access.
func (s *GatewayServer) handleAuditQuery(ctx context.Context, client *ClientSession, identity models.Identity, query *pb.AuditQuery) {
	requestID := query.RequestId

	// Check that audit logger is available
	if s.auditLogger == nil {
		sendAuditError(client, requestID, "audit logging not enabled")
		return
	}

	resolvedAuthority, err := s.resolveAuthorizationContext(ctx, client, identity, query.GetAuthorization())
	if err != nil {
		sendAuditError(client, requestID, "invalid authorization context: "+err.Error())
		return
	}

	// Permission check: system principals are allowed; others need admin_operations or workspace access
	if !s.isAllowedAuditQuery(ctx, client, identity, query, resolvedAuthority) {
		return // isAllowedAuditQuery sends the error response
	}

	// Build filter from query
	excludeActorTypes := make([]string, 0, len(query.ExcludeActorTypes))
	for _, t := range query.ExcludeActorTypes {
		if n := normalizeAuditPrincipalTypeFilter(t); n != "" {
			excludeActorTypes = append(excludeActorTypes, n)
		}
	}
	filter := audit.EventFilter{
		EventType:            query.EventType,
		ActorType:            normalizeAuditPrincipalTypeFilter(query.ActorType),
		ActorID:              query.ActorId,
		SubjectType:          normalizeAuditPrincipalTypeFilter(query.SubjectType),
		SubjectID:            query.SubjectId,
		AuthorityMode:        query.AuthorityMode,
		AuthorityGrantID:     query.AuthorityGrantId,
		ResourceType:         query.ResourceType,
		ResourceID:           query.ResourceId,
		Operation:            query.Operation,
		Workspace:            query.Workspace,
		ExcludeActorTypes:    excludeActorTypes,
		ExcludeWorkspaces:    append([]string(nil), query.ExcludeWorkspaces...),
		ExcludeServiceDirect: query.ExcludeServiceDirect,
		Limit:                int(query.Limit),
		Offset:               int(query.Offset),
	}

	if query.StartTime > 0 {
		t := time.Unix(query.StartTime, 0)
		filter.StartTime = &t
	}
	if query.EndTime > 0 {
		t := time.Unix(query.EndTime, 0)
		filter.EndTime = &t
	}
	if query.OnlyFailures {
		f := false
		filter.Success = &f
	}
	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 100
	}

	// Query the comprehensive audit log
	events, err := s.auditLogger.QueryAuditLog(ctx, filter)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("handleAuditQuery: query failed")
		sendAuditError(client, requestID, "audit query failed: "+err.Error())
		return
	}

	// Convert to proto entries
	entries := make([]*pb.AuditEntry, 0, len(events))
	for _, e := range events {
		entry := &pb.AuditEntry{
			AuditId:         e.AuditID,
			Timestamp:       e.Timestamp.Unix(),
			EventType:       e.EventType,
			ActorType:       e.ActorType,
			ActorId:         e.ActorID,
			ResourceType:    e.ResourceType,
			ResourceId:      e.ResourceID,
			Operation:       e.Operation,
			Workspace:       e.Workspace,
			SessionId:       e.SessionID.String(),
			GatewayId:       e.GatewayID,
			Success:         e.Success,
			ErrorMessage:    e.ErrorMessage,
			SubjectType:     e.SubjectType,
			SubjectId:       e.SubjectID,
			RootSubjectType: e.RootSubjectType,
			RootSubjectId:   e.RootSubjectID,
			AuthorityMode:   e.AuthorityMode,
			Source:          e.Source,
		}
		if e.RootAuthorityGrantID != nil {
			entry.RootAuthorityGrantId = *e.RootAuthorityGrantID
		}
		if e.AuthorityGrantID != nil {
			entry.AuthorityGrantId = *e.AuthorityGrantID
		}
		if e.ParentAuthorityGrantID != nil {
			entry.ParentAuthorityGrantId = *e.ParentAuthorityGrantID
		}
		if e.Metadata != nil {
			if b, err := json.Marshal(e.Metadata); err == nil {
				entry.MetadataJson = string(b)
			}
		}
		entries = append(entries, entry)
	}

	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuditResponse{
			AuditResponse: &pb.AuditQueryResponse{
				RequestId:  requestID,
				Success:    true,
				Entries:    entries,
				TotalCount: int32(len(entries)),
			},
		},
	})
}

// isAllowedAuditQuery checks whether the caller can query the audit log.
// System principals are allowed. Agents/users need admin_operations permission
// or workspace-level read access (if querying a specific workspace).
func (s *GatewayServer) isAllowedAuditQuery(ctx context.Context, client *ClientSession, identity models.Identity, query *pb.AuditQuery, authority *acl.ResolvedAuthority) bool {
	// System principals are implicitly allowed
	if authority == nil {
		switch identity.Type {
		case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
			return true
		}
	}

	// If no ACL service, deny (fail-closed for audit data)
	if s.acl == nil {
		sendAuditError(client, query.RequestId, "audit queries require admin privileges or workspace access")
		return false
	}

	check := func(resourceType, resourceID, operation, workspace string, level int) (*acl.ACLDecision, error) {
		if authority != nil {
			return s.acl.CheckAccessWithAuthority(ctx, identity, authority, resourceType, resourceID, operation, workspace, client.SessionUUID, level)
		}
		return s.acl.CheckAccess(ctx, identity, resourceType, resourceID, operation, workspace, client.SessionUUID, level)
	}

	// Single check against admin/audit; admin/* umbrella glob-matches via Casbin.
	decision, err := check(
		acl.ResourceTypeAdmin, "admin/audit",
		"audit_query", identity.Workspace, acl.AccessRead,
	)
	if err == nil && decision != nil && decision.Allowed {
		return true
	}

	// If querying a specific workspace, check workspace read access
	if query.Workspace != "" {
		decision, err = check(
			acl.ResourceTypeWorkspace, query.Workspace,
			"audit_query", query.Workspace, acl.AccessRead,
		)
		if err == nil && decision != nil && decision.Allowed {
			return true
		}
	}

	sendAuditError(client, query.RequestId, "insufficient permissions for audit query")
	return false
}

func sendAuditError(client *ClientSession, requestID string, msg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuditResponse{
			AuditResponse: &pb.AuditQueryResponse{
				RequestId: requestID,
				Success:   false,
				Error:     msg,
			},
		},
	})
}
