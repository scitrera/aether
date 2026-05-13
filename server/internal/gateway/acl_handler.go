package gateway

import (
	"context"
	"fmt"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
)

// handleACLOp processes an ACLOperation from a connected client.
func (s *GatewayServer) handleACLOp(ctx context.Context, client *ClientSession, op *pb.ACLOperation) {
	if s.adminProvider == nil {
		sendACLError(client, "admin provider not configured")
		return
	}

	switch op.Op {
	case pb.ACLOperation_LIST_RULES:
		filter := &admin.ACLRuleFilter{}
		if op.RuleFilter != nil {
			filter.PrincipalType = op.RuleFilter.PrincipalType
			filter.PrincipalID = op.RuleFilter.PrincipalId
			filter.ResourceType = op.RuleFilter.ResourceType
			filter.ResourceID = op.RuleFilter.ResourceId
		}
		rules, err := s.adminProvider.ListACLRules(ctx, filter)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleACLOp: list rules failed")
			sendACLError(client, err.Error())
			return
		}
		protoRules := make([]*pb.ACLRuleInfo, 0, len(rules))
		for _, r := range rules {
			protoRules = append(protoRules, adminACLRuleToProto(r))
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success:    true,
					Rules:      protoRules,
					TotalRules: int32(len(protoRules)),
				},
			},
		})

	case pb.ACLOperation_GET_RULE:
		// GET_RULE uses rule_id to find the rule but StateProvider.GetACLRule uses principal+resource
		// Since the interface takes principal/resource params, we use rule_filter fields as fallback
		var principalType, principalID, resourceType, resourceID string
		if op.RuleFilter != nil {
			principalType = op.RuleFilter.PrincipalType
			principalID = op.RuleFilter.PrincipalId
			resourceType = op.RuleFilter.ResourceType
			resourceID = op.RuleFilter.ResourceId
		}
		rule, err := s.adminProvider.GetACLRule(ctx, principalType, principalID, resourceType, resourceID)
		if err != nil {
			logging.Logger.Error().Err(err).Str("rule_id", op.RuleId).Msg("handleACLOp: get rule failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success: true,
					Rule:    adminACLRuleToProto(rule),
				},
			},
		})

	case pb.ACLOperation_GRANT:
		if op.GrantRequest == nil {
			sendACLError(client, "grant_request required for GRANT")
			return
		}
		req := protoGrantRequestToAdmin(op.GrantRequest)
		rule, err := s.adminProvider.GrantACLAccess(ctx, req)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleACLOp: grant access failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success: true,
					Message: "ACL access granted",
					Rule:    adminACLRuleToProto(rule),
				},
			},
		})

	case pb.ACLOperation_REVOKE:
		var principalType, principalID, resourceType, resourceID string
		if op.RuleFilter != nil {
			principalType = op.RuleFilter.PrincipalType
			principalID = op.RuleFilter.PrincipalId
			resourceType = op.RuleFilter.ResourceType
			resourceID = op.RuleFilter.ResourceId
		}
		if err := s.adminProvider.RevokeACLAccess(ctx, principalType, principalID, resourceType, resourceID); err != nil {
			logging.Logger.Error().Err(err).Str("rule_id", op.RuleId).Msg("handleACLOp: revoke access failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success: true,
					Message: "ACL access revoked",
				},
			},
		})

	case pb.ACLOperation_QUERY_AUDIT:
		filter := &admin.ACLAuditLogFilter{}
		if op.AuditFilter != nil {
			if op.AuditFilter.StartTime != 0 {
				t := time.Unix(op.AuditFilter.StartTime, 0)
				filter.StartTime = &t
			}
			if op.AuditFilter.EndTime != 0 {
				t := time.Unix(op.AuditFilter.EndTime, 0)
				filter.EndTime = &t
			}
			filter.PrincipalType = op.AuditFilter.PrincipalType
			filter.PrincipalID = op.AuditFilter.PrincipalId
			filter.ResourceType = op.AuditFilter.ResourceType
			filter.ResourceID = op.AuditFilter.ResourceId
			filter.Decision = op.AuditFilter.Decision
			filter.Workspace = op.AuditFilter.Workspace
			filter.Limit = int(op.AuditFilter.Limit)
		}
		entries, err := s.adminProvider.QueryACLAuditLog(ctx, filter)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleACLOp: query audit log failed")
			sendACLError(client, err.Error())
			return
		}
		protoEntries := make([]*pb.ACLAuditEntryInfo, 0, len(entries))
		for _, e := range entries {
			protoEntries = append(protoEntries, adminACLAuditEntryToProto(e))
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success:           true,
					AuditEntries:      protoEntries,
					TotalAuditEntries: int32(len(protoEntries)),
				},
			},
		})

	case pb.ACLOperation_GET_FALLBACK_POLICY:
		policy, err := s.adminProvider.GetACLFallbackPolicy(ctx, op.RuleCategory)
		if err != nil {
			logging.Logger.Error().Err(err).Str("rule_category", op.RuleCategory).Msg("handleACLOp: get fallback policy failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success:        true,
					FallbackPolicy: adminFallbackPolicyToProto(policy),
				},
			},
		})

	case pb.ACLOperation_SET_FALLBACK_POLICY:
		if op.FallbackRequest == nil {
			sendACLError(client, "fallback_request required for SET_FALLBACK_POLICY")
			return
		}
		req := &admin.SetFallbackPolicyRequest{
			RuleCategory:        op.FallbackRequest.RuleCategory,
			FallbackAccessLevel: int(op.FallbackRequest.FallbackAccessLevel),
			UpdatedBy:           op.FallbackRequest.UpdatedBy,
		}
		if err := s.adminProvider.SetACLFallbackPolicy(ctx, req); err != nil {
			logging.Logger.Error().Err(err).Str("rule_category", op.FallbackRequest.RuleCategory).Msg("handleACLOp: set fallback policy failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success: true,
					Message: "fallback policy updated successfully",
				},
			},
		})

	case pb.ACLOperation_CLEANUP_EXPIRED:
		count, err := s.adminProvider.CleanupExpiredACLRules(ctx)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleACLOp: cleanup expired rules failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success: true,
					Message: "expired ACL rules cleaned up",
					CleanupResult: &pb.ACLCleanupResult{
						DeletedCount: count,
						Message:      "expired ACL rules cleaned up",
					},
				},
			},
		})

	case pb.ACLOperation_CLEANUP_AUDIT_LOGS:
		retentionDays := int(op.RetentionDays)
		if retentionDays <= 0 {
			retentionDays = 90
		}
		count, err := s.adminProvider.CleanupOldACLAuditLogs(ctx, retentionDays)
		if err != nil {
			logging.Logger.Error().Err(err).Int("retention_days", retentionDays).Msg("handleACLOp: cleanup audit logs failed")
			sendACLError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Acl{
				Acl: &pb.ACLResponse{
					Success: true,
					Message: "audit logs cleaned up",
					CleanupResult: &pb.ACLCleanupResult{
						DeletedCount: count,
						Message:      "audit logs cleaned up",
					},
				},
			},
		})

	default:
		sendACLError(client, "unknown ACL operation")
	}
}

func sendACLError(client *ClientSession, msg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Acl{
			Acl: &pb.ACLResponse{
				Success: false,
				Error:   msg,
			},
		},
	})
}

func adminACLRuleToProto(r *admin.ACLRuleInfo) *pb.ACLRuleInfo {
	var grantedAt, expiresAt int64
	if !r.GrantedAt.IsZero() {
		grantedAt = r.GrantedAt.Unix()
	}
	if r.ExpiresAt != nil && !r.ExpiresAt.IsZero() {
		expiresAt = r.ExpiresAt.Unix()
	}
	return &pb.ACLRuleInfo{
		RuleId:          r.RuleID,
		PrincipalType:   r.PrincipalType,
		PrincipalId:     r.PrincipalID,
		ResourceType:    r.ResourceType,
		ResourceId:      r.ResourceID,
		AccessLevel:     int32(r.AccessLevel),
		AccessLevelName: r.AccessLevelName,
		GrantedBy:       r.GrantedBy,
		GrantedAt:       grantedAt,
		ExpiresAt:       expiresAt,
		Reason:          r.Reason,
	}
}

func protoGrantRequestToAdmin(g *pb.ACLGrantRequest) *admin.GrantACLAccessRequest {
	req := &admin.GrantACLAccessRequest{
		PrincipalType: g.PrincipalType,
		PrincipalID:   g.PrincipalId,
		ResourceType:  g.ResourceType,
		ResourceID:    g.ResourceId,
		AccessLevel:   int(g.AccessLevel),
		GrantedBy:     g.GrantedBy,
		Reason:        g.Reason,
	}
	if g.ExpiresAt != 0 {
		t := time.Unix(g.ExpiresAt, 0)
		req.ExpiresAt = &t
	}
	return req
}

func adminACLAuditEntryToProto(e *admin.ACLAuditLogEntryInfo) *pb.ACLAuditEntryInfo {
	entry := &pb.ACLAuditEntryInfo{
		AuditId:         e.AuditID,
		Timestamp:       e.Timestamp.Unix(),
		Decision:        e.Decision,
		AccessLevel:     int32(e.AccessLevel),
		AccessLevelName: e.AccessLevelName,
		PrincipalType:   e.PrincipalType,
		PrincipalId:     e.PrincipalID,
		ResourceType:    e.ResourceType,
		ResourceId:      e.ResourceID,
		Operation:       e.Operation,
		Workspace:       e.Workspace,
		FallbackApplied: e.FallbackApplied,
		GatewayId:       e.GatewayID,
		SessionId:       e.SessionID,
	}
	if e.RuleID != nil {
		entry.RuleId = *e.RuleID
	}
	meta := make(map[string]string)
	for k, v := range e.Metadata {
		if s, ok := v.(string); ok {
			meta[k] = s
		}
	}
	entry.Metadata = meta
	return entry
}

func adminFallbackPolicyToProto(p *admin.ACLFallbackPolicyInfo) *pb.ACLFallbackPolicyInfo {
	return &pb.ACLFallbackPolicyInfo{
		PolicyId:                p.PolicyID,
		RuleCategory:            p.RuleCategory,
		FallbackAccessLevel:     int32(p.FallbackAccessLevel),
		FallbackAccessLevelName: p.AccessLevelName,
		UpdatedBy:               p.UpdatedBy,
		UpdatedAt:               p.UpdatedAt.Unix(),
	}
}

func metadataToProtoStringMap(metadata map[string]interface{}) map[string]string {
	if len(metadata) == 0 {
		return nil
	}

	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		result[key] = fmt.Sprint(value)
	}

	return result
}

func protoStringMapToMetadata(values map[string]string) map[string]interface{} {
	if len(values) == 0 {
		return nil
	}

	result := make(map[string]interface{}, len(values))
	for key, value := range values {
		result[key] = value
	}

	return result
}
