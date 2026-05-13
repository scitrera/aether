package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/pkg/models"
)

// =============================================================================
// ACL Management
// =============================================================================

func (p *GatewayStateProvider) ListACLRules(ctx context.Context, filter *admin.ACLRuleFilter) ([]*admin.ACLRuleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	// Convert admin filter to ACL filter
	aclFilter := acl.RuleFilter{}
	if filter != nil {
		aclFilter.PrincipalType = filter.PrincipalType
		aclFilter.PrincipalID = filter.PrincipalID
		aclFilter.ResourceType = filter.ResourceType
		aclFilter.ResourceID = filter.ResourceID
	}

	rules, err := p.aclService.ListRules(ctx, aclFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACL rules: %w", err)
	}

	// Convert ACL rules to admin format
	result := make([]*admin.ACLRuleInfo, 0, len(rules))
	for _, rule := range rules {
		result = append(result, &admin.ACLRuleInfo{
			RuleID:          rule.RuleID,
			PrincipalType:   rule.PrincipalType,
			PrincipalID:     rule.PrincipalID,
			ResourceType:    rule.ResourceType,
			ResourceID:      rule.ResourceID,
			AccessLevel:     rule.AccessLevel,
			AccessLevelName: acl.AccessLevelName(rule.AccessLevel),
			GrantedBy:       rule.GrantedBy,
			GrantedAt:       rule.GrantedAt,
			ExpiresAt:       rule.ExpiresAt,
			Reason:          rule.Reason,
		})
	}

	return result, nil
}

func (p *GatewayStateProvider) GetACLRule(ctx context.Context, principalType, principalID, resourceType, resourceID string) (*admin.ACLRuleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	rule, err := p.aclService.GetRule(ctx, principalType, principalID, resourceType, resourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL rule: %w", err)
	}

	return &admin.ACLRuleInfo{
		RuleID:          rule.RuleID,
		PrincipalType:   rule.PrincipalType,
		PrincipalID:     rule.PrincipalID,
		ResourceType:    rule.ResourceType,
		ResourceID:      rule.ResourceID,
		AccessLevel:     rule.AccessLevel,
		AccessLevelName: acl.AccessLevelName(rule.AccessLevel),
		GrantedBy:       rule.GrantedBy,
		GrantedAt:       rule.GrantedAt,
		ExpiresAt:       rule.ExpiresAt,
		Reason:          rule.Reason,
	}, nil
}

func (p *GatewayStateProvider) GrantACLAccess(ctx context.Context, req *admin.GrantACLAccessRequest) (*admin.ACLRuleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	rule, err := p.aclService.GrantAccess(ctx,
		req.PrincipalType,
		req.PrincipalID,
		req.ResourceType,
		req.ResourceID,
		req.AccessLevel,
		req.GrantedBy,
		req.Reason,
		req.ExpiresAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to grant ACL access: %w", err)
	}

	return &admin.ACLRuleInfo{
		RuleID:          rule.RuleID,
		PrincipalType:   rule.PrincipalType,
		PrincipalID:     rule.PrincipalID,
		ResourceType:    rule.ResourceType,
		ResourceID:      rule.ResourceID,
		AccessLevel:     rule.AccessLevel,
		AccessLevelName: acl.AccessLevelName(rule.AccessLevel),
		GrantedBy:       rule.GrantedBy,
		GrantedAt:       rule.GrantedAt,
		ExpiresAt:       rule.ExpiresAt,
		Reason:          rule.Reason,
	}, nil
}

func (p *GatewayStateProvider) RevokeACLAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}

	return p.aclService.RevokeAccess(ctx, principalType, principalID, resourceType, resourceID)
}

func (p *GatewayStateProvider) ListACLAuthorityGrants(ctx context.Context, filter *admin.ACLAuthorityGrantFilter) ([]*admin.ACLAuthorityGrantInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	grantFilter := acl.AuthorityGrantFilter{}
	if filter != nil {
		grantFilter.RootGrantID = filter.RootGrantID
		grantFilter.SubjectType = normalizeACLPrincipalTypeString(filter.SubjectType)
		grantFilter.SubjectID = filter.SubjectID
		grantFilter.DelegateType = normalizeACLPrincipalTypeString(filter.DelegateType)
		grantFilter.DelegateID = filter.DelegateID
		grantFilter.AudienceType = filter.AudienceType
		grantFilter.AudienceID = filter.AudienceID
		grantFilter.IncludeRevoked = filter.IncludeRevoked
		grantFilter.ActiveOnly = filter.ActiveOnly
		grantFilter.Limit = filter.Limit
		grantFilter.Offset = filter.Offset
	}

	grants, err := p.aclService.ListAuthorityGrants(ctx, grantFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to list authority grants: %w", err)
	}

	result := make([]*admin.ACLAuthorityGrantInfo, 0, len(grants))
	for _, grant := range grants {
		result = append(result, authorityGrantToAdmin(grant))
	}

	return result, nil
}

func (p *GatewayStateProvider) GetACLAuthorityGrant(ctx context.Context, grantID string) (*admin.ACLAuthorityGrantInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	grant, err := p.aclService.GetAuthorityGrant(ctx, grantID)
	if err != nil {
		return nil, fmt.Errorf("failed to get authority grant: %w", err)
	}

	return authorityGrantToAdmin(grant), nil
}

func (p *GatewayStateProvider) CreateACLAuthorityGrant(ctx context.Context, req *admin.CreateACLAuthorityGrantRequest) (*admin.ACLAuthorityGrantInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	if req == nil {
		return nil, fmt.Errorf("authority grant request is required")
	}

	subject, err := adminPrincipalRefToIdentity(req.Subject)
	if err != nil {
		return nil, fmt.Errorf("invalid subject: %w", err)
	}
	delegate, err := adminPrincipalRefToIdentity(req.Delegate)
	if err != nil {
		return nil, fmt.Errorf("invalid delegate: %w", err)
	}
	issuedBy, err := adminPrincipalRefToIdentity(req.IssuedBy)
	if err != nil {
		return nil, fmt.Errorf("invalid issued_by: %w", err)
	}

	createReq := acl.CreateAuthorityGrantRequest{
		Subject:                  subject,
		Delegate:                 delegate,
		IssuedBy:                 issuedBy,
		ParentGrantID:            req.ParentGrantID,
		MayDelegate:              req.MayDelegate,
		RemainingHops:            req.RemainingHops,
		WorkspaceScope:           append([]string(nil), req.WorkspaceScope...),
		ResourceScope:            adminResourceScopeToACL(req.ResourceScope),
		OperationScope:           append([]string(nil), req.OperationScope...),
		MaxAccessLevel:           req.MaxAccessLevel,
		AudienceType:             req.AudienceType,
		AudienceID:               req.AudienceID,
		ValidWhileAudienceActive: req.ValidWhileAudienceActive,
		ExpiresAt:                req.ExpiresAt,
		RenewableUntil:           req.RenewableUntil,
		Reason:                   req.Reason,
		Metadata:                 req.Metadata,
	}
	if req.RootSubject != nil {
		rootSubject, err := adminPrincipalRefToIdentity(req.RootSubject)
		if err != nil {
			return nil, fmt.Errorf("invalid root_subject: %w", err)
		}
		createReq.RootSubject = &rootSubject
	}

	grant, err := p.aclService.CreateAuthorityGrant(ctx, createReq)
	if err != nil {
		return nil, fmt.Errorf("failed to create authority grant: %w", err)
	}

	return authorityGrantToAdmin(grant), nil
}

func (p *GatewayStateProvider) RenewACLAuthorityGrant(ctx context.Context, req *admin.RenewACLAuthorityGrantRequest) (*admin.ACLAuthorityGrantInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	if req == nil {
		return nil, fmt.Errorf("authority grant renewal request is required")
	}

	grant, err := p.aclService.RenewAuthorityGrant(ctx, req.GrantID, req.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to renew authority grant: %w", err)
	}

	return authorityGrantToAdmin(grant), nil
}

func (p *GatewayStateProvider) RevokeACLAuthorityGrant(ctx context.Context, grantID string) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}

	return p.aclService.RevokeAuthorityGrant(ctx, grantID)
}

func (p *GatewayStateProvider) SetACLFallbackPolicy(ctx context.Context, req *admin.SetFallbackPolicyRequest) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}

	return p.aclService.SetFallbackPolicy(ctx, req.RuleCategory, req.FallbackAccessLevel, req.UpdatedBy)
}

func (p *GatewayStateProvider) GetACLFallbackPolicy(ctx context.Context, ruleCategory string) (*admin.ACLFallbackPolicyInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	policy, err := p.aclService.GetFallbackPolicy(ctx, ruleCategory)
	if err != nil {
		return nil, fmt.Errorf("failed to get fallback policy: %w", err)
	}

	return &admin.ACLFallbackPolicyInfo{
		PolicyID:            policy.PolicyID,
		RuleCategory:        policy.RuleCategory,
		FallbackAccessLevel: policy.FallbackAccessLevel,
		AccessLevelName:     acl.AccessLevelName(policy.FallbackAccessLevel),
		UpdatedBy:           policy.UpdatedBy,
		UpdatedAt:           policy.UpdatedAt,
	}, nil
}

func (p *GatewayStateProvider) QueryACLAuditLog(ctx context.Context, filter *admin.ACLAuditLogFilter) ([]*admin.ACLAuditLogEntryInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	// Convert admin filter to ACL filter
	aclFilter := acl.AuditLogFilter{}
	if filter != nil {
		aclFilter.StartTime = filter.StartTime
		aclFilter.EndTime = filter.EndTime
		aclFilter.PrincipalType = filter.PrincipalType
		aclFilter.PrincipalID = filter.PrincipalID
		aclFilter.ResourceType = filter.ResourceType
		aclFilter.ResourceID = filter.ResourceID
		aclFilter.Decision = filter.Decision
		aclFilter.Workspace = filter.Workspace
		aclFilter.Limit = filter.Limit
	}

	entries, err := p.aclService.QueryAuditLog(ctx, aclFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit log: %w", err)
	}

	// Convert ACL entries to admin format
	result := make([]*admin.ACLAuditLogEntryInfo, 0, len(entries))
	for _, entry := range entries {
		sessionIDStr := entry.SessionID.String()
		result = append(result, &admin.ACLAuditLogEntryInfo{
			AuditID:         entry.AuditID,
			Timestamp:       entry.Timestamp,
			Decision:        entry.Decision,
			AccessLevel:     entry.AccessLevel,
			AccessLevelName: acl.AccessLevelName(entry.AccessLevel),
			PrincipalType:   entry.PrincipalType,
			PrincipalID:     entry.PrincipalID,
			ResourceType:    entry.ResourceType,
			ResourceID:      entry.ResourceID,
			Operation:       entry.Operation,
			Workspace:       entry.Workspace,
			RuleID:          entry.RuleID,
			FallbackApplied: entry.FallbackApplied,
			GatewayID:       entry.GatewayID,
			SessionID:       sessionIDStr,
			Metadata:        entry.Metadata,
		})
	}

	return result, nil
}

func (p *GatewayStateProvider) CleanupExpiredACLRules(ctx context.Context) (int64, error) {
	if p.aclService == nil {
		return 0, fmt.Errorf("ACL service not available")
	}

	return p.aclService.CleanupExpiredRules(ctx)
}

func (p *GatewayStateProvider) CleanupOldACLAuditLogs(ctx context.Context, retentionDays int) (int64, error) {
	if p.aclService == nil {
		return 0, fmt.Errorf("ACL service not available")
	}

	return p.aclService.CleanupOldAuditLogs(ctx, retentionDays)
}

func authorityGrantToAdmin(grant *acl.AuthorityGrant) *admin.ACLAuthorityGrantInfo {
	if grant == nil {
		return nil
	}

	resourceScope := make([]*admin.ACLAuthorityGrantResourceScope, 0, len(grant.ResourceScope))
	for resourceType, patterns := range grant.ResourceScope {
		resourceScope = append(resourceScope, &admin.ACLAuthorityGrantResourceScope{
			ResourceType: resourceType,
			Patterns:     append([]string(nil), patterns...),
		})
	}

	return &admin.ACLAuthorityGrantInfo{
		GrantID:                  grant.GrantID,
		RootGrantID:              grant.RootGrantID,
		Subject:                  &admin.PrincipalRef{PrincipalType: grant.SubjectType, PrincipalID: grant.SubjectID},
		Delegate:                 &admin.PrincipalRef{PrincipalType: grant.DelegateType, PrincipalID: grant.DelegateID},
		IssuedBy:                 &admin.PrincipalRef{PrincipalType: grant.IssuedByType, PrincipalID: grant.IssuedByID},
		RootSubject:              &admin.PrincipalRef{PrincipalType: grant.RootSubjectType, PrincipalID: grant.RootSubjectID},
		ParentGrantID:            grant.ParentGrantID,
		MayDelegate:              grant.MayDelegate,
		RemainingHops:            grant.RemainingHops,
		WorkspaceScope:           append([]string(nil), grant.WorkspaceScope...),
		ResourceScope:            resourceScope,
		OperationScope:           append([]string(nil), grant.OperationScope...),
		MaxAccessLevel:           grant.MaxAccessLevel,
		AccessLevelName:          acl.AccessLevelName(grant.MaxAccessLevel),
		AudienceType:             grant.AudienceType,
		AudienceID:               grant.AudienceID,
		ValidWhileAudienceActive: grant.ValidWhileAudienceActive,
		ExpiresAt:                grant.ExpiresAt,
		RenewableUntil:           grant.RenewableUntil,
		RenewedAt:                grant.RenewedAt,
		Revoked:                  grant.Revoked,
		RevokedAt:                grant.RevokedAt,
		Reason:                   grant.Reason,
		Metadata:                 grant.Metadata,
		CreatedAt:                grant.CreatedAt,
	}
}

func normalizeACLPrincipalTypeString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	if pt, err := parsePrincipalTypeString(trimmed); err == nil {
		return acl.PrincipalTypeForModel(pt)
	}

	return strings.ToLower(trimmed)
}

func adminPrincipalRefToIdentity(ref *admin.PrincipalRef) (models.Identity, error) {
	if ref == nil {
		return models.Identity{}, fmt.Errorf("principal reference is required")
	}

	pt, err := parsePrincipalTypeString(ref.PrincipalType)
	if err != nil {
		return models.Identity{}, err
	}

	identity := models.Identity{
		Type: pt,
		ID:   ref.PrincipalID,
	}

	switch pt {
	case models.PrincipalAgent, models.PrincipalTask, models.PrincipalBridge, models.PrincipalService:
		if parsed, err := models.ParseIdentity(ref.PrincipalID); err == nil && parsed.Type == pt {
			return parsed, nil
		}
	}

	return identity, nil
}

func adminResourceScopeToACL(entries []*admin.ACLAuthorityGrantResourceScope) map[string][]string {
	if len(entries) == 0 {
		return nil
	}

	result := make(map[string][]string, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.ResourceType == "" {
			continue
		}
		result[entry.ResourceType] = append([]string(nil), entry.Patterns...)
	}
	if len(result) == 0 {
		return nil
	}

	return result
}
