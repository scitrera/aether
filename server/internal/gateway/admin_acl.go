package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
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

func (p *GatewayStateProvider) GetACLRuleByID(ctx context.Context, ruleID string) (*admin.ACLRuleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}

	rule, err := p.aclService.GetRuleByID(ctx, ruleID)
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL rule by id: %w", err)
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

// =============================================================================
// ACL Groups & Roles
// =============================================================================

func (p *GatewayStateProvider) ListACLGroups(ctx context.Context) ([]*admin.ACLGroupInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	groups, err := p.aclService.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	result := make([]*admin.ACLGroupInfo, 0, len(groups))
	for _, g := range groups {
		result = append(result, groupToAdmin(g))
	}
	return result, nil
}

func (p *GatewayStateProvider) CreateACLGroup(ctx context.Context, req *admin.CreateACLGroupRequest) (*admin.ACLGroupInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	g, err := p.aclService.CreateGroup(ctx, req.Name, req.Description, req.CreatedBy, req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create group: %w", err)
	}
	return groupToAdmin(g), nil
}

func (p *GatewayStateProvider) GetACLGroup(ctx context.Context, name string) (*admin.ACLGroupInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	g, err := p.aclService.GetGroup(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get group: %w", err)
	}
	return groupToAdmin(g), nil
}

func (p *GatewayStateProvider) DeleteACLGroup(ctx context.Context, name string) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}
	return p.aclService.DeleteGroup(ctx, name)
}

func (p *GatewayStateProvider) ListACLGroupMembers(ctx context.Context, groupName string) ([]*admin.ACLGroupMemberInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	members, err := p.aclService.ListGroupMembers(ctx, groupName)
	if err != nil {
		return nil, fmt.Errorf("failed to list group members: %w", err)
	}
	result := make([]*admin.ACLGroupMemberInfo, 0, len(members))
	for _, m := range members {
		result = append(result, groupMemberToAdmin(m))
	}
	return result, nil
}

func (p *GatewayStateProvider) AddACLGroupMember(ctx context.Context, groupName string, req *admin.AddACLGroupMemberRequest) (*admin.ACLGroupMemberInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	m, err := p.aclService.AddGroupMember(ctx, groupName, req.MemberType, req.MemberID, req.GrantedBy, req.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to add group member: %w", err)
	}
	return groupMemberToAdmin(m), nil
}

func (p *GatewayStateProvider) RemoveACLGroupMember(ctx context.Context, groupName, memberType, memberID string) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}
	return p.aclService.RemoveGroupMember(ctx, groupName, memberType, memberID)
}

func (p *GatewayStateProvider) ListACLRoles(ctx context.Context) ([]*admin.ACLRoleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	roles, err := p.aclService.ListRoles(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list roles: %w", err)
	}
	result := make([]*admin.ACLRoleInfo, 0, len(roles))
	for _, r := range roles {
		result = append(result, roleToAdmin(r))
	}
	return result, nil
}

func (p *GatewayStateProvider) CreateACLRole(ctx context.Context, req *admin.CreateACLRoleRequest) (*admin.ACLRoleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	r, err := p.aclService.CreateRole(ctx, req.Name, req.Description, req.CreatedBy, req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create role: %w", err)
	}
	return roleToAdmin(r), nil
}

func (p *GatewayStateProvider) GetACLRole(ctx context.Context, name string) (*admin.ACLRoleInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	r, err := p.aclService.GetRole(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get role: %w", err)
	}
	return roleToAdmin(r), nil
}

func (p *GatewayStateProvider) DeleteACLRole(ctx context.Context, name string) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}
	return p.aclService.DeleteRole(ctx, name)
}

func (p *GatewayStateProvider) ListACLRoleAssignments(ctx context.Context, roleName string) ([]*admin.ACLRoleAssignmentInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	assignments, err := p.aclService.ListRoleAssignments(ctx, roleName)
	if err != nil {
		return nil, fmt.Errorf("failed to list role assignments: %w", err)
	}
	result := make([]*admin.ACLRoleAssignmentInfo, 0, len(assignments))
	for _, a := range assignments {
		result = append(result, roleAssignmentToAdmin(a))
	}
	return result, nil
}

func (p *GatewayStateProvider) AssignACLRole(ctx context.Context, roleName string, req *admin.AssignACLRoleRequest) (*admin.ACLRoleAssignmentInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	a, err := p.aclService.AssignRole(ctx, roleName, req.AssigneeType, req.AssigneeID, req.GrantedBy, req.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to assign role: %w", err)
	}
	return roleAssignmentToAdmin(a), nil
}

func (p *GatewayStateProvider) UnassignACLRole(ctx context.Context, roleName, assigneeType, assigneeID string) error {
	if p.aclService == nil {
		return fmt.Errorf("ACL service not available")
	}
	return p.aclService.UnassignRole(ctx, roleName, assigneeType, assigneeID)
}

func (p *GatewayStateProvider) ListACLPrincipalGroups(ctx context.Context, memberType, memberID string) ([]*admin.ACLGroupMemberInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	members, err := p.aclService.ListPrincipalGroups(ctx, memberType, memberID)
	if err != nil {
		return nil, fmt.Errorf("failed to list principal groups: %w", err)
	}
	result := make([]*admin.ACLGroupMemberInfo, 0, len(members))
	for _, m := range members {
		result = append(result, groupMemberToAdmin(m))
	}
	return result, nil
}

func (p *GatewayStateProvider) ListACLPrincipalRoles(ctx context.Context, assigneeType, assigneeID string) ([]*admin.ACLRoleAssignmentInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	assignments, err := p.aclService.ListPrincipalRoles(ctx, assigneeType, assigneeID)
	if err != nil {
		return nil, fmt.Errorf("failed to list principal roles: %w", err)
	}
	result := make([]*admin.ACLRoleAssignmentInfo, 0, len(assignments))
	for _, a := range assignments {
		result = append(result, roleAssignmentToAdmin(a))
	}
	return result, nil
}

func (p *GatewayStateProvider) ExplainACLAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, requiredLevel int, callerType, callerID string) (*admin.ACLAccessExplanationInfo, error) {
	if p.aclService == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	exp, err := p.aclService.ExplainAccess(ctx, principalType, principalID, resourceType, resourceID, requiredLevel, callerType, callerID)
	if err != nil {
		return nil, fmt.Errorf("failed to explain access: %w", err)
	}
	out := &admin.ACLAccessExplanationInfo{
		Principal: exp.Principal,
		Subjects:  exp.Subjects,
	}
	for _, c := range exp.Contributions {
		out.Contributions = append(out.Contributions, &admin.ACLAccessContributionInfo{
			Subject:     c.Subject,
			RuleID:      c.RuleID,
			AccessLevel: c.AccessLevel,
			Resource:    c.Resource,
			Expired:     c.Expired,
		})
	}
	if exp.Decision != nil {
		out.Allowed = exp.Decision.Allowed
		out.Decision = exp.Decision.Decision
		out.EffectiveLevel = exp.Decision.EffectiveAccessLevel
		out.FallbackApplied = exp.Decision.FallbackApplied
		out.Reason = exp.Decision.Reason
	}
	return out, nil
}

// groupToAdmin converts a store Group to an admin DTO.
func groupToAdmin(g *aclstore.Group) *admin.ACLGroupInfo {
	if g == nil {
		return nil
	}
	return &admin.ACLGroupInfo{
		GroupID:     g.GroupID,
		GroupName:   g.GroupName,
		Description: g.Description,
		CreatedBy:   g.CreatedBy,
		CreatedAt:   g.CreatedAt,
		Metadata:    g.Metadata,
	}
}

// roleToAdmin converts a store Role to an admin DTO.
func roleToAdmin(r *aclstore.Role) *admin.ACLRoleInfo {
	if r == nil {
		return nil
	}
	return &admin.ACLRoleInfo{
		RoleID:      r.RoleID,
		RoleName:    r.RoleName,
		Description: r.Description,
		CreatedBy:   r.CreatedBy,
		CreatedAt:   r.CreatedAt,
		Metadata:    r.Metadata,
	}
}

// groupMemberToAdmin converts a store GroupMember to an admin DTO.
func groupMemberToAdmin(m *aclstore.GroupMember) *admin.ACLGroupMemberInfo {
	if m == nil {
		return nil
	}
	return &admin.ACLGroupMemberInfo{
		GroupName:  m.GroupName,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
		GrantedBy:  m.GrantedBy,
		GrantedAt:  m.GrantedAt,
		ExpiresAt:  m.ExpiresAt,
	}
}

// roleAssignmentToAdmin converts a store RoleAssignment to an admin DTO.
func roleAssignmentToAdmin(a *aclstore.RoleAssignment) *admin.ACLRoleAssignmentInfo {
	if a == nil {
		return nil
	}
	return &admin.ACLRoleAssignmentInfo{
		RoleName:     a.RoleName,
		AssigneeType: a.AssigneeType,
		AssigneeID:   a.AssigneeID,
		GrantedBy:    a.GrantedBy,
		GrantedAt:    a.GrantedAt,
		ExpiresAt:    a.ExpiresAt,
	}
}

// isACLGroupNotFound reports whether err wraps aclstore.ErrGroupNotFound.
func isACLGroupNotFound(err error) bool { return errors.Is(err, aclstore.ErrGroupNotFound) }

// isACLRoleNotFound reports whether err wraps aclstore.ErrRoleNotFound.
func isACLRoleNotFound(err error) bool { return errors.Is(err, aclstore.ErrRoleNotFound) }

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
