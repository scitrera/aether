package aether

import (
	"context"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Role/group authorization admin operations.
//
// These mirror the REST endpoints under /api/acl/groups, /api/acl/roles, and
// /api/acl/principals/{type}/{id}/{groups,roles}. A group is a named collection
// of principals; a role is a named permission bundle (its permissions are
// granted with CreateACLRule using PrincipalType="role", PrincipalID=<role>).
// Membership/assignment edges are resolved transitively and combined additively
// at evaluation time.
// =============================================================================

// CreateGroupOptions configures CreateGroup.
type CreateGroupOptions struct {
	AdminTimeoutOption
	Name        string // Required.
	Description string
	CreatedBy   string
	Metadata    map[string]string
}

// CreateGroup creates a named group.
func (a *AdminClient) CreateGroup(ctx context.Context, opts CreateGroupOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op: pb.ACLOperation_CREATE_GROUP,
		GroupRequest: &pb.ACLGroupRequest{
			Name:        opts.Name,
			Description: opts.Description,
			CreatedBy:   opts.CreatedBy,
			Metadata:    opts.Metadata,
		},
	}, opts.Timeout)
}

// NameOptions configures operations that take a single group/role name.
type NameOptions struct {
	AdminTimeoutOption
	Name string // Required.
}

// GetGroup fetches a group by name.
func (a *AdminClient) GetGroup(ctx context.Context, opts NameOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_GET_GROUP, Name: opts.Name}, opts.Timeout)
}

// DeleteGroup deletes a group by name.
func (a *AdminClient) DeleteGroup(ctx context.Context, opts NameOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_DELETE_GROUP, Name: opts.Name}, opts.Timeout)
}

// ListGroups lists all groups.
func (a *AdminClient) ListGroups(ctx context.Context, opts AdminTimeoutOption) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_LIST_GROUPS}, opts.Timeout)
}

// AddGroupMemberOptions configures AddGroupMember.
type AddGroupMemberOptions struct {
	AdminTimeoutOption
	GroupName  string // Required.
	MemberType string // principal type or "group" (nesting). Required.
	MemberID   string // Required.
	GrantedBy  string
	ExpiresAt  int64 // Unix seconds, 0 = no expiry.
}

// AddGroupMember adds (or refreshes) a member of a group.
func (a *AdminClient) AddGroupMember(ctx context.Context, opts AddGroupMemberOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:   pb.ACLOperation_ADD_GROUP_MEMBER,
		Name: opts.GroupName,
		MemberRequest: &pb.ACLGroupMemberRequest{
			MemberType: opts.MemberType,
			MemberId:   opts.MemberID,
			GrantedBy:  opts.GrantedBy,
			ExpiresAt:  opts.ExpiresAt,
		},
	}, opts.Timeout)
}

// PrincipalEdgeOptions configures member/assignee removal and principal lookups.
type PrincipalEdgeOptions struct {
	AdminTimeoutOption
	Name          string // group/role name (for remove/unassign). Empty for principal lookups.
	PrincipalType string // Required.
	PrincipalID   string // Required.
}

// RemoveGroupMember removes a member from a group.
func (a *AdminClient) RemoveGroupMember(ctx context.Context, opts PrincipalEdgeOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:        pb.ACLOperation_REMOVE_GROUP_MEMBER,
		Name:      opts.Name,
		Principal: &pb.PrincipalRef{PrincipalType: opts.PrincipalType, PrincipalId: opts.PrincipalID},
	}, opts.Timeout)
}

// ListGroupMembers lists the direct members of a group.
func (a *AdminClient) ListGroupMembers(ctx context.Context, opts NameOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_LIST_GROUP_MEMBERS, Name: opts.Name}, opts.Timeout)
}

// CreateRoleOptions configures CreateRole.
type CreateRoleOptions struct {
	AdminTimeoutOption
	Name        string // Required.
	Description string
	CreatedBy   string
	Metadata    map[string]string
}

// CreateRole creates a named role. Grant its permissions with CreateACLRule
// using PrincipalType="role", PrincipalID=<role name>.
func (a *AdminClient) CreateRole(ctx context.Context, opts CreateRoleOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op: pb.ACLOperation_CREATE_ROLE,
		RoleRequest: &pb.ACLRoleRequest{
			Name:        opts.Name,
			Description: opts.Description,
			CreatedBy:   opts.CreatedBy,
			Metadata:    opts.Metadata,
		},
	}, opts.Timeout)
}

// GetRole fetches a role by name.
func (a *AdminClient) GetRole(ctx context.Context, opts NameOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_GET_ROLE, Name: opts.Name}, opts.Timeout)
}

// DeleteRole deletes a role (and its permission rules) by name.
func (a *AdminClient) DeleteRole(ctx context.Context, opts NameOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_DELETE_ROLE, Name: opts.Name}, opts.Timeout)
}

// ListRoles lists all roles.
func (a *AdminClient) ListRoles(ctx context.Context, opts AdminTimeoutOption) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_LIST_ROLES}, opts.Timeout)
}

// AssignRoleOptions configures AssignRole.
type AssignRoleOptions struct {
	AdminTimeoutOption
	RoleName     string // Required.
	AssigneeType string // principal type or "group". Required.
	AssigneeID   string // Required.
	GrantedBy    string
	ExpiresAt    int64 // Unix seconds, 0 = no expiry.
}

// AssignRole assigns (or refreshes) a role to a principal or group.
func (a *AdminClient) AssignRole(ctx context.Context, opts AssignRoleOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:   pb.ACLOperation_ASSIGN_ROLE,
		Name: opts.RoleName,
		AssignmentRequest: &pb.ACLRoleAssignmentRequest{
			AssigneeType: opts.AssigneeType,
			AssigneeId:   opts.AssigneeID,
			GrantedBy:    opts.GrantedBy,
			ExpiresAt:    opts.ExpiresAt,
		},
	}, opts.Timeout)
}

// UnassignRole removes a role assignment.
func (a *AdminClient) UnassignRole(ctx context.Context, opts PrincipalEdgeOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:        pb.ACLOperation_UNASSIGN_ROLE,
		Name:      opts.Name,
		Principal: &pb.PrincipalRef{PrincipalType: opts.PrincipalType, PrincipalId: opts.PrincipalID},
	}, opts.Timeout)
}

// ListRoleAssignments lists the direct assignees of a role.
func (a *AdminClient) ListRoleAssignments(ctx context.Context, opts NameOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_LIST_ROLE_ASSIGNMENTS, Name: opts.Name}, opts.Timeout)
}

// ListPrincipalGroups lists the groups a principal is a direct member of.
func (a *AdminClient) ListPrincipalGroups(ctx context.Context, opts PrincipalEdgeOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:        pb.ACLOperation_LIST_PRINCIPAL_GROUPS,
		Principal: &pb.PrincipalRef{PrincipalType: opts.PrincipalType, PrincipalId: opts.PrincipalID},
	}, opts.Timeout)
}

// ListPrincipalRoles lists the roles directly assigned to a principal.
func (a *AdminClient) ListPrincipalRoles(ctx context.Context, opts PrincipalEdgeOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:        pb.ACLOperation_LIST_PRINCIPAL_ROLES,
		Principal: &pb.PrincipalRef{PrincipalType: opts.PrincipalType, PrincipalId: opts.PrincipalID},
	}, opts.Timeout)
}

// ExplainAccessOptions configures ExplainAccess.
type ExplainAccessOptions struct {
	AdminTimeoutOption
	PrincipalType string // canonical principal_type, e.g. "user". Required.
	PrincipalID   string // canonical principal_id. Required.
	ResourceType  string // Required.
	ResourceID    string // Required.
	RequiredLevel int32  // threshold the decision is compared against (0 = NONE).
}

// ExplainAccess explains how a principal's effective access to a resource is
// decided: the resolved subject set (self + transitive groups/roles), every
// rule that matched, and the resulting decision. It does not gate access, but
// the gateway records an "explain_access" audit event attributing the call to
// the connected principal. Read the result from ACLResponse.Explanation.
func (a *AdminClient) ExplainAccess(ctx context.Context, opts ExplainAccessOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:            pb.ACLOperation_EXPLAIN_ACCESS,
		Principal:     &pb.PrincipalRef{PrincipalType: opts.PrincipalType, PrincipalId: opts.PrincipalID},
		ResourceType:  opts.ResourceType,
		ResourceId:    opts.ResourceID,
		RequiredLevel: opts.RequiredLevel,
	}, opts.Timeout)
}
