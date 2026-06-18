package acl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/logging"
)

// Sentinel errors for the role/group surface.
var (
	ErrGroupNotFound      = errors.New("group not found")
	ErrRoleNotFound       = errors.New("role not found")
	ErrGroupExists        = errors.New("group already exists")
	ErrRoleExists         = errors.New("role already exists")
	ErrMembershipNotFound = errors.New("membership not found")
	ErrAssignmentNotFound = errors.New("role assignment not found")
	// ErrMembershipCycle is returned when an edge would introduce a cycle in
	// the group/role membership DAG (e.g. group A in B in A).
	ErrMembershipCycle = errors.New("membership would create a cycle")
)

// Group is a named collection of principals. Permissions granted to a group
// (acl_rules rows with principal_type='group') apply to all its members.
type Group struct {
	GroupID     string                 `json:"group_id"`
	GroupName   string                 `json:"group_name"`
	Description string                 `json:"description,omitempty"`
	CreatedBy   string                 `json:"created_by,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// Role is a named bundle of permissions (acl_rules rows with
// principal_type='role') assignable to principals or groups.
type Role struct {
	RoleID      string                 `json:"role_id"`
	RoleName    string                 `json:"role_name"`
	Description string                 `json:"description,omitempty"`
	CreatedBy   string                 `json:"created_by,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// GroupMember is a single membership edge: a principal (or nested group)
// belongs to a group.
type GroupMember struct {
	GroupName  string     `json:"group_name"`
	MemberType string     `json:"member_type"`
	MemberID   string     `json:"member_id"`
	GrantedBy  string     `json:"granted_by,omitempty"`
	GrantedAt  time.Time  `json:"granted_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

// RoleAssignment is a single assignment edge: a principal (or group) is granted
// a role.
type RoleAssignment struct {
	RoleName     string     `json:"role_name"`
	AssigneeType string     `json:"assignee_type"`
	AssigneeID   string     `json:"assignee_id"`
	GrantedBy    string     `json:"granted_by,omitempty"`
	GrantedAt    time.Time  `json:"granted_at"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

// AccessExplanation describes how a principal's effective access to a resource
// was (or would be) decided. It surfaces what CheckAccess cannot: the full
// resolved subject set (self + transitive groups/roles), every rule that
// matched across those subjects (Contributions — the "why"), and the winning
// decision (nil when no rule matched and the fallback would apply).
type AccessExplanation struct {
	Principal     string               `json:"principal"`
	Subjects      []string             `json:"subjects"`
	Contributions []AccessContribution `json:"contributions,omitempty"`
	Decision      *ACLDecision         `json:"decision,omitempty"`
}

// AccessContribution is a single rule that matched the principal or one of its
// groups/roles for the explained resource.
type AccessContribution struct {
	Subject     string `json:"subject"`      // granting subject, e.g. "role:wsadmin" or "user:alice"
	RuleID      string `json:"rule_id"`      // acl_rules.rule_id
	AccessLevel int    `json:"access_level"` // level the rule confers
	Resource    string `json:"resource"`     // matched object pattern, e.g. "workspace:prod" or "workspace:*"
	Expired     bool   `json:"expired"`      // true if the rule is past its expiry (excluded from the decision)
}

// memberSubject builds the Casbin subject string for a membership/assignment
// edge source. Group members use "group:<name>" when nesting.
func memberSubject(memberType, memberID string) string {
	return memberType + ":" + memberID
}

// =========================================================================
// Group CRUD
// =========================================================================

func (s *Service) CreateGroup(ctx context.Context, name, description, createdBy string, metadata map[string]interface{}) (*Group, error) {
	if name == "" {
		return nil, fmt.Errorf("group name is required")
	}
	g := &Group{
		GroupID:     uuid.New().String(),
		GroupName:   name,
		Description: description,
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
		Metadata:    metadata,
	}
	meta, err := marshalMetadata(metadata)
	if err != nil {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO acl_groups (group_id, group_name, description, created_by, created_at, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, g.GroupID, g.GroupName, nullString(description), nullString(createdBy), g.CreatedAt, meta)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrGroupExists
		}
		return nil, fmt.Errorf("failed to create group: %w", err)
	}
	return g, nil
}

func (s *Service) DeleteGroup(ctx context.Context, name string) error {
	// Remove in-memory grouping edges that target this group, then delete the
	// row. ON DELETE CASCADE removes acl_group_members; the enforcer is
	// refreshed best-effort so derived access stops immediately.
	res, err := s.db.ExecContext(ctx, `DELETE FROM acl_groups WHERE group_name = $1`, name)
	if err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrGroupNotFound
	}
	s.reloadEnforcer("delete group")
	return nil
}

func (s *Service) GetGroup(ctx context.Context, name string) (*Group, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT group_id, group_name, description, created_by, created_at, metadata
		FROM acl_groups WHERE group_name = $1
	`, name)
	return scanGroup(row)
}

func (s *Service) ListGroups(ctx context.Context) ([]*Group, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT group_id, group_name, description, created_by, created_at, metadata
		FROM acl_groups ORDER BY group_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	defer rows.Close()
	var out []*Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// =========================================================================
// Role CRUD
// =========================================================================

func (s *Service) CreateRole(ctx context.Context, name, description, createdBy string, metadata map[string]interface{}) (*Role, error) {
	if name == "" {
		return nil, fmt.Errorf("role name is required")
	}
	r := &Role{
		RoleID:      uuid.New().String(),
		RoleName:    name,
		Description: description,
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
		Metadata:    metadata,
	}
	meta, err := marshalMetadata(metadata)
	if err != nil {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO acl_roles (role_id, role_name, description, created_by, created_at, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, r.RoleID, r.RoleName, nullString(description), nullString(createdBy), r.CreatedAt, meta)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrRoleExists
		}
		return nil, fmt.Errorf("failed to create role: %w", err)
	}
	return r, nil
}

func (s *Service) DeleteRole(ctx context.Context, name string) error {
	// Delete the role definition and its permission rules (acl_rules rows with
	// principal_type='role'). ON DELETE CASCADE clears acl_role_assignments.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `DELETE FROM acl_roles WHERE role_name = $1`, name)
	if err != nil {
		return fmt.Errorf("failed to delete role: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrRoleNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acl_rules WHERE principal_type = $1 AND principal_id = $2`,
		PrincipalTypeRole, name); err != nil {
		return fmt.Errorf("failed to delete role permissions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit role delete: %w", err)
	}
	s.reloadEnforcer("delete role")
	return nil
}

func (s *Service) GetRole(ctx context.Context, name string) (*Role, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT role_id, role_name, description, created_by, created_at, metadata
		FROM acl_roles WHERE role_name = $1
	`, name)
	return scanRole(row)
}

func (s *Service) ListRoles(ctx context.Context) ([]*Role, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT role_id, role_name, description, created_by, created_at, metadata
		FROM acl_roles ORDER BY role_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list roles: %w", err)
	}
	defer rows.Close()
	var out []*Role
	for rows.Next() {
		r, err := scanRole(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// =========================================================================
// Group membership
// =========================================================================

func (s *Service) AddGroupMember(ctx context.Context, groupName, memberType, memberID, grantedBy string, expiresAt *time.Time) (*GroupMember, error) {
	groupID, err := s.groupIDByName(ctx, groupName)
	if err != nil {
		return nil, err
	}
	target := GroupSubjectPrefix + groupName
	source := memberSubject(memberType, memberID)
	if err := s.checkNoCycle(source, target); err != nil {
		return nil, err
	}

	m := &GroupMember{
		GroupName:  groupName,
		MemberType: memberType,
		MemberID:   memberID,
		GrantedBy:  grantedBy,
		GrantedAt:  time.Now(),
		ExpiresAt:  expiresAt,
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO acl_group_members (id, group_id, member_type, member_id, granted_by, granted_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (group_id, member_type, member_id)
		DO UPDATE SET granted_by = EXCLUDED.granted_by, granted_at = EXCLUDED.granted_at, expires_at = EXCLUDED.expires_at
	`, uuid.New().String(), groupID, memberType, memberID, nullString(grantedBy), m.GrantedAt, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to add group member: %w", err)
	}
	s.addGroupingEdge(source, target)
	return m, nil
}

func (s *Service) RemoveGroupMember(ctx context.Context, groupName, memberType, memberID string) error {
	groupID, err := s.groupIDByName(ctx, groupName)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM acl_group_members WHERE group_id = $1 AND member_type = $2 AND member_id = $3
	`, groupID, memberType, memberID)
	if err != nil {
		return fmt.Errorf("failed to remove group member: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrMembershipNotFound
	}
	s.removeGroupingEdge(memberSubject(memberType, memberID), GroupSubjectPrefix+groupName)
	return nil
}

func (s *Service) ListGroupMembers(ctx context.Context, groupName string) ([]*GroupMember, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.member_type, m.member_id, m.granted_by, m.granted_at, m.expires_at
		FROM acl_group_members m JOIN acl_groups g ON g.group_id = m.group_id
		WHERE g.group_name = $1 ORDER BY m.member_type, m.member_id
	`, groupName)
	if err != nil {
		return nil, fmt.Errorf("failed to list group members: %w", err)
	}
	defer rows.Close()
	var out []*GroupMember
	for rows.Next() {
		m := &GroupMember{GroupName: groupName}
		var grantedBy sql.NullString
		var expiresAt sql.NullTime
		if err := rows.Scan(&m.MemberType, &m.MemberID, &grantedBy, &m.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		m.GrantedBy = grantedBy.String
		if expiresAt.Valid {
			m.ExpiresAt = &expiresAt.Time
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Service) ListPrincipalGroups(ctx context.Context, memberType, memberID string) ([]*GroupMember, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.group_name, m.granted_by, m.granted_at, m.expires_at
		FROM acl_group_members m JOIN acl_groups g ON g.group_id = m.group_id
		WHERE m.member_type = $1 AND m.member_id = $2 ORDER BY g.group_name
	`, memberType, memberID)
	if err != nil {
		return nil, fmt.Errorf("failed to list principal groups: %w", err)
	}
	defer rows.Close()
	var out []*GroupMember
	for rows.Next() {
		m := &GroupMember{MemberType: memberType, MemberID: memberID}
		var grantedBy sql.NullString
		var expiresAt sql.NullTime
		if err := rows.Scan(&m.GroupName, &grantedBy, &m.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		m.GrantedBy = grantedBy.String
		if expiresAt.Valid {
			m.ExpiresAt = &expiresAt.Time
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// =========================================================================
// Role assignment
// =========================================================================

func (s *Service) AssignRole(ctx context.Context, roleName, assigneeType, assigneeID, grantedBy string, expiresAt *time.Time) (*RoleAssignment, error) {
	roleID, err := s.roleIDByName(ctx, roleName)
	if err != nil {
		return nil, err
	}
	target := RoleSubjectPrefix + roleName
	source := memberSubject(assigneeType, assigneeID)
	if err := s.checkNoCycle(source, target); err != nil {
		return nil, err
	}

	a := &RoleAssignment{
		RoleName:     roleName,
		AssigneeType: assigneeType,
		AssigneeID:   assigneeID,
		GrantedBy:    grantedBy,
		GrantedAt:    time.Now(),
		ExpiresAt:    expiresAt,
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO acl_role_assignments (id, role_id, assignee_type, assignee_id, granted_by, granted_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (role_id, assignee_type, assignee_id)
		DO UPDATE SET granted_by = EXCLUDED.granted_by, granted_at = EXCLUDED.granted_at, expires_at = EXCLUDED.expires_at
	`, uuid.New().String(), roleID, assigneeType, assigneeID, nullString(grantedBy), a.GrantedAt, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to assign role: %w", err)
	}
	s.addGroupingEdge(source, target)
	return a, nil
}

func (s *Service) UnassignRole(ctx context.Context, roleName, assigneeType, assigneeID string) error {
	roleID, err := s.roleIDByName(ctx, roleName)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM acl_role_assignments WHERE role_id = $1 AND assignee_type = $2 AND assignee_id = $3
	`, roleID, assigneeType, assigneeID)
	if err != nil {
		return fmt.Errorf("failed to unassign role: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrAssignmentNotFound
	}
	s.removeGroupingEdge(memberSubject(assigneeType, assigneeID), RoleSubjectPrefix+roleName)
	return nil
}

func (s *Service) ListRoleAssignments(ctx context.Context, roleName string) ([]*RoleAssignment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.assignee_type, a.assignee_id, a.granted_by, a.granted_at, a.expires_at
		FROM acl_role_assignments a JOIN acl_roles r ON r.role_id = a.role_id
		WHERE r.role_name = $1 ORDER BY a.assignee_type, a.assignee_id
	`, roleName)
	if err != nil {
		return nil, fmt.Errorf("failed to list role assignments: %w", err)
	}
	defer rows.Close()
	var out []*RoleAssignment
	for rows.Next() {
		a := &RoleAssignment{RoleName: roleName}
		var grantedBy sql.NullString
		var expiresAt sql.NullTime
		if err := rows.Scan(&a.AssigneeType, &a.AssigneeID, &grantedBy, &a.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		a.GrantedBy = grantedBy.String
		if expiresAt.Valid {
			a.ExpiresAt = &expiresAt.Time
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Service) ListPrincipalRoles(ctx context.Context, assigneeType, assigneeID string) ([]*RoleAssignment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.role_name, a.granted_by, a.granted_at, a.expires_at
		FROM acl_role_assignments a JOIN acl_roles r ON r.role_id = a.role_id
		WHERE a.assignee_type = $1 AND a.assignee_id = $2 ORDER BY r.role_name
	`, assigneeType, assigneeID)
	if err != nil {
		return nil, fmt.Errorf("failed to list principal roles: %w", err)
	}
	defer rows.Close()
	var out []*RoleAssignment
	for rows.Next() {
		a := &RoleAssignment{AssigneeType: assigneeType, AssigneeID: assigneeID}
		var grantedBy sql.NullString
		var expiresAt sql.NullTime
		if err := rows.Scan(&a.RoleName, &grantedBy, &a.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		a.GrantedBy = grantedBy.String
		if expiresAt.Valid {
			a.ExpiresAt = &expiresAt.Time
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// =========================================================================
// Introspection + cleanup
// =========================================================================

// ExplainAccess returns the resolved subject set (self + transitive
// groups/roles), every rule that matched across those subjects, and the winning
// decision for a (principal, resource) pair. principalType/principalID are the
// canonical acl_rules values (e.g. "user","alice" or "agent","ag::ws::impl::spec").
// It performs no audit write — it is a pure introspection helper.
func (s *Service) ExplainAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, requiredLevel int, callerType, callerID string) (*AccessExplanation, error) {
	resourceType, resourceID, _ = rewriteLegacyPermission(resourceType, resourceID)
	self := principalType + ":" + principalID
	exp := &AccessExplanation{Principal: self, Subjects: []string{self}}
	if s.enforcer != nil {
		exp.Subjects = s.enforcer.SubjectSet(self)
		exp.Contributions = s.enforcer.Contributions(exp.Subjects, resourceType, resourceID)
		exp.Decision = s.enforcer.EvaluateBySubject(principalType, principalID, resourceType, resourceID, requiredLevel)
	}
	// Audit the introspection: who asked (caller) about whose access (principal).
	if s.audit != nil {
		s.audit.LogExplain(ctx, callerType, callerID, principalType, principalID, resourceType, resourceID, requiredLevel, exp.Decision)
	}
	return exp, nil
}

// CleanupExpiredMemberships deletes expired membership/assignment edges and
// reloads the enforcer. Returns the number of rows removed.
func (s *Service) CleanupExpiredMemberships(ctx context.Context) (int64, error) {
	var deleted int64
	if err := s.db.QueryRowContext(ctx, `SELECT cleanup_expired_acl_memberships()`).Scan(&deleted); err != nil {
		return 0, fmt.Errorf("failed to cleanup expired memberships: %w", err)
	}
	if deleted > 0 {
		s.reloadEnforcer("cleanup expired memberships")
	}
	return deleted, nil
}

// =========================================================================
// Internal helpers
// =========================================================================

func (s *Service) groupIDByName(ctx context.Context, name string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT group_id FROM acl_groups WHERE group_name = $1`, name).Scan(&id)
	if err == sql.ErrNoRows {
		return "", ErrGroupNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to look up group: %w", err)
	}
	return id, nil
}

func (s *Service) roleIDByName(ctx context.Context, name string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT role_id FROM acl_roles WHERE role_name = $1`, name).Scan(&id)
	if err == sql.ErrNoRows {
		return "", ErrRoleNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to look up role: %w", err)
	}
	return id, nil
}

// checkNoCycle rejects an edge source -> target that would create a cycle: the
// edge is unsafe if target already resolves (transitively) back to source, or
// source == target.
func (s *Service) checkNoCycle(source, target string) error {
	if source == target {
		return ErrMembershipCycle
	}
	if s.enforcer == nil {
		return nil
	}
	reachable, err := s.enforcer.ImplicitRoles(target)
	if err != nil {
		return nil // best-effort: a resolver error must not block writes
	}
	for _, r := range reachable {
		if r == source {
			return ErrMembershipCycle
		}
	}
	return nil
}

func (s *Service) addGroupingEdge(source, target string) {
	if s.enforcer == nil {
		return
	}
	if _, err := s.enforcer.AddGrouping(source, target); err != nil {
		logging.Logger.Warn().Err(err).Str("source", source).Str("target", target).Msg("acl: in-memory AddGrouping failed; persisted state authoritative")
	}
}

func (s *Service) removeGroupingEdge(source, target string) {
	if s.enforcer == nil {
		return
	}
	if _, err := s.enforcer.RemoveGrouping(source, target); err != nil {
		logging.Logger.Warn().Err(err).Str("source", source).Str("target", target).Msg("acl: in-memory RemoveGrouping failed; persisted state authoritative")
	}
}

func (s *Service) reloadEnforcer(reason string) {
	if s.enforcer == nil {
		return
	}
	if err := s.enforcer.ReloadPolicies(); err != nil {
		logging.Logger.Warn().Err(err).Str("reason", reason).Msg("acl: enforcer reload failed; in-memory model may lag DB until next reload")
	}
}

func scanGroup(s scanner) (*Group, error) {
	g := &Group{}
	var description, createdBy sql.NullString
	var meta []byte
	if err := s.Scan(&g.GroupID, &g.GroupName, &description, &createdBy, &g.CreatedAt, &meta); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrGroupNotFound
		}
		return nil, fmt.Errorf("failed to scan group: %w", err)
	}
	g.Description = description.String
	g.CreatedBy = createdBy.String
	g.Metadata = unmarshalMetadata(meta)
	return g, nil
}

func scanRole(s scanner) (*Role, error) {
	r := &Role{}
	var description, createdBy sql.NullString
	var meta []byte
	if err := s.Scan(&r.RoleID, &r.RoleName, &description, &createdBy, &r.CreatedAt, &meta); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrRoleNotFound
		}
		return nil, fmt.Errorf("failed to scan role: %w", err)
	}
	r.Description = description.String
	r.CreatedBy = createdBy.String
	r.Metadata = unmarshalMetadata(meta)
	return r, nil
}

func marshalMetadata(m map[string]interface{}) (interface{}, error) {
	if len(m) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return b, nil
}

func unmarshalMetadata(b []byte) map[string]interface{} {
	if len(b) == 0 {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// isUniqueViolation reports whether err is a unique-constraint violation.
// Driver-agnostic substring match (Postgres "duplicate key value violates
// unique constraint"; SQLite "UNIQUE constraint failed").
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "UNIQUE constraint")
}
