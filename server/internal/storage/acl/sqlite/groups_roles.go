package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/logging"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
)

// This file mirrors internal/acl/groups_roles.go (the Postgres path) using the
// native SQLite dialect: ? placeholders, ISO-8601 TEXT timestamps via
// formatTime, Go-generated UUIDs, and direct *casbin.SyncedEnforcer access.

func memberSubject(memberType, memberID string) string { return memberType + ":" + memberID }

// =========================================================================
// Group CRUD
// =========================================================================

func (s *Store) CreateGroup(ctx context.Context, name, description, createdBy string, metadata map[string]interface{}) (*aclstore.Group, error) {
	if name == "" {
		return nil, fmt.Errorf("group name is required")
	}
	g := &aclstore.Group{
		GroupID:     uuid.NewString(),
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
		VALUES (?, ?, ?, ?, ?, ?)
	`, g.GroupID, g.GroupName, nullString(description), nullString(createdBy), formatTime(g.CreatedAt), meta)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, aclstore.ErrGroupExists
		}
		return nil, fmt.Errorf("failed to create group: %w", err)
	}
	return g, nil
}

func (s *Store) DeleteGroup(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM acl_groups WHERE group_name = ?`, name)
	if err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return aclstore.ErrGroupNotFound
	}
	s.reloadEnforcer("delete group")
	return nil
}

func (s *Store) GetGroup(ctx context.Context, name string) (*aclstore.Group, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT group_id, group_name, description, created_by, created_at, metadata
		FROM acl_groups WHERE group_name = ?
	`, name)
	return scanGroupRow(row)
}

func (s *Store) ListGroups(ctx context.Context) ([]*aclstore.Group, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT group_id, group_name, description, created_by, created_at, metadata
		FROM acl_groups ORDER BY group_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	defer rows.Close()
	var out []*aclstore.Group
	for rows.Next() {
		g, err := scanGroupRow(rows)
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

func (s *Store) CreateRole(ctx context.Context, name, description, createdBy string, metadata map[string]interface{}) (*aclstore.Role, error) {
	if name == "" {
		return nil, fmt.Errorf("role name is required")
	}
	r := &aclstore.Role{
		RoleID:      uuid.NewString(),
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
		VALUES (?, ?, ?, ?, ?, ?)
	`, r.RoleID, r.RoleName, nullString(description), nullString(createdBy), formatTime(r.CreatedAt), meta)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, aclstore.ErrRoleExists
		}
		return nil, fmt.Errorf("failed to create role: %w", err)
	}
	return r, nil
}

func (s *Store) DeleteRole(ctx context.Context, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `DELETE FROM acl_roles WHERE role_name = ?`, name)
	if err != nil {
		return fmt.Errorf("failed to delete role: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return aclstore.ErrRoleNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acl_rules WHERE principal_type = ? AND principal_id = ?`,
		aclstore.PrincipalTypeRole, name); err != nil {
		return fmt.Errorf("failed to delete role permissions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit role delete: %w", err)
	}
	s.reloadEnforcer("delete role")
	return nil
}

func (s *Store) GetRole(ctx context.Context, name string) (*aclstore.Role, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT role_id, role_name, description, created_by, created_at, metadata
		FROM acl_roles WHERE role_name = ?
	`, name)
	return scanRoleRow(row)
}

func (s *Store) ListRoles(ctx context.Context) ([]*aclstore.Role, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT role_id, role_name, description, created_by, created_at, metadata
		FROM acl_roles ORDER BY role_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list roles: %w", err)
	}
	defer rows.Close()
	var out []*aclstore.Role
	for rows.Next() {
		r, err := scanRoleRow(rows)
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

func (s *Store) AddGroupMember(ctx context.Context, groupName, memberType, memberID, grantedBy string, expiresAt *time.Time) (*aclstore.GroupMember, error) {
	groupID, err := s.groupIDByName(ctx, groupName)
	if err != nil {
		return nil, err
	}
	target := aclstore.GroupSubjectPrefix + groupName
	source := memberSubject(memberType, memberID)
	if err := s.checkNoCycle(source, target); err != nil {
		return nil, err
	}

	m := &aclstore.GroupMember{
		GroupName:  groupName,
		MemberType: memberType,
		MemberID:   memberID,
		GrantedBy:  grantedBy,
		GrantedAt:  time.Now(),
		ExpiresAt:  expiresAt,
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO acl_group_members (id, group_id, member_type, member_id, granted_by, granted_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (group_id, member_type, member_id)
		DO UPDATE SET granted_by = excluded.granted_by, granted_at = excluded.granted_at, expires_at = excluded.expires_at
	`, uuid.NewString(), groupID, memberType, memberID, nullString(grantedBy), formatTime(m.GrantedAt), nullableTime(expiresAt))
	if err != nil {
		return nil, fmt.Errorf("failed to add group member: %w", err)
	}
	s.addGroupingEdge(source, target)
	return m, nil
}

func (s *Store) RemoveGroupMember(ctx context.Context, groupName, memberType, memberID string) error {
	groupID, err := s.groupIDByName(ctx, groupName)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM acl_group_members WHERE group_id = ? AND member_type = ? AND member_id = ?
	`, groupID, memberType, memberID)
	if err != nil {
		return fmt.Errorf("failed to remove group member: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return aclstore.ErrMembershipNotFound
	}
	s.removeGroupingEdge(memberSubject(memberType, memberID), aclstore.GroupSubjectPrefix+groupName)
	return nil
}

func (s *Store) ListGroupMembers(ctx context.Context, groupName string) ([]*aclstore.GroupMember, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.member_type, m.member_id, m.granted_by, m.granted_at, m.expires_at
		FROM acl_group_members m JOIN acl_groups g ON g.group_id = m.group_id
		WHERE g.group_name = ? ORDER BY m.member_type, m.member_id
	`, groupName)
	if err != nil {
		return nil, fmt.Errorf("failed to list group members: %w", err)
	}
	defer rows.Close()
	var out []*aclstore.GroupMember
	for rows.Next() {
		m := &aclstore.GroupMember{GroupName: groupName}
		var grantedBy, expiresAt sql.NullString
		if err := rows.Scan(&m.MemberType, &m.MemberID, &grantedBy, &m.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		m.GrantedBy = grantedBy.String
		assignExpiry(&m.ExpiresAt, expiresAt)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) ListPrincipalGroups(ctx context.Context, memberType, memberID string) ([]*aclstore.GroupMember, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.group_name, m.granted_by, m.granted_at, m.expires_at
		FROM acl_group_members m JOIN acl_groups g ON g.group_id = m.group_id
		WHERE m.member_type = ? AND m.member_id = ? ORDER BY g.group_name
	`, memberType, memberID)
	if err != nil {
		return nil, fmt.Errorf("failed to list principal groups: %w", err)
	}
	defer rows.Close()
	var out []*aclstore.GroupMember
	for rows.Next() {
		m := &aclstore.GroupMember{MemberType: memberType, MemberID: memberID}
		var grantedBy, expiresAt sql.NullString
		if err := rows.Scan(&m.GroupName, &grantedBy, &m.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		m.GrantedBy = grantedBy.String
		assignExpiry(&m.ExpiresAt, expiresAt)
		out = append(out, m)
	}
	return out, rows.Err()
}

// =========================================================================
// Role assignment
// =========================================================================

func (s *Store) AssignRole(ctx context.Context, roleName, assigneeType, assigneeID, grantedBy string, expiresAt *time.Time) (*aclstore.RoleAssignment, error) {
	roleID, err := s.roleIDByName(ctx, roleName)
	if err != nil {
		return nil, err
	}
	target := aclstore.RoleSubjectPrefix + roleName
	source := memberSubject(assigneeType, assigneeID)
	if err := s.checkNoCycle(source, target); err != nil {
		return nil, err
	}

	a := &aclstore.RoleAssignment{
		RoleName:     roleName,
		AssigneeType: assigneeType,
		AssigneeID:   assigneeID,
		GrantedBy:    grantedBy,
		GrantedAt:    time.Now(),
		ExpiresAt:    expiresAt,
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO acl_role_assignments (id, role_id, assignee_type, assignee_id, granted_by, granted_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (role_id, assignee_type, assignee_id)
		DO UPDATE SET granted_by = excluded.granted_by, granted_at = excluded.granted_at, expires_at = excluded.expires_at
	`, uuid.NewString(), roleID, assigneeType, assigneeID, nullString(grantedBy), formatTime(a.GrantedAt), nullableTime(expiresAt))
	if err != nil {
		return nil, fmt.Errorf("failed to assign role: %w", err)
	}
	s.addGroupingEdge(source, target)
	return a, nil
}

func (s *Store) UnassignRole(ctx context.Context, roleName, assigneeType, assigneeID string) error {
	roleID, err := s.roleIDByName(ctx, roleName)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM acl_role_assignments WHERE role_id = ? AND assignee_type = ? AND assignee_id = ?
	`, roleID, assigneeType, assigneeID)
	if err != nil {
		return fmt.Errorf("failed to unassign role: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return aclstore.ErrAssignmentNotFound
	}
	s.removeGroupingEdge(memberSubject(assigneeType, assigneeID), aclstore.RoleSubjectPrefix+roleName)
	return nil
}

func (s *Store) ListRoleAssignments(ctx context.Context, roleName string) ([]*aclstore.RoleAssignment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.assignee_type, a.assignee_id, a.granted_by, a.granted_at, a.expires_at
		FROM acl_role_assignments a JOIN acl_roles r ON r.role_id = a.role_id
		WHERE r.role_name = ? ORDER BY a.assignee_type, a.assignee_id
	`, roleName)
	if err != nil {
		return nil, fmt.Errorf("failed to list role assignments: %w", err)
	}
	defer rows.Close()
	var out []*aclstore.RoleAssignment
	for rows.Next() {
		a := &aclstore.RoleAssignment{RoleName: roleName}
		var grantedBy, expiresAt sql.NullString
		if err := rows.Scan(&a.AssigneeType, &a.AssigneeID, &grantedBy, &a.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		a.GrantedBy = grantedBy.String
		assignExpiry(&a.ExpiresAt, expiresAt)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) ListPrincipalRoles(ctx context.Context, assigneeType, assigneeID string) ([]*aclstore.RoleAssignment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.role_name, a.granted_by, a.granted_at, a.expires_at
		FROM acl_role_assignments a JOIN acl_roles r ON r.role_id = a.role_id
		WHERE a.assignee_type = ? AND a.assignee_id = ? ORDER BY r.role_name
	`, assigneeType, assigneeID)
	if err != nil {
		return nil, fmt.Errorf("failed to list principal roles: %w", err)
	}
	defer rows.Close()
	var out []*aclstore.RoleAssignment
	for rows.Next() {
		a := &aclstore.RoleAssignment{AssigneeType: assigneeType, AssigneeID: assigneeID}
		var grantedBy, expiresAt sql.NullString
		if err := rows.Scan(&a.RoleName, &grantedBy, &a.GrantedAt, &expiresAt); err != nil {
			return nil, err
		}
		a.GrantedBy = grantedBy.String
		assignExpiry(&a.ExpiresAt, expiresAt)
		out = append(out, a)
	}
	return out, rows.Err()
}

// =========================================================================
// Introspection + cleanup
// =========================================================================

func (s *Store) ExplainAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, requiredLevel int, callerType, callerID string) (*aclstore.AccessExplanation, error) {
	resourceType, resourceID, _ = acl.RewriteLegacyPermission(resourceType, resourceID)
	self := principalType + ":" + principalID
	exp := &aclstore.AccessExplanation{Principal: self, Subjects: []string{self}}
	if s.enforcer != nil {
		exp.Subjects = subjectSet(s.enforcer, self)
		exp.Contributions = acl.GatherContributions(s.enforcer, exp.Subjects, resourceType, resourceID)
		exp.Decision = s.evaluateBySubject(principalType, principalID, resourceType, resourceID, requiredLevel)
	}
	// Audit the introspection: who asked (caller) about whose access (principal).
	if s.aclAudit != nil {
		s.aclAudit.LogExplain(ctx, callerType, callerID, principalType, principalID, resourceType, resourceID, requiredLevel, exp.Decision)
	}
	return exp, nil
}

// CleanupExpiredMemberships deletes expired membership/assignment edges via
// parameterized DELETEs (no stored function in SQLite) and reloads the enforcer.
func (s *Store) CleanupExpiredMemberships(ctx context.Context) (int64, error) {
	now := formatTime(time.Now())
	var total int64
	for _, q := range []string{
		`DELETE FROM acl_group_members WHERE expires_at IS NOT NULL AND expires_at <= ?`,
		`DELETE FROM acl_role_assignments WHERE expires_at IS NOT NULL AND expires_at <= ?`,
	} {
		res, err := s.db.ExecContext(ctx, q, now)
		if err != nil {
			return total, fmt.Errorf("failed to cleanup expired memberships: %w", err)
		}
		n, _ := res.RowsAffected()
		total += n
	}
	if total > 0 {
		s.reloadEnforcer("cleanup expired memberships")
	}
	return total, nil
}

// =========================================================================
// Internal helpers
// =========================================================================

func (s *Store) groupIDByName(ctx context.Context, name string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT group_id FROM acl_groups WHERE group_name = ?`, name).Scan(&id)
	if err == sql.ErrNoRows {
		return "", aclstore.ErrGroupNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to look up group: %w", err)
	}
	return id, nil
}

func (s *Store) roleIDByName(ctx context.Context, name string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT role_id FROM acl_roles WHERE role_name = ?`, name).Scan(&id)
	if err == sql.ErrNoRows {
		return "", aclstore.ErrRoleNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to look up role: %w", err)
	}
	return id, nil
}

func (s *Store) checkNoCycle(source, target string) error {
	if source == target {
		return aclstore.ErrMembershipCycle
	}
	if s.enforcer == nil {
		return nil
	}
	reachable, err := s.enforcer.GetImplicitRolesForUser(target)
	if err != nil {
		return nil
	}
	for _, r := range reachable {
		if r == source {
			return aclstore.ErrMembershipCycle
		}
	}
	return nil
}

func (s *Store) addGroupingEdge(source, target string) {
	if s.enforcer == nil {
		return
	}
	if _, err := s.enforcer.AddGroupingPolicy(source, target); err != nil {
		logging.Logger.Warn().Err(err).Str("source", source).Str("target", target).Msg("acl sqlite: in-memory AddGroupingPolicy failed; persisted state authoritative")
	}
}

func (s *Store) removeGroupingEdge(source, target string) {
	if s.enforcer == nil {
		return
	}
	if _, err := s.enforcer.RemoveGroupingPolicy(source, target); err != nil {
		logging.Logger.Warn().Err(err).Str("source", source).Str("target", target).Msg("acl sqlite: in-memory RemoveGroupingPolicy failed; persisted state authoritative")
	}
}

func (s *Store) reloadEnforcer(reason string) {
	if s.enforcer == nil {
		return
	}
	if err := s.enforcer.LoadPolicy(); err != nil {
		logging.Logger.Warn().Err(err).Str("reason", reason).Msg("acl sqlite: enforcer reload failed; in-memory model may lag DB until next reload")
	}
}

func scanGroupRow(sc interface{ Scan(...interface{}) error }) (*aclstore.Group, error) {
	g := &aclstore.Group{}
	var description, createdBy, createdAt sql.NullString
	var meta []byte
	if err := sc.Scan(&g.GroupID, &g.GroupName, &description, &createdBy, &createdAt, &meta); err != nil {
		if err == sql.ErrNoRows {
			return nil, aclstore.ErrGroupNotFound
		}
		return nil, fmt.Errorf("failed to scan group: %w", err)
	}
	g.Description = description.String
	g.CreatedBy = createdBy.String
	g.CreatedAt = parseTime(createdAt.String)
	g.Metadata = unmarshalMetadata(meta)
	return g, nil
}

func scanRoleRow(sc interface{ Scan(...interface{}) error }) (*aclstore.Role, error) {
	r := &aclstore.Role{}
	var description, createdBy, createdAt sql.NullString
	var meta []byte
	if err := sc.Scan(&r.RoleID, &r.RoleName, &description, &createdBy, &createdAt, &meta); err != nil {
		if err == sql.ErrNoRows {
			return nil, aclstore.ErrRoleNotFound
		}
		return nil, fmt.Errorf("failed to scan role: %w", err)
	}
	r.Description = description.String
	r.CreatedBy = createdBy.String
	r.CreatedAt = parseTime(createdAt.String)
	r.Metadata = unmarshalMetadata(meta)
	return r, nil
}

// assignExpiry parses an ISO-8601 TEXT expiry column into *time.Time.
func assignExpiry(dst **time.Time, col sql.NullString) {
	if col.Valid && col.String != "" {
		t := parseTime(col.String)
		if !t.IsZero() {
			*dst = &t
		}
	}
}

func nullableTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

func marshalMetadata(m map[string]interface{}) (interface{}, error) {
	if len(m) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return string(b), nil
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

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "duplicate key")
}
