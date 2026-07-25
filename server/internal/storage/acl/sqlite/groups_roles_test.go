package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	aclstore "github.com/scitrera/aether/server/internal/storage/acl"
	aclsqlite "github.com/scitrera/aether/server/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/server/pkg/models"
)

func newRolesTestStore(t *testing.T) *aclsqlite.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acl_roles.db")
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	store, err := aclsqlite.New(db, nil, nil, "test-gw")
	if err != nil {
		t.Fatalf("aclsqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
	return store
}

func user(id string) models.Identity { return models.Identity{Type: models.PrincipalUser, ID: id} }

// Full stack: role permission + role->group + group membership reaches a user.
func TestSQLiteGroupsRoles_EndToEnd(t *testing.T) {
	ctx := context.Background()
	s := newRolesTestStore(t)

	if _, err := s.CreateRole(ctx, "wsadmin", "workspace admin", "tester", nil); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	// Grant the role MANAGE on workspace:prod (role permission = acl_rules row).
	if _, err := s.GrantAccess(ctx, aclstore.PrincipalTypeRole, "wsadmin", aclstore.ResourceTypeWorkspace, "prod", aclstore.AccessManage, "tester", "", nil); err != nil {
		t.Fatalf("GrantAccess to role: %v", err)
	}
	if _, err := s.CreateGroup(ctx, "eng", "engineering", "tester", nil); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if _, err := s.AssignRole(ctx, "wsadmin", aclstore.PrincipalTypeGroup, "eng", "tester", nil); err != nil {
		t.Fatalf("AssignRole to group: %v", err)
	}
	if _, err := s.AddGroupMember(ctx, "eng", aclstore.PrincipalTypeUser, "alice", "tester", nil); err != nil {
		t.Fatalf("AddGroupMember: %v", err)
	}

	// alice -> group:eng -> role:wsadmin -> workspace:prod MANAGE
	d, err := s.CheckAccess(ctx, user("alice"), aclstore.ResourceTypeWorkspace, "prod", "connect", "prod", uuid.Nil, aclstore.AccessManage)
	if err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if d == nil || !d.Allowed || d.EffectiveAccessLevel != aclstore.AccessManage {
		t.Fatalf("expected allow@30 via role, got %+v", d)
	}

	// A user with no group/role gets no rule match (would fall to fallback).
	d2, err := s.CheckAccess(ctx, user("bob"), aclstore.ResourceTypeWorkspace, "prod", "connect", "prod", uuid.Nil, aclstore.AccessManage)
	if err != nil {
		t.Fatalf("CheckAccess bob: %v", err)
	}
	if d2 != nil && d2.Allowed && d2.RuleApplied != nil {
		t.Fatalf("bob should not have a matching role/group rule, got %+v", d2)
	}

	// Reload from DB (adapter g-load path) must preserve the derived access.
	if _, err := s.CleanupExpiredMemberships(ctx); err != nil {
		t.Fatalf("CleanupExpiredMemberships: %v", err)
	}
	d3, err := s.CheckAccess(ctx, user("alice"), aclstore.ResourceTypeWorkspace, "prod", "connect", "prod", uuid.Nil, aclstore.AccessManage)
	if err != nil || d3 == nil || !d3.Allowed {
		t.Fatalf("expected alice still allowed after reload, got %+v err=%v", d3, err)
	}

	// Removing the membership revokes the derived access.
	if err := s.RemoveGroupMember(ctx, "eng", aclstore.PrincipalTypeUser, "alice"); err != nil {
		t.Fatalf("RemoveGroupMember: %v", err)
	}
	d4, _ := s.CheckAccess(ctx, user("alice"), aclstore.ResourceTypeWorkspace, "prod", "connect", "prod", uuid.Nil, aclstore.AccessManage)
	if d4 != nil && d4.Allowed && d4.RuleApplied != nil {
		t.Fatalf("alice access should be revoked after membership removal, got %+v", d4)
	}
}

// TestSQLiteGroupsRoles_ListReadsGrantedAt guards the list read-back paths for
// role assignments + group members. SQLite stores granted_at as TEXT, so these
// queries must scan it as a string and parse it — a direct time.Time scan fails
// with "unsupported Scan ... storing driver.Value type string into type
// *time.Time". These paths had no coverage (the other tests use CheckAccess /
// ExplainAccess, which read the in-memory enforcer, not the SQL rows) and shipped
// that bug.
func TestSQLiteGroupsRoles_ListReadsGrantedAt(t *testing.T) {
	ctx := context.Background()
	s := newRolesTestStore(t)
	before := time.Now().Add(-time.Minute)

	if _, err := s.CreateRole(ctx, "wsadmin", "", "tester", nil); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if _, err := s.CreateGroup(ctx, "eng", "", "tester", nil); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if _, err := s.AssignRole(ctx, "wsadmin", aclstore.PrincipalTypeGroup, "eng", "tester", nil); err != nil {
		t.Fatalf("AssignRole: %v", err)
	}
	if _, err := s.AddGroupMember(ctx, "eng", aclstore.PrincipalTypeUser, "alice", "tester", nil); err != nil {
		t.Fatalf("AddGroupMember: %v", err)
	}

	assigns, err := s.ListRoleAssignments(ctx, "wsadmin")
	if err != nil {
		t.Fatalf("ListRoleAssignments: %v", err)
	}
	if len(assigns) != 1 {
		t.Fatalf("want 1 role assignment, got %d", len(assigns))
	}
	if assigns[0].GrantedAt.IsZero() || assigns[0].GrantedAt.Before(before) {
		t.Fatalf("assignment GrantedAt not parsed from TEXT: %v", assigns[0].GrantedAt)
	}
	if assigns[0].GrantedBy != "tester" {
		t.Fatalf("assignment GrantedBy = %q, want tester", assigns[0].GrantedBy)
	}

	members, err := s.ListGroupMembers(ctx, "eng")
	if err != nil {
		t.Fatalf("ListGroupMembers: %v", err)
	}
	if len(members) != 1 || members[0].GrantedAt.IsZero() || members[0].GrantedAt.Before(before) {
		t.Fatalf("member GrantedAt not parsed from TEXT: %+v", members)
	}

	// Principal-oriented reads share the same scan path.
	proles, err := s.ListPrincipalRoles(ctx, aclstore.PrincipalTypeGroup, "eng")
	if err != nil {
		t.Fatalf("ListPrincipalRoles: %v", err)
	}
	if len(proles) != 1 || proles[0].GrantedAt.IsZero() {
		t.Fatalf("principal-role GrantedAt not parsed: %+v", proles)
	}
	pgroups, err := s.ListPrincipalGroups(ctx, aclstore.PrincipalTypeUser, "alice")
	if err != nil {
		t.Fatalf("ListPrincipalGroups: %v", err)
	}
	if len(pgroups) != 1 || pgroups[0].GrantedAt.IsZero() {
		t.Fatalf("principal-group GrantedAt not parsed: %+v", pgroups)
	}
}

// ExplainAccess surfaces the resolved subject set, the contributing rules
// (the "why"), and the decision — none of which CheckAccess exposes.
func TestSQLiteGroupsRoles_ExplainAccess(t *testing.T) {
	ctx := context.Background()
	s := newRolesTestStore(t)

	if _, err := s.CreateRole(ctx, "wsadmin", "", "tester", nil); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if _, err := s.GrantAccess(ctx, aclstore.PrincipalTypeRole, "wsadmin", aclstore.ResourceTypeWorkspace, "prod", aclstore.AccessManage, "tester", "", nil); err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}
	// A weaker direct grant on the same resource — should appear as a
	// contribution but not be the winner (additive max picks the role's 30).
	if _, err := s.GrantAccess(ctx, aclstore.PrincipalTypeUser, "alice", aclstore.ResourceTypeWorkspace, "prod", aclstore.AccessRead, "tester", "", nil); err != nil {
		t.Fatalf("GrantAccess direct: %v", err)
	}
	if _, err := s.CreateGroup(ctx, "eng", "", "tester", nil); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if _, err := s.AssignRole(ctx, "wsadmin", aclstore.PrincipalTypeGroup, "eng", "tester", nil); err != nil {
		t.Fatalf("AssignRole: %v", err)
	}
	if _, err := s.AddGroupMember(ctx, "eng", aclstore.PrincipalTypeUser, "alice", "tester", nil); err != nil {
		t.Fatalf("AddGroupMember: %v", err)
	}

	exp, err := s.ExplainAccess(ctx, aclstore.PrincipalTypeUser, "alice", aclstore.ResourceTypeWorkspace, "prod", aclstore.AccessManage, "admin_api", "test")
	if err != nil {
		t.Fatalf("ExplainAccess: %v", err)
	}
	if exp.Principal != "user:alice" {
		t.Errorf("principal = %q, want user:alice", exp.Principal)
	}
	// Subject set must include self + transitive group + role.
	want := map[string]bool{"user:alice": false, "group:eng": false, "role:wsadmin": false}
	for _, s := range exp.Subjects {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for sub, found := range want {
		if !found {
			t.Errorf("subject %q missing from %v", sub, exp.Subjects)
		}
	}
	// Both the direct READ rule and the role's MANAGE rule should be reported.
	var sawRole, sawSelf bool
	for _, c := range exp.Contributions {
		if c.Subject == "role:wsadmin" && c.AccessLevel == aclstore.AccessManage {
			sawRole = true
		}
		if c.Subject == "user:alice" && c.AccessLevel == aclstore.AccessRead {
			sawSelf = true
		}
	}
	if !sawRole || !sawSelf {
		t.Errorf("contributions missing role/self rule: %+v", exp.Contributions)
	}
	// Decision: additive max => MANAGE, allowed.
	if exp.Decision == nil || !exp.Decision.Allowed || exp.Decision.EffectiveAccessLevel != aclstore.AccessManage {
		t.Errorf("decision = %+v, want allow@30", exp.Decision)
	}
}

func TestSQLiteGroupsRoles_CycleRejected(t *testing.T) {
	ctx := context.Background()
	s := newRolesTestStore(t)
	for _, g := range []string{"a", "b"} {
		if _, err := s.CreateGroup(ctx, g, "", "tester", nil); err != nil {
			t.Fatalf("CreateGroup %s: %v", g, err)
		}
	}
	// b is a member of a.
	if _, err := s.AddGroupMember(ctx, "a", aclstore.PrincipalTypeGroup, "b", "tester", nil); err != nil {
		t.Fatalf("AddGroupMember a<-b: %v", err)
	}
	// Adding a as a member of b would create a cycle a->b->a.
	_, err := s.AddGroupMember(ctx, "b", aclstore.PrincipalTypeGroup, "a", "tester", nil)
	if !errors.Is(err, aclstore.ErrMembershipCycle) {
		t.Fatalf("expected ErrMembershipCycle, got %v", err)
	}
}

func TestSQLiteGroupsRoles_MembershipExpiry(t *testing.T) {
	ctx := context.Background()
	s := newRolesTestStore(t)
	if _, err := s.CreateRole(ctx, "reader", "", "tester", nil); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if _, err := s.GrantAccess(ctx, aclstore.PrincipalTypeRole, "reader", aclstore.ResourceTypeWorkspace, "prod", aclstore.AccessRead, "tester", "", nil); err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}
	past := time.Now().Add(-1 * time.Hour)
	if _, err := s.AssignRole(ctx, "reader", aclstore.PrincipalTypeUser, "carol", "tester", &past); err != nil {
		t.Fatalf("AssignRole expired: %v", err)
	}
	n, err := s.CleanupExpiredMemberships(ctx)
	if err != nil {
		t.Fatalf("CleanupExpiredMemberships: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 expired assignment removed, got %d", n)
	}
	// After cleanup + reload, carol must not retain role-derived access.
	d, _ := s.CheckAccess(ctx, user("carol"), aclstore.ResourceTypeWorkspace, "prod", "connect", "prod", uuid.Nil, aclstore.AccessRead)
	if d != nil && d.Allowed && d.RuleApplied != nil {
		t.Fatalf("carol access should be gone after expiry cleanup, got %+v", d)
	}
}
