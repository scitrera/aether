package acl

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

// addTestGrouping adds a grouping (g) edge directly to the in-memory enforcer.
func addTestGrouping(t *testing.T, ce *CasbinEnforcer, member, target string) {
	t.Helper()
	ok, err := ce.enforcer.AddGroupingPolicy(member, target)
	if err != nil || !ok {
		t.Fatalf("failed to add grouping %s -> %s: err=%v ok=%v", member, target, err, ok)
	}
}

func userPrincipal(id string) models.Identity {
	return models.Identity{Type: models.PrincipalUser, ID: id}
}

// Backward compatibility: with no groups/roles, evaluation is unchanged and the
// reason carries no "via" annotation.
func TestEvaluate_NoGroups_BackwardCompatible(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "20", "", "r1")

	d, err := ce.EvaluateAccess(context.Background(), userPrincipal("alice"), ResourceTypeWorkspace, "prod", AccessReadWrite)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if d == nil || !d.Allowed || d.EffectiveAccessLevel != AccessReadWrite {
		t.Fatalf("expected allow@20, got %+v", d)
	}
	if strings.Contains(d.Reason, "via ") {
		t.Errorf("reason should not be annotated with via for a direct rule: %q", d.Reason)
	}
}

// Additive max: a role grant higher than the principal's own grant wins, and
// the decision is annotated with the granting subject.
func TestEvaluate_AdditiveMax_RoleHigherThanSelf(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "r-self")   // READ
	addTestPolicy(t, ce, "role:wsadmin", "workspace:prod", "30", "", "r-role") // MANAGE
	addTestGrouping(t, ce, "user:alice", "role:wsadmin")

	d, err := ce.EvaluateAccess(context.Background(), userPrincipal("alice"), ResourceTypeWorkspace, "prod", AccessManage)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if d == nil || !d.Allowed || d.EffectiveAccessLevel != AccessManage {
		t.Fatalf("expected allow@30 via role, got %+v", d)
	}
	if d.RuleApplied == nil || d.RuleApplied.RuleID != "r-role" {
		t.Errorf("expected winning rule r-role, got %+v", d.RuleApplied)
	}
	if !strings.Contains(d.Reason, "via role:wsadmin") {
		t.Errorf("expected reason annotated 'via role:wsadmin', got %q", d.Reason)
	}
}

// A direct grant higher than the group grant wins, with no via annotation.
func TestEvaluate_AdditiveMax_SelfHigherThanGroup(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "40", "", "r-self") // ADMIN
	addTestPolicy(t, ce, "group:eng", "workspace:prod", "20", "", "r-grp")   // READWRITE
	addTestGrouping(t, ce, "user:alice", "group:eng")

	d, err := ce.EvaluateAccess(context.Background(), userPrincipal("alice"), ResourceTypeWorkspace, "prod", AccessManage)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if d == nil || !d.Allowed || d.EffectiveAccessLevel != AccessAdmin {
		t.Fatalf("expected allow@40 from self, got %+v", d)
	}
	if strings.Contains(d.Reason, "via ") {
		t.Errorf("self rule won; reason should not be via-annotated: %q", d.Reason)
	}
}

// Transitive nesting: user -> group -> role; the role's grant reaches the user.
func TestEvaluate_TransitiveNesting(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "role:wsadmin", "workspace:prod", "30", "", "r-role")
	addTestGrouping(t, ce, "user:alice", "group:eng")
	addTestGrouping(t, ce, "group:eng", "role:wsadmin")

	d, err := ce.EvaluateAccess(context.Background(), userPrincipal("alice"), ResourceTypeWorkspace, "prod", AccessManage)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if d == nil || !d.Allowed || d.EffectiveAccessLevel != AccessManage {
		t.Fatalf("expected allow@30 via nested role, got %+v", d)
	}
}

// A role granted at the wildcard-resource tier still applies to a member.
func TestEvaluate_RoleWildcardResource(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "role:wsadmin", "workspace:*", "30", "", "r-role")
	addTestGrouping(t, ce, "user:alice", "role:wsadmin")

	d, err := ce.EvaluateAccess(context.Background(), userPrincipal("alice"), ResourceTypeWorkspace, "prod", AccessManage)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if d == nil || !d.Allowed || d.EffectiveAccessLevel != AccessManage {
		t.Fatalf("expected allow@30 via role wildcard-resource, got %+v", d)
	}
}

// Removing a grouping edge revokes the derived access.
func TestEvaluate_RemoveGroupingRevokesAccess(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "role:wsadmin", "workspace:prod", "30", "", "r-role")
	addTestGrouping(t, ce, "user:alice", "role:wsadmin")

	if _, err := ce.RemoveGrouping("user:alice", "role:wsadmin"); err != nil {
		t.Fatalf("remove grouping: %v", err)
	}
	d, err := ce.EvaluateAccess(context.Background(), userPrincipal("alice"), ResourceTypeWorkspace, "prod", AccessManage)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if d != nil {
		t.Fatalf("expected no rule match after grouping removal, got %+v", d)
	}
}
