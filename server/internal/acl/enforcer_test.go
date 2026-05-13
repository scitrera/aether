package acl

import (
	"context"
	"testing"
	"time"

	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/model"
	"github.com/scitrera/aether/pkg/models"
)

// newTestEnforcer builds a CasbinEnforcer backed by an in-memory policy store
// so no real database is needed.
func newTestEnforcer(t *testing.T) *CasbinEnforcer {
	t.Helper()
	m, err := model.NewModelFromString(casbinModelText)
	if err != nil {
		t.Fatalf("failed to create model: %v", err)
	}
	e, err := casbin.NewSyncedEnforcer(m)
	if err != nil {
		t.Fatalf("failed to create enforcer: %v", err)
	}
	e.EnableAutoSave(false)
	return &CasbinEnforcer{enforcer: e}
}

// addTestPolicy adds a policy directly to the in-memory enforcer.
func addTestPolicy(t *testing.T, ce *CasbinEnforcer, sub, obj, act, expires, ruleID string) {
	t.Helper()
	ok, err := ce.enforcer.AddPolicy(sub, obj, act, expires, ruleID)
	if err != nil || !ok {
		t.Fatalf("failed to add test policy sub=%s obj=%s act=%s: err=%v ok=%v", sub, obj, act, err, ok)
	}
}

// --- wildcardSubjects ---

func TestWildcardSubjects_UserReturnsAnyUser(t *testing.T) {
	subs := wildcardSubjects(PrincipalTypeUser)
	if len(subs) != 1 {
		t.Fatalf("expected 1 wildcard subject for user, got %d", len(subs))
	}
	want := PrincipalTypeWildcard + ":" + WildcardAnyAuthenticatedUser
	if subs[0] != want {
		t.Errorf("wildcardSubjects(user)[0] = %q, want %q", subs[0], want)
	}
}

func TestWildcardSubjects_AgentReturnsAnyAgent(t *testing.T) {
	subs := wildcardSubjects(PrincipalTypeAgent)
	if len(subs) != 1 {
		t.Fatalf("expected 1 wildcard subject for agent, got %d", len(subs))
	}
	want := PrincipalTypeWildcard + ":" + WildcardAnyAgent
	if subs[0] != want {
		t.Errorf("wildcardSubjects(agent)[0] = %q, want %q", subs[0], want)
	}
}

func TestWildcardSubjects_TaskReturnsAnyTask(t *testing.T) {
	subs := wildcardSubjects(PrincipalTypeTask)
	if len(subs) != 1 {
		t.Fatalf("expected 1 wildcard subject for task, got %d", len(subs))
	}
	want := PrincipalTypeWildcard + ":" + WildcardAnyTask
	if subs[0] != want {
		t.Errorf("wildcardSubjects(task)[0] = %q, want %q", subs[0], want)
	}
}

func TestWildcardSubjects_UnknownTypeReturnsNil(t *testing.T) {
	subs := wildcardSubjects("orchestrator")
	if len(subs) != 0 {
		t.Errorf("expected no wildcard subjects for orchestrator, got %v", subs)
	}
}

// --- findAndEvaluate ---

func TestFindAndEvaluate_NoMatchReturnsNil(t *testing.T) {
	ce := newTestEnforcer(t)
	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result != nil {
		t.Errorf("expected nil when no policy exists, got %+v", result)
	}
}

func TestFindAndEvaluate_ExactMatchAllowed(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "rule-1")

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Allowed {
		t.Error("expected Allowed=true")
	}
	if result.EffectiveAccessLevel != AccessRead {
		t.Errorf("EffectiveAccessLevel = %d, want %d", result.EffectiveAccessLevel, AccessRead)
	}
	if result.Decision != DecisionAllow {
		t.Errorf("Decision = %q, want ALLOW", result.Decision)
	}
}

func TestFindAndEvaluate_InsufficientLevelDenied(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "rule-1")

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessManage, "test")
	if result == nil {
		t.Fatal("expected non-nil result (rule matched, level insufficient)")
	}
	if result.Allowed {
		t.Error("expected Allowed=false when access level insufficient")
	}
	if result.Decision != DecisionDeny {
		t.Errorf("Decision = %q, want DENY", result.Decision)
	}
}

func TestFindAndEvaluate_ExpiredRuleReturnsNil(t *testing.T) {
	ce := newTestEnforcer(t)
	past := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", past, "rule-expired")

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result != nil {
		t.Errorf("expected nil for expired rule, got %+v", result)
	}
}

func TestFindAndEvaluate_FutureExpiryAllowed(t *testing.T) {
	ce := newTestEnforcer(t)
	future := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", future, "rule-valid")

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result == nil {
		t.Fatal("expected non-nil result for not-yet-expired rule")
	}
	if !result.Allowed {
		t.Error("expected Allowed=true for future expiry")
	}
}

func TestFindAndEvaluate_BestLevelWinsAcrossMultiplePolicies(t *testing.T) {
	ce := newTestEnforcer(t)
	// Two policies for same sub+obj with different levels
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "rule-read")
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "30", "", "rule-manage")

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessManage, "test")
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.EffectiveAccessLevel != AccessManage {
		t.Errorf("EffectiveAccessLevel = %d, want %d (best level)", result.EffectiveAccessLevel, AccessManage)
	}
}

func TestFindAndEvaluate_RuleIDPopulatedInDecision(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "rule-abc-123")

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.RuleApplied == nil {
		t.Fatal("expected RuleApplied to be set")
	}
	if result.RuleApplied.RuleID != "rule-abc-123" {
		t.Errorf("RuleApplied.RuleID = %q, want rule-abc-123", result.RuleApplied.RuleID)
	}
}

// --- EvaluateAccess: specificity-priority steps ---

func TestEvaluateAccess_ExactPrincipalExactResource(t *testing.T) {
	ce := newTestEnforcer(t)
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "r1")

	alice := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	d, err := ce.EvaluateAccess(context.Background(), alice, ResourceTypeWorkspace, "prod", AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d == nil || !d.Allowed {
		t.Error("expected exact match to allow access")
	}
}

func TestEvaluateAccess_WildcardPrincipalExactResource(t *testing.T) {
	ce := newTestEnforcer(t)
	// wildcard subject for any user
	wildSub := PrincipalTypeWildcard + ":" + WildcardAnyAuthenticatedUser
	addTestPolicy(t, ce, wildSub, "workspace:prod", "10", "", "r-wild")

	bob := models.Identity{Type: models.PrincipalUser, ID: "bob"}
	d, err := ce.EvaluateAccess(context.Background(), bob, ResourceTypeWorkspace, "prod", AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d == nil || !d.Allowed {
		t.Error("expected wildcard principal rule to allow access")
	}
}

func TestEvaluateAccess_ExactPrincipalWildcardResource(t *testing.T) {
	ce := newTestEnforcer(t)
	wildObj := ResourceTypeWorkspace + ":" + WildcardAnyResource
	addTestPolicy(t, ce, "user:alice", wildObj, "10", "", "r-wildobj")

	alice := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	d, err := ce.EvaluateAccess(context.Background(), alice, ResourceTypeWorkspace, "any-specific-ws", AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d == nil || !d.Allowed {
		t.Error("expected wildcard resource rule to allow access")
	}
}

func TestEvaluateAccess_NoMatchReturnsNil(t *testing.T) {
	ce := newTestEnforcer(t)

	alice := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	d, err := ce.EvaluateAccess(context.Background(), alice, ResourceTypeWorkspace, "prod", AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != nil {
		t.Error("expected nil decision when no matching rule exists")
	}
}

func TestEvaluateAccess_WildcardResourceNotUsedWhenExactMatch(t *testing.T) {
	ce := newTestEnforcer(t)
	// Exact resource match at read level, wildcard resource match at admin level
	addTestPolicy(t, ce, "user:alice", "workspace:prod", "10", "", "r-exact")
	wildObj := ResourceTypeWorkspace + ":" + WildcardAnyResource
	addTestPolicy(t, ce, "user:alice", wildObj, "40", "", "r-wildobj")

	alice := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	// For exact resource "prod", exact match (step 1) wins first
	d, err := ce.EvaluateAccess(context.Background(), alice, ResourceTypeWorkspace, "prod", AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d == nil {
		t.Fatal("expected non-nil decision")
	}
	// The exact match returns level 10, which satisfies read
	if !d.Allowed {
		t.Error("expected allowed via exact match")
	}
}

// --- AddPolicy / RemovePolicy ---

func TestAddAndRemovePolicy_RoundTrip(t *testing.T) {
	ce := newTestEnforcer(t)

	ok, err := ce.AddPolicy("user:alice", "workspace:prod", "10", "", "rule-x")
	if err != nil || !ok {
		t.Fatalf("AddPolicy failed: err=%v ok=%v", err, ok)
	}

	result := ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result == nil || !result.Allowed {
		t.Error("expected to find policy after AddPolicy")
	}

	ok, err = ce.RemovePolicy("user:alice", "workspace:prod")
	if err != nil || !ok {
		t.Fatalf("RemovePolicy failed: err=%v ok=%v", err, ok)
	}

	result = ce.findAndEvaluate("user:alice", "workspace:prod", AccessRead, "test")
	if result != nil {
		t.Error("expected nil after RemovePolicy")
	}
}
