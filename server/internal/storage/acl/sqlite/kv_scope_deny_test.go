package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	aclstore "github.com/scitrera/aether/server/internal/storage/acl"
	aclsqlite "github.com/scitrera/aether/server/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/server/pkg/models"
)

// kv_scope access is DENY-BY-DEFAULT on the SQLite (lite) path, exactly as on
// postgres — lite and full are meant to differ in deployment logistics, not
// behaviour.
//
// History worth keeping: both trees originally seeded user/agent/task/
// service_kv_scope at READ_WRITE(20), so anything without a rule was ALLOWED —
// including cross-agent reads of another user's shared KV (billing, API keys,
// OAuth tokens). That was first patched with per-scope NONE(0) rows, which broke
// legitimate access (see TestSQLiteKVScope_ExplicitGlobGrantResolves) and is now
// expressed as a deny-by-default fallback instead (migrations 031 / sqlite 006).
func newKVScopeTestStore(t *testing.T) *aclsqlite.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acl_kv_scope.db")
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

func kvScopeAllowed(t *testing.T, store *aclsqlite.Store, principal models.Identity, scope string) bool {
	t.Helper()
	const accessRead = 10
	decision, err := store.CheckAccess(context.Background(), principal,
		"kv_scope", scope, "read", "", uuid.Nil, accessRead)
	if err != nil {
		t.Fatalf("CheckAccess(%s, %s): %v", principal.Type, scope, err)
	}
	if decision == nil {
		t.Fatalf("CheckAccess(%s, %s) returned a nil decision", principal.Type, scope)
	}
	return decision.Allowed
}

// No principal type reaches the shared per-user scopes without an explicit
// grant. These are the scopes the whole exercise exists to protect.
func TestSQLiteKVScope_SharedUserScopesDeniedByDefault(t *testing.T) {
	store := newKVScopeTestStore(t)

	principals := []models.Identity{
		{Type: models.PrincipalUser, ID: "alice@example.com"},
		{Type: models.PrincipalAgent, ID: "ag::some::agent"},
		{Type: models.PrincipalTask, ID: "task-1"},
		{Type: models.PrincipalService, ID: "sv::some-service"},
	}
	scopes := []string{"user-shared", "user-workspace-shared"}

	for _, p := range principals {
		for _, scope := range scopes {
			if kvScopeAllowed(t, store, p, scope) {
				t.Errorf("%s principal was ALLOWED on kv_scope:%s — the privacy default-deny " +
					"is not in effect (fallback leaked through)", p.Type, scope)
			}
		}
	}
}

// Deny-by-default: with no explicit grant, a principal reaches NO kv_scope.
//
// This replaced a row-based deny that encoded denial as access_level NONE(0).
// NONE is the absence of an access level, not an assertion of denial — and
// structurally it could not be carved out, because the enforcer returns at the
// first matching specificity tier and a wildcard-principal rule on an exact
// resource (tier 2) always outranks an sv::<impl>::* glob grant (tier 5).
func TestSQLiteKVScope_DenyByDefaultWithoutAGrant(t *testing.T) {
	store := newKVScopeTestStore(t)

	agent := models.Identity{Type: models.PrincipalAgent, ID: "ag::some::agent"}
	for _, scope := range []string{"user-shared", "user-workspace-shared", "global", "workspace"} {
		if kvScopeAllowed(t, store, agent, scope) {
			t.Errorf("ungranted agent was ALLOWED on kv_scope:%s — the fallback should "+
				"no longer hand out access to scopes nobody granted", scope)
		}
	}
}

// THE REGRESSION THIS EXISTS FOR: an explicit glob grant must actually resolve.
//
// Under the old row-based deny, platform-server held exactly this grant and was
// still denied in production ("KV access denied ... Wildcard rule: NONE"),
// because the tier-2 wildcard deny short-circuited before the tier-5 glob was
// ever consulted. With the deny expressed as a fallback instead, nothing
// short-circuits ahead of the grant.
//
// The identity embeds the pod name, which is why the grant must be a glob and
// cannot be an exact tier-1 rule.
func TestSQLiteKVScope_ExplicitGlobGrantResolves(t *testing.T) {
	store := newKVScopeTestStore(t)
	ctx := context.Background()

	platformServer := models.Identity{
		Type: models.PrincipalService,
		ID:   "sv::platform-server::ws-platform-server-7ccbf5cbf9-bhn4c-botwinick",
	}
	if kvScopeAllowed(t, store, platformServer, "user-shared") {
		t.Fatal("precondition failed: allowed before any grant exists")
	}

	if _, err := store.GrantAccess(ctx, "service", "sv::platform-server::*",
		"kv_scope", "user-shared", 20, "_test", "per-user session state", nil); err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}

	if !kvScopeAllowed(t, store, platformServer, "user-shared") {
		t.Error("explicit sv::platform-server::* grant did NOT resolve for a concrete " +
			"pod identity — the glob tier is being short-circuited again")
	}
	// The grant is scoped: it must not leak to the sibling scope or to others.
	if kvScopeAllowed(t, store, platformServer, "user-workspace-shared") {
		t.Error("grant on user-shared leaked to user-workspace-shared")
	}
	other := models.Identity{Type: models.PrincipalService, ID: "sv::other-service::pod-1"}
	if kvScopeAllowed(t, store, other, "user-shared") {
		t.Error("grant scoped to platform-server leaked to another service")
	}
}

// The inert `_global` rule was REMOVED (migration 005 / postgres 030) rather
// than repaired. Removal must be a no-op for access: _global was never reachable
// via that rule (principal_id '_any_authenticated' matches no wildcard subject),
// only via the *_workspace fallback policies — which are untouched.
//
// Repairing it instead would have GRANTED every authenticated principal
// READ_WRITE on _global, access nobody holds today. This test pins that we did
// not accidentally do that, and did not lose _global access either.
func TestSQLiteGlobalWorkspace_InertRuleRemovedWithoutChangingAccess(t *testing.T) {
	store := newKVScopeTestStore(t)
	const accessRead = 10

	// The dead row must be gone.
	rules, err := store.ListRules(context.Background(), aclstore.RuleFilter{
		PrincipalType: "wildcard",
		PrincipalID:   "_any_authenticated",
	})
	if err == nil && len(rules) > 0 {
		t.Errorf("inert '_any_authenticated' rules still present after migration: %d", len(rules))
	}

	// ...and _global is still reachable, via the fallback rather than the rule.
	for _, p := range []models.Identity{
		{Type: models.PrincipalUser, ID: "alice@example.com"},
		{Type: models.PrincipalAgent, ID: "ag::some::agent"},
	} {
		decision, err := store.CheckAccess(context.Background(), p,
			"workspace", "_global", "read", "_global", uuid.Nil, accessRead)
		if err != nil {
			t.Fatalf("CheckAccess(%s, workspace:_global): %v", p.Type, err)
		}
		if decision == nil || !decision.Allowed {
			t.Errorf("%s LOST access to workspace:_global — removal was supposed to be a "+
				"no-op (the fallback, not the rule, is what grants it)", p.Type)
		}
	}
}
