package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	aclsqlite "github.com/scitrera/aether/server/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/server/pkg/models"
)

// The cross-agent shared per-user KV scopes must be DENIED by default on the
// SQLite (lite) path, exactly as on postgres.
//
// This is a parity test, not just a seed test. lite and full are meant to differ
// in deployment logistics, not behaviour, but the SQLite tree never carried the
// equivalent of postgres 019: both trees seed user/agent/task/service_kv_scope
// at READ_WRITE(20), so with no explicit deny a cross-agent read of another
// user's shared KV resolved enforcer-no-match -> fallback -> ALLOWED. These
// scopes hold billing, API keys and OAuth tokens.
//
// An explicit rule is the only way to express it: acl_fallback_policies is keyed
// by (principal_type, resource_type) and cannot distinguish scope NAMES.
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

// Every principal type that can hold a wildcard subject must be denied — "all
// authenticated principals" cannot be expressed as one row, so a per-type rule
// is seeded for each.
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

// Guard against over-tightening: the deny is scoped to the two SHARED scopes.
// Everything else on kv_scope must still resolve via the seeded fallback, or we
// would have broken every agent's access to its own KV.
func TestSQLiteKVScope_OtherScopesStillPermitted(t *testing.T) {
	store := newKVScopeTestStore(t)

	agent := models.Identity{Type: models.PrincipalAgent, ID: "ag::some::agent"}
	for _, scope := range []string{"global-exclusive", "workspace-exclusive"} {
		if !kvScopeAllowed(t, store, agent, scope) {
			t.Errorf("agent was DENIED on kv_scope:%s — the deny must be limited to the " +
				"two cross-agent shared per-user scopes", scope)
		}
	}
}
