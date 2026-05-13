package gateway

// Cross-agent KV rendezvous tests using a real BadgerKVStore (no Redis).
//
// These tests verify the storage-layout semantics introduced by the KV scope
// revamp:
//
//   - ScopeUserShared (cross-agent per-user): agent A writes, agent B reads → same value.
//   - ScopeUserWorkspaceShared (cross-agent per-user-per-workspace): same rendezvous guarantee.
//   - ScopeWorkspaceExclusive (per-agent per-workspace): A and B write to "same" key → isolated namespaces, no collision.
//   - ScopeGlobalExclusive (per-agent global): same isolation guarantee across agents.
//
// No Redis is required; all tests use an in-process Badger DB.

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
)

// newScopesTestBadgerStore opens a fresh in-memory Badger DB and registers
// cleanup on t. Using InMemory avoids on-disk flush goroutines that can cause
// test timeouts at shutdown.
func newScopesTestBadgerStore(t *testing.T) *kv.BadgerKVStore {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open (in-memory): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return kv.NewBadgerKVStore(db)
}

// agentA and agentB are two distinct agent identities sharing the same
// workspace. The "Shared" scope tests demonstrate cross-agent storage
// rendezvous; the "Exclusive" scope tests demonstrate isolation.
func agentA() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-test",
		Implementation: "worker",
		Specifier:      "agent-a",
	}
}

func agentB() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-test",
		Implementation: "worker",
		Specifier:      "agent-b",
	}
}

// ---------------------------------------------------------------------------
// USER_SHARED: cross-agent rendezvous for a specific user
// ---------------------------------------------------------------------------

// TestKVScopes_UserShared_CrossAgentRendezvous verifies that a write by
// agent A to ScopeUserShared is immediately readable by agent B under the
// same userID — demonstrating the storage rendezvous property.
func TestKVScopes_UserShared_CrossAgentRendezvous(t *testing.T) {
	store := newScopesTestBadgerStore(t)
	ctx := context.Background()
	const userID = "alice"

	// Agent A writes.
	if err := store.Set(ctx, agentA(), kv.ScopeUserShared, "notebook", "page-1-content", userID, "", 0); err != nil {
		t.Fatalf("agentA Set: %v", err)
	}

	// Agent B reads — must see the same value.
	val, err := store.Get(ctx, agentB(), kv.ScopeUserShared, "notebook", userID, "")
	if err != nil {
		t.Fatalf("agentB Get: %v", err)
	}
	if val != "page-1-content" {
		t.Errorf("cross-agent rendezvous failed: agentB got %q, want %q", val, "page-1-content")
	}
}

// TestKVScopes_UserShared_ListRendezvous verifies that List also returns
// keys written by a different agent under the shared user scope.
func TestKVScopes_UserShared_ListRendezvous(t *testing.T) {
	store := newScopesTestBadgerStore(t)
	ctx := context.Background()
	const userID = "bob"

	if err := store.Set(ctx, agentA(), kv.ScopeUserShared, "prefs", "dark-mode", userID, "", 0); err != nil {
		t.Fatalf("agentA Set: %v", err)
	}

	items, err := store.List(ctx, agentB(), kv.ScopeUserShared, userID, "")
	if err != nil {
		t.Fatalf("agentB List: %v", err)
	}
	if v, ok := items["prefs"]; !ok || v != "dark-mode" {
		t.Errorf("expected agentB List to include agentA's write; got %v", items)
	}
}

// ---------------------------------------------------------------------------
// USER_WORKSPACE_SHARED: cross-agent rendezvous scoped to a workspace
// ---------------------------------------------------------------------------

func TestKVScopes_UserWorkspaceShared_CrossAgentRendezvous(t *testing.T) {
	store := newScopesTestBadgerStore(t)
	ctx := context.Background()
	const userID = "carol"
	const workspace = "ws-test"

	if err := store.Set(ctx, agentA(), kv.ScopeUserWorkspaceShared, "state", "active", userID, workspace, 0); err != nil {
		t.Fatalf("agentA Set: %v", err)
	}

	val, err := store.Get(ctx, agentB(), kv.ScopeUserWorkspaceShared, "state", userID, workspace)
	if err != nil {
		t.Fatalf("agentB Get: %v", err)
	}
	if val != "active" {
		t.Errorf("cross-agent rendezvous failed for user-workspace-shared: got %q, want \"active\"", val)
	}
}

// ---------------------------------------------------------------------------
// WORKSPACE_EXCLUSIVE: per-agent isolation (no cross-agent bleed)
// ---------------------------------------------------------------------------

// TestKVScopes_WorkspaceExclusive_IsolatesAgents verifies that agent A and
// agent B writing to the same logical key under ScopeWorkspaceExclusive do
// NOT share storage — each agent reads back only its own value.
func TestKVScopes_WorkspaceExclusive_IsolatesAgents(t *testing.T) {
	store := newScopesTestBadgerStore(t)
	ctx := context.Background()
	const workspace = "ws-test"

	if err := store.Set(ctx, agentA(), kv.ScopeWorkspaceExclusive, "model-version", "v1", "", workspace, 0); err != nil {
		t.Fatalf("agentA Set: %v", err)
	}
	if err := store.Set(ctx, agentB(), kv.ScopeWorkspaceExclusive, "model-version", "v2", "", workspace, 0); err != nil {
		t.Fatalf("agentB Set: %v", err)
	}

	// Agent A reads back its own value.
	valA, err := store.Get(ctx, agentA(), kv.ScopeWorkspaceExclusive, "model-version", "", workspace)
	if err != nil {
		t.Fatalf("agentA Get: %v", err)
	}
	if valA != "v1" {
		t.Errorf("agentA workspace-exclusive isolation broken: got %q, want \"v1\"", valA)
	}

	// Agent B reads back its own value (not A's).
	valB, err := store.Get(ctx, agentB(), kv.ScopeWorkspaceExclusive, "model-version", "", workspace)
	if err != nil {
		t.Fatalf("agentB Get: %v", err)
	}
	if valB != "v2" {
		t.Errorf("agentB workspace-exclusive isolation broken: got %q, want \"v2\"", valB)
	}
}

// ---------------------------------------------------------------------------
// GLOBAL_EXCLUSIVE: per-agent global isolation
// ---------------------------------------------------------------------------

func TestKVScopes_GlobalExclusive_IsolatesAgents(t *testing.T) {
	store := newScopesTestBadgerStore(t)
	ctx := context.Background()

	if err := store.Set(ctx, agentA(), kv.ScopeGlobalExclusive, "global-cfg", "cfg-a", "", "", 0); err != nil {
		t.Fatalf("agentA Set: %v", err)
	}
	if err := store.Set(ctx, agentB(), kv.ScopeGlobalExclusive, "global-cfg", "cfg-b", "", "", 0); err != nil {
		t.Fatalf("agentB Set: %v", err)
	}

	valA, err := store.Get(ctx, agentA(), kv.ScopeGlobalExclusive, "global-cfg", "", "")
	if err != nil {
		t.Fatalf("agentA Get: %v", err)
	}
	if valA != "cfg-a" {
		t.Errorf("agentA global-exclusive isolation broken: got %q, want \"cfg-a\"", valA)
	}

	valB, err := store.Get(ctx, agentB(), kv.ScopeGlobalExclusive, "global-cfg", "", "")
	if err != nil {
		t.Fatalf("agentB Get: %v", err)
	}
	if valB != "cfg-b" {
		t.Errorf("agentB global-exclusive isolation broken: got %q, want \"cfg-b\"", valB)
	}
}
