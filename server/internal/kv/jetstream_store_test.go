package kv_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/cluster/nats"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
)

// newTestJSStore boots an embedded NATS server and returns a JetStreamKVStore.
func newTestJSStore(t *testing.T) *kv.JetStreamKVStore {
	t.Helper()
	es := &nats.EmbeddedServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := nats.Config{
		DataDir:     t.TempDir(),
		ListenHost:  "127.0.0.1",
		ClientPort:  -1,
		ClusterPort: -1,
	}
	if err := es.Start(ctx, cfg); err != nil {
		t.Fatalf("start nats: %v", err)
	}
	t.Cleanup(es.Stop)

	store, err := kv.NewJetStreamKVStore(context.Background(), es.JetStream())
	if err != nil {
		t.Fatalf("new jetstream kv store: %v", err)
	}
	return store
}

func jsAgent() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "production",
		Implementation: "python-worker",
		Specifier:      "instance-1",
	}
}

// TestJetStreamKV_Set_Get_Delete verifies basic CRUD operations.
func TestJetStreamKV_Set_Get_Delete(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	// Set then Get.
	if err := s.Set(ctx, agent, kv.ScopeGlobal, "mykey", "myvalue", "", "", 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, err := s.Get(ctx, agent, kv.ScopeGlobal, "mykey", "", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "myvalue" {
		t.Errorf("Get returned %q, want %q", val, "myvalue")
	}

	// Delete then Get should return ErrKeyNotFound.
	if err := s.Delete(ctx, agent, kv.ScopeGlobal, "mykey", "", ""); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = s.Get(ctx, agent, kv.ScopeGlobal, "mykey", "", "")
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}

	// Double-delete is idempotent.
	if err := s.Delete(ctx, agent, kv.ScopeGlobal, "mykey", "", ""); err != nil {
		t.Errorf("double-delete returned error: %v", err)
	}

	// Get on non-existent key returns ErrKeyNotFound.
	_, err = s.Get(ctx, agent, kv.ScopeGlobal, "no-such-key", "", "")
	if err == nil {
		t.Fatal("expected ErrKeyNotFound for missing key")
	}
}

// TestJetStreamKV_TTLExpiry sets a key with a short TTL and confirms that
// after the TTL elapses the key is logically gone.
func TestJetStreamKV_TTLExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TTL sleep test in -short mode")
	}
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	ttl := 200 * time.Millisecond
	if err := s.Set(ctx, agent, kv.ScopeGlobal, "ttlkey", "ttlval", "", "", ttl); err != nil {
		t.Fatalf("Set with TTL: %v", err)
	}

	// Immediately should be present.
	val, err := s.Get(ctx, agent, kv.ScopeGlobal, "ttlkey", "", "")
	if err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	if val != "ttlval" {
		t.Errorf("got %q before expiry, want %q", val, "ttlval")
	}

	// Wait for TTL to lapse.
	time.Sleep(ttl + 100*time.Millisecond)

	_, err = s.Get(ctx, agent, kv.ScopeGlobal, "ttlkey", "", "")
	if err == nil {
		t.Error("expected ErrKeyNotFound after TTL, got nil")
	}
}

// TestJetStreamKV_List_AllScopes verifies List across all eight KV scopes.
func TestJetStreamKV_List_AllScopes(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	type scopeCase struct {
		scope     kv.KVScope
		userID    string
		workspace string
	}
	cases := []scopeCase{
		{kv.ScopeGlobal, "", ""},
		{kv.ScopeGlobalExclusive, "", ""},
		{kv.ScopeWorkspace, "", "prod"},
		{kv.ScopeWorkspaceExclusive, "", "prod"},
		{kv.ScopeUserShared, "alice", ""},
		{kv.ScopeUser, "alice", ""},
		{kv.ScopeUserWorkspaceShared, "alice", "prod"},
		{kv.ScopeUserWorkspace, "alice", "prod"},
	}

	for _, c := range cases {
		if err := s.Set(ctx, agent, c.scope, "k1", "v1", c.userID, c.workspace, 0); err != nil {
			t.Errorf("Set scope=%s: %v", c.scope, err)
			continue
		}
		if err := s.Set(ctx, agent, c.scope, "k2", "v2", c.userID, c.workspace, 0); err != nil {
			t.Errorf("Set scope=%s: %v", c.scope, err)
			continue
		}
		items, err := s.List(ctx, agent, c.scope, c.userID, c.workspace)
		if err != nil {
			t.Errorf("List scope=%s: %v", c.scope, err)
			continue
		}
		if len(items) != 2 {
			t.Errorf("List scope=%s: got %d items, want 2", c.scope, len(items))
			continue
		}
		if items["k1"] != "v1" || items["k2"] != "v2" {
			t.Errorf("List scope=%s: unexpected items %v", c.scope, items)
		}
	}
}

// TestJetStreamKV_ListPaginated_OffsetLimit verifies offset+limit pagination.
func TestJetStreamKV_ListPaginated_OffsetLimit(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	// Insert 5 keys.
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i)) // a, b, c, d, e
		if err := s.Set(ctx, agent, kv.ScopeGlobal, key, key, "", "", 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	// Page 1: limit=2, offset=0.
	res, err := s.ListPaginated(ctx, agent, kv.ScopeGlobal, "", "", &kv.ListOptions{Limit: 2, Cursor: "0"})
	if err != nil {
		t.Fatalf("ListPaginated page1: %v", err)
	}
	if len(res.Items) != 2 {
		t.Errorf("page1: got %d items, want 2", len(res.Items))
	}
	if !res.HasMore {
		t.Error("page1: expected HasMore=true")
	}

	// Page 2 using NextCursor.
	res2, err := s.ListPaginated(ctx, agent, kv.ScopeGlobal, "", "", &kv.ListOptions{Limit: 2, Cursor: res.NextCursor})
	if err != nil {
		t.Fatalf("ListPaginated page2: %v", err)
	}
	if len(res2.Items) != 2 {
		t.Errorf("page2: got %d items, want 2", len(res2.Items))
	}
	if !res2.HasMore {
		t.Error("page2: expected HasMore=true")
	}

	// Page 3 (remainder).
	res3, err := s.ListPaginated(ctx, agent, kv.ScopeGlobal, "", "", &kv.ListOptions{Limit: 2, Cursor: res2.NextCursor})
	if err != nil {
		t.Fatalf("ListPaginated page3: %v", err)
	}
	if len(res3.Items) != 1 {
		t.Errorf("page3: got %d items, want 1", len(res3.Items))
	}
	if res3.HasMore {
		t.Error("page3: expected HasMore=false")
	}
}

// TestJetStreamKV_IncrementDecrement_Concurrent runs N goroutines each
// incrementing the same key once, then asserts the final value is N.
func TestJetStreamKV_IncrementDecrement_Concurrent(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()
	const N = 20

	var wg sync.WaitGroup
	var errCount atomic.Int32
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Increment(ctx, agent, kv.ScopeGlobal, "counter", "", ""); err != nil {
				t.Errorf("Increment: %v", err)
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if errCount.Load() > 0 {
		t.Fatalf("%d increment errors", errCount.Load())
	}

	val, err := s.Get(ctx, agent, kv.ScopeGlobal, "counter", "", "")
	if err != nil {
		t.Fatalf("Get counter: %v", err)
	}
	if val != "20" {
		t.Errorf("concurrent counter final=%q, want %q", val, "20")
	}

	// Decrement back to 0.
	for i := 0; i < N; i++ {
		if _, err := s.Decrement(ctx, agent, kv.ScopeGlobal, "counter", "", ""); err != nil {
			t.Fatalf("Decrement: %v", err)
		}
	}
	val, err = s.Get(ctx, agent, kv.ScopeGlobal, "counter", "", "")
	if err != nil {
		t.Fatalf("Get after decrement: %v", err)
	}
	if val != "0" {
		t.Errorf("after decrement counter=%q, want %q", val, "0")
	}
}

// TestJetStreamKV_IncrementIf_RespectsCeiling verifies the ceiling guard.
func TestJetStreamKV_IncrementIf_RespectsCeiling(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	// Increment by 5 with ceiling 10 → applied, result 5.
	val, applied, err := s.IncrementIf(ctx, agent, kv.ScopeGlobal, "cap", "", "", 5, 10)
	if err != nil {
		t.Fatalf("IncrementIf: %v", err)
	}
	if !applied || val != 5 {
		t.Errorf("got applied=%v val=%d, want applied=true val=5", applied, val)
	}

	// Increment by 6 would exceed ceiling 10 → rejected.
	val, applied, err = s.IncrementIf(ctx, agent, kv.ScopeGlobal, "cap", "", "", 6, 10)
	if err != nil {
		t.Fatalf("IncrementIf rejected: %v", err)
	}
	if applied || val != 5 {
		t.Errorf("got applied=%v val=%d, want applied=false val=5", applied, val)
	}

	// Increment by 5 to exactly ceiling → applied, result 10.
	val, applied, err = s.IncrementIf(ctx, agent, kv.ScopeGlobal, "cap", "", "", 5, 10)
	if err != nil {
		t.Fatalf("IncrementIf at ceiling: %v", err)
	}
	if !applied || val != 10 {
		t.Errorf("got applied=%v val=%d, want applied=true val=10", applied, val)
	}
}

// TestJetStreamKV_DecrementIf_RespectsFloor verifies the floor guard.
func TestJetStreamKV_DecrementIf_RespectsFloor(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	// Seed to 100.
	if _, _, err := s.IncrementIf(ctx, agent, kv.ScopeGlobal, "bal", "", "", 100, 10000); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Decrement by 25, floor 0 → applied, result 75.
	val, applied, err := s.DecrementIf(ctx, agent, kv.ScopeGlobal, "bal", "", "", 25, 0)
	if err != nil {
		t.Fatalf("DecrementIf: %v", err)
	}
	if !applied || val != 75 {
		t.Errorf("got applied=%v val=%d, want applied=true val=75", applied, val)
	}

	// Decrement by 80 would go below floor 0 → rejected.
	val, applied, err = s.DecrementIf(ctx, agent, kv.ScopeGlobal, "bal", "", "", 80, 0)
	if err != nil {
		t.Fatalf("DecrementIf over-floor: %v", err)
	}
	if applied || val != 75 {
		t.Errorf("got applied=%v val=%d, want applied=false val=75", applied, val)
	}

	// Decrement by exactly 75 to floor → applied, result 0.
	val, applied, err = s.DecrementIf(ctx, agent, kv.ScopeGlobal, "bal", "", "", 75, 0)
	if err != nil {
		t.Fatalf("DecrementIf to floor: %v", err)
	}
	if !applied || val != 0 {
		t.Errorf("got applied=%v val=%d, want applied=true val=0", applied, val)
	}
}

// TestJetStreamKV_ScopeIsolation writes a key in global scope and confirms
// it does not appear when listing workspace scope.
func TestJetStreamKV_ScopeIsolation(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	if err := s.Set(ctx, agent, kv.ScopeGlobal, "isolated", "globalval", "", "", 0); err != nil {
		t.Fatalf("Set global: %v", err)
	}

	// Listing workspace scope should not include the global key.
	items, err := s.List(ctx, agent, kv.ScopeWorkspace, "", "prod")
	if err != nil {
		t.Fatalf("List workspace: %v", err)
	}
	if _, ok := items["isolated"]; ok {
		t.Error("global-scope key should not appear in workspace-scope listing")
	}

	// Set in workspace scope and confirm global scope doesn't see it.
	if err := s.Set(ctx, agent, kv.ScopeWorkspace, "wskey", "wsval", "", "prod", 0); err != nil {
		t.Fatalf("Set workspace: %v", err)
	}
	globalItems, err := s.List(ctx, agent, kv.ScopeGlobal, "", "")
	if err != nil {
		t.Fatalf("List global: %v", err)
	}
	if _, ok := globalItems["wskey"]; ok {
		t.Error("workspace-scope key should not appear in global-scope listing")
	}
}
