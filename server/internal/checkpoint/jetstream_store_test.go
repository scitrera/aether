package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/pkg/models"
)

// startTestNATS boots an in-process NATS server with JetStream and returns
// a jetstream.JetStream context. The server is stopped via t.Cleanup.
func startTestNATS(t *testing.T) jetstream.JetStream {
	t.Helper()

	opts := &natsserver.Options{
		Host:               "127.0.0.1",
		Port:               -1, // ephemeral port
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 256 * 1024 * 1024,
		JetStreamMaxStore:  512 * 1024 * 1024,
		NoSigs:             true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready within 10s")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream new: %v", err)
	}
	return js
}

// testAgent returns a simple agent Identity for tests.
func testAgent(workspace, impl, spec string) models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: impl,
		Specifier:      spec,
	}
}

// newTestStore creates a JetStreamCheckpointStore with a configurable pruner interval.
func newTestStore(t *testing.T, js jetstream.JetStream, pruneInterval time.Duration) *JetStreamCheckpointStore {
	t.Helper()
	ctx := context.Background()
	store, err := newJetStreamCheckpointStoreWithInterval(ctx, js, pruneInterval)
	if err != nil {
		t.Fatalf("new jetstream checkpoint store: %v", err)
	}
	return store
}

// readSidecar reads the sidecar index entry for the given identity + key.
func readSidecar(t *testing.T, store *JetStreamCheckpointStore, id models.Identity, key string) jsSidecarEntry {
	t.Helper()
	ctx := context.Background()
	entry, err := store.idx.Get(ctx, jsKey(id, key))
	if err != nil {
		t.Fatalf("idx.Get(%q): %v", jsKey(id, key), err)
	}
	var sc jsSidecarEntry
	if err := json.Unmarshal(entry.Value(), &sc); err != nil {
		t.Fatalf("unmarshal sidecar: %v", err)
	}
	return sc
}

// --- Tests ---

func TestJetStreamCheckpoint_Save_Load_Small(t *testing.T) {
	js := startTestNATS(t)
	store := newTestStore(t, js, prunerDefaultInterval)
	ctx := context.Background()

	id := testAgent("test-ws", "my-agent", "instance-1")
	payload := bytes.Repeat([]byte("x"), 1024) // 1 KB

	if err := store.Save(ctx, id, "state", payload, 0); err != nil {
		t.Fatalf("save: %v", err)
	}

	cp, err := store.Load(ctx, id, "state")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected non-nil checkpoint")
	}
	if !bytes.Equal(cp.Data, payload) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(cp.Data), len(payload))
	}
	if cp.Key != "state" {
		t.Fatalf("unexpected key: %q", cp.Key)
	}
	if cp.Identity != id.String() {
		t.Fatalf("unexpected identity: %q", cp.Identity)
	}

	// Confirm routed to KV.
	sc := readSidecar(t, store, id, "state")
	if sc.Location != "kv" {
		t.Fatalf("expected location=kv for 1KB payload, got %q", sc.Location)
	}
}

func TestJetStreamCheckpoint_Save_Load_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-payload test in -short mode")
	}

	js := startTestNATS(t)
	store := newTestStore(t, js, prunerDefaultInterval)
	ctx := context.Background()

	id := testAgent("test-ws", "my-agent", "instance-2")
	payload := bytes.Repeat([]byte("L"), 1024*1024) // 1 MB — routes to Object Store

	if err := store.Save(ctx, id, "bigstate", payload, 0); err != nil {
		t.Fatalf("save large: %v", err)
	}

	// Confirm routed to obj.
	sc := readSidecar(t, store, id, "bigstate")
	if sc.Location != "obj" {
		t.Fatalf("expected location=obj for 1MB payload, got %q", sc.Location)
	}

	cp, err := store.Load(ctx, id, "bigstate")
	if err != nil {
		t.Fatalf("load large: %v", err)
	}
	if cp == nil {
		t.Fatal("expected non-nil checkpoint")
	}
	if !bytes.Equal(cp.Data, payload) {
		t.Fatalf("data mismatch: got %d bytes, want %d bytes", len(cp.Data), len(payload))
	}
}

func TestJetStreamCheckpoint_Delete_RemovesBothLocations(t *testing.T) {
	js := startTestNATS(t)
	store := newTestStore(t, js, prunerDefaultInterval)
	ctx := context.Background()

	id := testAgent("test-ws", "delete-agent", "spec-1")
	payload := []byte("hello checkpoint")

	if err := store.Save(ctx, id, "k1", payload, 0); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Confirm it exists.
	cp, err := store.Load(ctx, id, "k1")
	if err != nil || cp == nil {
		t.Fatalf("expected checkpoint to exist; err=%v, cp=%v", err, cp)
	}

	if err := store.Delete(ctx, id, "k1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Should be gone.
	cp, err = store.Load(ctx, id, "k1")
	if err != nil {
		t.Fatalf("unexpected error after delete: %v", err)
	}
	if cp != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestJetStreamCheckpoint_List(t *testing.T) {
	js := startTestNATS(t)
	store := newTestStore(t, js, prunerDefaultInterval)
	ctx := context.Background()

	id := testAgent("list-ws", "list-agent", "spec-1")
	otherId := testAgent("list-ws", "other-agent", "spec-1")

	wantKeys := []string{"alpha", "beta", "gamma"}
	for _, k := range wantKeys {
		if err := store.Save(ctx, id, k, []byte("data-"+k), 0); err != nil {
			t.Fatalf("save %q: %v", k, err)
		}
	}
	// Save one for a different identity — must not appear in List for id.
	if err := store.Save(ctx, otherId, "delta", []byte("other"), 0); err != nil {
		t.Fatalf("save other: %v", err)
	}

	listed, err := store.List(ctx, id)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != len(wantKeys) {
		t.Fatalf("expected %d keys, got %d: %v", len(wantKeys), len(listed), listed)
	}
	keySet := make(map[string]bool, len(listed))
	for _, k := range listed {
		keySet[k] = true
	}
	for _, want := range wantKeys {
		if !keySet[want] {
			t.Errorf("expected key %q in list, got: %v", want, listed)
		}
	}
}

func TestJetStreamCheckpoint_TTL_PrunerRemovesExpired(t *testing.T) {
	js := startTestNATS(t)
	// Use a long pruner interval; we drive pruning manually via pruneOnce.
	store := newTestStore(t, js, prunerDefaultInterval)
	ctx := context.Background()

	id := testAgent("ttl-ws", "ttl-agent", "spec-1")
	payload := []byte("temporary data")

	// Save with a short TTL.
	shortTTL := 50 * time.Millisecond
	if err := store.Save(ctx, id, "expiring", payload, shortTTL); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Confirm present immediately.
	cp, err := store.Load(ctx, id, "expiring")
	if err != nil || cp == nil {
		t.Fatalf("expected checkpoint immediately after save; err=%v", err)
	}

	// Wait for the TTL to lapse, then manually trigger the pruner.
	time.Sleep(100 * time.Millisecond)
	if err := store.pruneOnce(ctx); err != nil {
		t.Fatalf("pruneOnce: %v", err)
	}

	// Should be gone after pruning.
	cp, err = store.Load(ctx, id, "expiring")
	if err != nil {
		t.Fatalf("unexpected error after prune: %v", err)
	}
	if cp != nil {
		t.Fatal("expected entry to be pruned, but it still exists")
	}
}

func TestJetStreamCheckpoint_SizeBoundary(t *testing.T) {
	js := startTestNATS(t)
	store := newTestStore(t, js, prunerDefaultInterval)
	ctx := context.Background()

	id := testAgent("boundary-ws", "boundary-agent", "spec-1")

	exactly256KB := bytes.Repeat([]byte("A"), smallPayloadThreshold)  // exactly at threshold → KV
	oneByteOver := bytes.Repeat([]byte("B"), smallPayloadThreshold+1) // just over threshold → obj

	// Exactly 256 KB — must route to KV.
	if err := store.Save(ctx, id, "small-boundary", exactly256KB, 0); err != nil {
		t.Fatalf("save 256KB: %v", err)
	}
	sc := readSidecar(t, store, id, "small-boundary")
	if sc.Location != "kv" {
		t.Fatalf("expected location=kv for exactly 256KB payload, got %q", sc.Location)
	}
	cp1, err := store.Load(ctx, id, "small-boundary")
	if err != nil || cp1 == nil || !bytes.Equal(cp1.Data, exactly256KB) {
		t.Fatalf("load small-boundary failed: err=%v cp=%v", err, cp1)
	}

	// 256 KB + 1 byte — must route to Object Store.
	if testing.Short() {
		t.Skip("skipping object-store boundary check in -short mode")
	}
	if err := store.Save(ctx, id, "large-boundary", oneByteOver, 0); err != nil {
		t.Fatalf("save 256KB+1: %v", err)
	}
	sc2 := readSidecar(t, store, id, "large-boundary")
	if sc2.Location != "obj" {
		t.Fatalf("expected location=obj for 256KB+1 payload, got %q", sc2.Location)
	}
	cp2, err := store.Load(ctx, id, "large-boundary")
	if err != nil || cp2 == nil || !bytes.Equal(cp2.Data, oneByteOver) {
		t.Fatalf("load large-boundary failed: err=%v cp=%v", err, cp2)
	}
}
