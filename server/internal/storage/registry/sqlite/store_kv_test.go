package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nats-io/nats.go/jetstream"

	clusternats "github.com/scitrera/aether/internal/cluster/nats"
	legacyregistry "github.com/scitrera/aether/internal/registry"
	registrysqlite "github.com/scitrera/aether/internal/storage/registry/sqlite"
	sqliteregistrymigrations "github.com/scitrera/aether/migrations/sqlite_registry"
)

func newWriteSideStore(t *testing.T) (*registrysqlite.Store, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "registry.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	stateDB, err := badger.Open(opts)
	if err != nil {
		_ = db.Close()
		t.Fatalf("badger.Open: %v", err)
	}
	state := legacyregistry.NewBadgerProfileStateStore(stateDB)
	store, err := registrysqlite.New(db, state, sqliteregistrymigrations.MigrationFS)
	if err != nil {
		_ = stateDB.Close()
		_ = db.Close()
		t.Fatalf("registrysqlite.New: %v", err)
	}
	cleanup := func() {
		_ = stateDB.Close()
		_ = db.Close()
	}
	return store, cleanup
}

func newWriteSideJS(t *testing.T) jetstream.JetStream {
	t.Helper()
	es := &clusternats.EmbeddedServer{}
	startCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := es.Start(startCtx, clusternats.Config{
		DataDir:    t.TempDir(),
		ListenHost: "127.0.0.1",
		ClientPort: -1,
	}); err != nil {
		t.Fatalf("start embedded nats: %v", err)
	}
	t.Cleanup(func() { es.Stop() })
	return es.JetStream()
}

func TestRegistrySQLite_Register_PropagatesToKV_WhenSet(t *testing.T) {
	store, cleanup := newWriteSideStore(t)
	defer cleanup()
	ctx := context.Background()
	js := newWriteSideJS(t)
	kv, err := legacyregistry.CreateOrOpenRegistryBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("CreateOrOpenRegistryBucket: %v", err)
	}
	store.SetRegistryKV(kv)

	reg := &legacyregistry.AgentRegistration{
		Implementation: "com.example.write-side-agent",
		LaunchParams:   map[string]interface{}{"profile": "test", "image": "test:1"},
	}
	if err := store.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	encodedKey, err := legacyregistry.EncodeRegistryKey(reg.Implementation)
	if err != nil {
		t.Fatalf("EncodeRegistryKey: %v", err)
	}
	if _, err := kv.Get(ctx, encodedKey); err != nil {
		t.Fatalf("expected KV entry for %q, got error: %v", encodedKey, err)
	}
}

func TestRegistrySQLite_Delete_PropagatesToKV_WhenSet(t *testing.T) {
	store, cleanup := newWriteSideStore(t)
	defer cleanup()
	ctx := context.Background()
	js := newWriteSideJS(t)
	kv, err := legacyregistry.CreateOrOpenRegistryBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("CreateOrOpenRegistryBucket: %v", err)
	}
	store.SetRegistryKV(kv)

	reg := &legacyregistry.AgentRegistration{
		Implementation: "com.example.delete-me",
		LaunchParams:   map[string]interface{}{"profile": "test", "image": "test:1"},
	}
	if err := store.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := store.Delete(ctx, reg.Implementation); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	encodedKey, err := legacyregistry.EncodeRegistryKey(reg.Implementation)
	if err != nil {
		t.Fatalf("EncodeRegistryKey: %v", err)
	}
	if _, err := kv.Get(ctx, encodedKey); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for %q, got: %v", encodedKey, err)
	}
}

func TestRegistrySQLite_NilKV_NoOp(t *testing.T) {
	store, cleanup := newWriteSideStore(t)
	defer cleanup()
	ctx := context.Background()

	reg := &legacyregistry.AgentRegistration{
		Implementation: "com.example.no-kv",
		LaunchParams:   map[string]interface{}{"profile": "test", "image": "test:1"},
	}
	if err := store.Register(ctx, reg); err != nil {
		t.Fatalf("Register without KV: %v", err)
	}
	got, err := store.Get(ctx, reg.Implementation)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Implementation != reg.Implementation {
		t.Fatalf("got %q, want %q", got.Implementation, reg.Implementation)
	}
	if err := store.Delete(ctx, reg.Implementation); err != nil {
		t.Fatalf("Delete without KV: %v", err)
	}
}
