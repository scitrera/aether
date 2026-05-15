package workflow

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
)

// TestNewServer_legacyConstructor verifies the backward-compatible
// constructor builds a Server that owns its DB handle. This is the
// existing code path — no behavioral change, just confirming it still
// compiles and returns a valid Server.
func TestNewServer_legacyConstructor(t *testing.T) {
	cfg := &Config{
		Mode: ModeLite,
		SQLite: SQLiteConfig{
			Path: filepath.Join(t.TempDir(), "workflow.db"),
		},
	}

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv == nil {
		t.Fatal("NewServer returned nil Server")
	}
	if !srv.ownsDB {
		t.Fatal("NewServer should set ownsDB=true")
	}
	if srv.store != nil {
		t.Fatal("NewServer should not pre-populate store (set during Run)")
	}
}

// TestNewServerWithStore_rejectsNilStore verifies that passing nil
// triggers an error per §14.1 (nil Store is a class-A crash).
func TestNewServerWithStore_rejectsNilStore(t *testing.T) {
	cfg := &Config{Mode: ModeLite}
	srv, err := NewServerWithStore(cfg, nil)
	if err == nil {
		t.Fatal("NewServerWithStore(nil) should return an error")
	}
	if srv != nil {
		t.Fatal("NewServerWithStore(nil) should return nil Server")
	}
}

// TestNewServerWithStore_legacyStore verifies injection with the legacy
// *Store type (which satisfies WorkflowStore via structural typing).
// This uses sqlite_compat, avoiding the import cycle with the native
// sqlite package.
func TestNewServerWithStore_legacyStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	store := NewStore(db, true)

	cfg := &Config{
		Mode: ModeLite,
		SQLite: SQLiteConfig{
			Path: dbPath,
		},
	}

	srv, err := NewServerWithStore(cfg, store)
	if err != nil {
		t.Fatalf("NewServerWithStore: %v", err)
	}
	if srv == nil {
		t.Fatal("NewServerWithStore returned nil Server")
	}
	if srv.ownsDB {
		t.Fatal("NewServerWithStore should set ownsDB=false")
	}
	if srv.store == nil {
		t.Fatal("NewServerWithStore should pre-populate store")
	}
}

// TestNewServerWithStore_initComponentsPreservesInjected verifies that
// initComponents does NOT overwrite a pre-injected store.
func TestNewServerWithStore_initComponentsPreservesInjected(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	store := NewStore(db, true)

	cfg := &Config{
		Mode: ModeLite,
		SQLite: SQLiteConfig{
			Path: dbPath,
		},
		Aether: AetherConfig{
			Implementation: "test-impl",
			Workspace:      "_system",
		},
	}

	srv, err := NewServerWithStore(cfg, store)
	if err != nil {
		t.Fatalf("NewServerWithStore: %v", err)
	}

	// initComponents should NOT overwrite the injected store.
	srv.initComponents()

	if srv.store != store {
		t.Fatal("initComponents overwrote the injected store — expected it to be preserved")
	}
}

// TestNewServer_initComponentsCreatesLegacyStore verifies that
// initComponents constructs the legacy store when none was injected.
func TestNewServer_initComponentsCreatesLegacyStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	cfg := &Config{
		Mode: ModeLite,
		SQLite: SQLiteConfig{
			Path: dbPath,
		},
		Aether: AetherConfig{
			Implementation: "test-impl",
			Workspace:      "_system",
		},
	}

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Simulate what Run() does before initComponents.
	srv.db = db

	srv.initComponents()

	if srv.store == nil {
		t.Fatal("initComponents did not populate store for legacy (ownsDB=true) path")
	}
}
