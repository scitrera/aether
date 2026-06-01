package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/workflow"
)

// TestBuildEmbeddedWorkflowStore_SelectsSQLiteByDefault verifies that with no
// Postgres host configured the embedded engine opens the local SQLite store
// (the zero-dependency single-node default).
func TestBuildEmbeddedWorkflowStore_SelectsSQLiteByDefault(t *testing.T) {
	wfCfg := &workflow.Config{}
	wfCfg.SQLite.Path = filepath.Join(t.TempDir(), "workflow.db")

	store, closeFn, err := buildEmbeddedWorkflowStore(context.Background(), wfCfg)
	if err != nil {
		t.Fatalf("buildEmbeddedWorkflowStore (sqlite default): %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil SQLite store")
	}
	if closeFn == nil {
		t.Fatal("expected non-nil close func")
	}
	closeFn()
}

// TestBuildEmbeddedWorkflowStore_SelectsPostgresWhenHostSet verifies that
// setting a Postgres host routes to the Postgres branch. We point at an
// unreachable address so the connect/ping fails fast — the returned error
// proves the Postgres path was taken rather than silently falling back to
// SQLite.
func TestBuildEmbeddedWorkflowStore_SelectsPostgresWhenHostSet(t *testing.T) {
	wfCfg := &workflow.Config{}
	wfCfg.SQLite.Path = filepath.Join(t.TempDir(), "workflow.db") // present but must be ignored
	wfCfg.Postgres.Host = "127.0.0.1"
	wfCfg.Postgres.Port = 1 // nothing listens here
	wfCfg.Postgres.Database = "wf"
	wfCfg.Postgres.User = "wf"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, closeFn, err := buildEmbeddedWorkflowStore(ctx, wfCfg)
	if err == nil {
		if closeFn != nil {
			closeFn()
		}
		t.Fatal("expected an error connecting to the unreachable Postgres, got nil (did it fall back to SQLite?)")
	}
	if store != nil {
		t.Fatal("expected nil store on Postgres connect failure")
	}
}
