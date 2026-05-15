// Package audit_test contains the cross-backend conformance suite for
// audit.Store. The same test cases run against every implementation —
// postgres today, sqlite-native once Stage 2 lands. Drift between
// implementations gets caught here.
//
// Per `.slop/20260514_storage_interfaces_stage0.md` §8, the suite is
// table-driven with one subtest per backend. The postgres subtest skips
// when DATABASE_URL / dev infra is unavailable; the sqlite subtest is
// always runnable since it spins up a temp-dir SQLite file.
package audit_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/storage/audit"
	auditpg "github.com/scitrera/aether/internal/storage/audit/postgres"
	auditsqlite "github.com/scitrera/aether/internal/storage/audit/sqlite"
	"github.com/scitrera/aether/internal/testutil"
)

// storeFactory builds a Store and returns a cleanup func. The factory may
// call t.Skip if its prerequisites are unmet (e.g. postgres dev infra not
// running) — the harness honors that and reports the subtest as skipped.
type storeFactory func(t *testing.T) (store audit.Store, supportsCleanup bool, cleanup func())

func TestStoreConformance(t *testing.T) {
	backends := []struct {
		name    string
		factory storeFactory
	}{
		{name: "postgres", factory: postgresFactory},
		{name: "sqlite_native", factory: sqliteNativeFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("LogEventSync_then_Query", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runLogEventSyncRoundTrip(t, store)
			})
			t.Run("LogEvent_async_then_Query", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runLogEventAsyncRoundTrip(t, store)
			})
			t.Run("Close_is_idempotent", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runCloseIdempotent(t, store)
			})
			t.Run("CleanupOldLogs", func(t *testing.T) {
				store, supportsCleanup, cleanup := b.factory(t)
				defer cleanup()
				if !supportsCleanup {
					// SQLite Stage 1 path: legacy CleanupOldLogs invokes a
					// PG stored function (cleanup_old_comprehensive_audit_logs)
					// that does not exist in the SQLite migration set. This
					// gap is intentional and gets closed in Stage 2 when the
					// sqlite-native impl ships with its own parameterized
					// DELETE. The interface contract is unchanged.
					t.Skip("CleanupOldLogs not implemented on this backend until Stage 2 (PG-function dependency)")
				}
				runCleanupOldLogs(t, store)
			})
		})
	}
}

// runLogEventSyncRoundTrip verifies that an event written via LogEventSync
// is immediately readable via QueryAuditLog.
func runLogEventSyncRoundTrip(t *testing.T, store audit.Store) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTag(t, "sync")
	ev := newTestEvent(tag, "conformance-sync")
	if err := store.LogEventSync(ctx, ev); err != nil {
		t.Fatalf("LogEventSync: %v", err)
	}

	got := queryByActorID(t, store, tag)
	if len(got) != 1 {
		t.Fatalf("expected 1 event for actor %s, got %d", tag, len(got))
	}
	if got[0].Operation != "conformance-sync" {
		t.Fatalf("unexpected operation: got %q want %q", got[0].Operation, "conformance-sync")
	}
}

// runLogEventAsyncRoundTrip verifies that an event enqueued via LogEvent
// becomes visible after the batched writer flushes.
func runLogEventAsyncRoundTrip(t *testing.T, store audit.Store) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTag(t, "async")
	ev := newTestEvent(tag, "conformance-async")
	store.LogEvent(ctx, ev)

	// Poll for visibility — the async writer flushes either when the batch
	// fills or after FlushPeriod (default 5s). Calling Close() forces a
	// drain, which is faster and deterministic for tests. We can't call
	// Close here because the factory's cleanup will, and Close is one-shot
	// per the contract; so poll with a generous deadline.
	deadline := time.Now().Add(8 * time.Second)
	var got []*audit.Event
	for time.Now().Before(deadline) {
		got = queryByActorID(t, store, tag)
		if len(got) >= 1 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 async event for actor %s after poll window, got %d", tag, len(got))
	}
	if got[0].Operation != "conformance-async" {
		t.Fatalf("unexpected operation: got %q want %q", got[0].Operation, "conformance-async")
	}
}

// runCloseIdempotent verifies Close can be called twice without panic or
// error. The factory's cleanup will Close again, which is the third call.
func runCloseIdempotent(t *testing.T, store audit.Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// runCleanupOldLogs verifies CleanupOldLogs removes entries older than the
// retention window and leaves recent entries intact. Currently only the
// postgres backend supports this; sqlite returns supportsCleanup=false.
func runCleanupOldLogs(t *testing.T, store audit.Store) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTag(t, "cleanup")

	// Recent event — within retention window, must survive.
	recent := newTestEvent(tag+"-recent", "conformance-cleanup-recent")
	recent.Timestamp = time.Now()
	if err := store.LogEventSync(ctx, recent); err != nil {
		t.Fatalf("LogEventSync recent: %v", err)
	}

	// Old event — well outside retention window, must be deleted.
	old := newTestEvent(tag+"-old", "conformance-cleanup-old")
	old.Timestamp = time.Now().Add(-100 * 24 * time.Hour) // 100 days ago
	if err := store.LogEventSync(ctx, old); err != nil {
		t.Fatalf("LogEventSync old: %v", err)
	}

	// Retain 30 days — old event drops out, recent stays.
	deleted, err := store.CleanupOldLogs(ctx, 30)
	if err != nil {
		t.Fatalf("CleanupOldLogs: %v", err)
	}
	if deleted < 1 {
		t.Fatalf("expected at least 1 deleted row, got %d", deleted)
	}

	recentRows := queryByActorID(t, store, tag+"-recent")
	if len(recentRows) != 1 {
		t.Fatalf("recent event was incorrectly cleaned up: got %d rows want 1", len(recentRows))
	}
	oldRows := queryByActorID(t, store, tag+"-old")
	if len(oldRows) != 0 {
		t.Fatalf("old event was not cleaned up: got %d rows want 0", len(oldRows))
	}
}

// =============================================================================
// Backend factories
// =============================================================================

// postgresFactory connects to the dev postgres instance via testutil.
// Skips when the dev infra isn't reachable.
func postgresFactory(t *testing.T) (audit.Store, bool, func()) {
	t.Helper()
	testDB, cleanupDB := testutil.SetupTestDB(t)
	if testDB == nil {
		// SetupTestDB calls t.Skip on its own when infra is unavailable;
		// if we reach here with nil, just bail.
		return nil, false, func() {}
	}

	gatewayID := fmt.Sprintf("conformance-gw-%d", time.Now().UnixNano())
	cfg := audit.DefaultConfig()
	cfg.BatchSize = 1                       // flush immediately so async tests don't wait
	cfg.FlushPeriod = 50 * time.Millisecond // tight flush window
	cfg.ChannelBuffer = 16
	store := auditpg.New(testDB.DB, gatewayID, cfg)

	cleanup := func() {
		// Close the logger first so its writer goroutine exits before the
		// test DB handle is torn down by SetupTestDB's cleanup.
		_ = store.Close()
		cleanupDB()
	}
	return store, true, cleanup
}

// sqliteNativeFactory opens a fresh temp-dir SQLite database using the
// native audit.sqlite.Store implementation (Stage 2). This uses the bare
// "sqlite" driver (modernc.org/sqlite) directly — no dbcompat translation.
// The native impl handles its own migrations, timestamp formatting, and
// boolean-to-integer conversion.
//
// CleanupOldLogs IS supported — the native impl uses a parameterized
// DELETE instead of the PG stored function.
func sqliteNativeFactory(t *testing.T) (audit.Store, bool, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "audit_native.db")
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}

	cfg := audit.DefaultConfig()
	cfg.BatchSize = 1                       // flush immediately so async tests don't wait
	cfg.FlushPeriod = 50 * time.Millisecond // tight flush window
	cfg.ChannelBuffer = 16

	gatewayID := fmt.Sprintf("conformance-gw-%d", time.Now().UnixNano())
	store, err := auditsqlite.New(db, gatewayID, cfg)
	if err != nil {
		_ = db.Close()
		t.Fatalf("auditsqlite.New: %v", err)
	}

	cleanup := func() {
		_ = store.Close()
		_ = db.Close()
	}
	// supportsCleanup=true: native impl has parameterized DELETE.
	return store, true, cleanup
}

// =============================================================================
// Helpers
// =============================================================================

// uniqueTag returns a per-test-per-call actor identifier that won't collide
// with rows other tests inserted into the shared dev database.
func uniqueTag(t *testing.T, hint string) string {
	t.Helper()
	return fmt.Sprintf("conformance-%s-%s-%d", hint, t.Name(), time.Now().UnixNano())
}

// newTestEvent constructs a minimal valid AuditEvent. The actor ID doubles
// as the row's unique selector for QueryAuditLog assertions.
func newTestEvent(actorID, operation string) *audit.Event {
	return &audit.Event{
		EventType:       audit.EventTypeAuth,
		ActorType:       "test",
		ActorID:         actorID,
		SubjectType:     "test",
		SubjectID:       actorID,
		RootSubjectType: "test",
		RootSubjectID:   actorID,
		AuthorityMode:   audit.AuthorityModeDirect,
		ResourceType:    audit.ResourceTypeWorkspace,
		ResourceID:      "_test",
		Operation:       operation,
		Workspace:       "_test",
		SessionID:       uuid.New(),
		Success:         true,
		Source:          audit.SourceGateway,
		Metadata:        map[string]interface{}{"hint": "conformance"},
	}
}

// queryByActorID is a tiny shim around QueryAuditLog that filters to the
// one actor under test so we don't trip over rows from prior runs or other
// concurrent tests sharing the dev database.
func queryByActorID(t *testing.T, store audit.Store, actorID string) []*audit.Event {
	t.Helper()
	ctx := context.Background()
	got, err := store.QueryAuditLog(ctx, audit.EventFilter{ActorID: actorID, Limit: 10})
	if err != nil {
		t.Fatalf("QueryAuditLog: %v", err)
	}
	return got
}
