// Package tasks_test contains the cross-backend conformance suite for
// tasks.Store. The same test cases run against every implementation —
// postgres today, sqlite-native once Stage 2 lands. Drift between
// implementations gets caught here.
//
// Per `.slop/20260514_storage_interfaces_stage0.md` §8, the suite is
// table-driven with one subtest per backend. The postgres subtest skips when
// DATABASE_URL / dev infra is unavailable; the sqlite subtest is always
// runnable since it spins up a temp-dir SQLite file.
package tasks_test

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/storage/tasks"
	taskspg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/internal/testutil"
	sqlitemigrations "github.com/scitrera/aether/migrations/sqlite"

	// Connects the legacy tasks package to the *sql.DB for both backends.
	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
)

// storeFactory builds a Store plus the raw *sql.DB handle (needed for the
// transactional RecordAuditEventTx test) and returns a cleanup func. The
// factory may call t.Skip if its prerequisites are unmet — the harness honors
// that and reports the subtest as skipped.
type storeFactory func(t *testing.T) (store tasks.Store, db *sql.DB, cleanup func())

func TestStoreConformance(t *testing.T) {
	backends := []struct {
		name    string
		factory storeFactory
	}{
		{name: "postgres", factory: postgresFactory},
		{name: "sqlite", factory: sqliteFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("LifecycleRoundTrip", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runLifecycleRoundTrip(t, store)
			})
			t.Run("StateTransitions", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runStateTransitions(t, store)
			})
			t.Run("Listing", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runListing(t, store)
			})
			t.Run("PoolClaim", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runPoolClaim(t, store)
			})
			t.Run("AuditEvents", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAuditEvents(t, store)
			})
			t.Run("AuditEventsTx", func(t *testing.T) {
				store, db, cleanup := b.factory(t)
				defer cleanup()
				runAuditEventsTx(t, store, db)
			})
			t.Run("Timers", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runTimers(t, store)
			})
			t.Run("Checkpoints", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runCheckpoints(t, store)
			})
		})
	}
}

// =============================================================================
// Test bodies
// =============================================================================

// runLifecycleRoundTrip: CreateTask → GetTask returns it → UpdateTaskStatus
// → GetTask shows the new status.
func runLifecycleRoundTrip(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "lifecycle")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after create: %v", err)
	}
	if got.TaskID != task.TaskID {
		t.Fatalf("GetTask returned wrong id: got %q want %q", got.TaskID, task.TaskID)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Fatalf("expected status %q after create, got %q", tasks.TaskStatusPending, got.Status)
	}

	if err := store.UpdateTaskStatus(ctx, task.TaskID, tasks.TaskStatusRunning); err != nil {
		t.Fatalf("UpdateTaskStatus: %v", err)
	}
	got2, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after status update: %v", err)
	}
	if got2.Status != tasks.TaskStatusRunning {
		t.Fatalf("expected status %q after update, got %q", tasks.TaskStatusRunning, got2.Status)
	}
}

// runStateTransitions: CreateTask → AssignTask → StartTask → CompleteTask,
// each step verified by a GetTask.
func runStateTransitions(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "transitions")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if err := store.AssignTask(ctx, task.TaskID, "worker-1"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	assertStatus(t, store, task.TaskID, tasks.TaskStatusAssigned)

	if err := store.StartTask(ctx, task.TaskID); err != nil {
		t.Fatalf("StartTask: %v", err)
	}
	assertStatus(t, store, task.TaskID, tasks.TaskStatusRunning)

	if err := store.CompleteTask(ctx, task.TaskID); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	assertStatus(t, store, task.TaskID, tasks.TaskStatusCompleted)
}

// runListing: CreateTask N=3 with the same workspace tag → ListTasks scoped
// to that workspace returns 3 → GetTasksByStatus(pending) returns those 3.
func runListing(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	workspace := fmt.Sprintf("ws-listing-%d", time.Now().UnixNano())
	const n = 3
	for i := 0; i < n; i++ {
		task := newTestTask(t, fmt.Sprintf("list-%d", i))
		task.Workspace = workspace
		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask[%d]: %v", i, err)
		}
	}

	got, err := store.ListTasks(ctx, &tasks.TaskFilter{Workspace: workspace, Limit: 10})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(got) != n {
		t.Fatalf("ListTasks: got %d rows want %d", len(got), n)
	}

	pending := tasks.TaskStatusPending
	gotByStatus, err := store.ListTasks(ctx, &tasks.TaskFilter{
		Workspace: workspace,
		Status:    &pending,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListTasks(status=pending): %v", err)
	}
	if len(gotByStatus) != n {
		t.Fatalf("ListTasks(status=pending): got %d rows want %d", len(gotByStatus), n)
	}
}

// runPoolClaim: CreateTask in pool mode → ClaimPoolTask succeeds the first
// time → ClaimPoolTask fails (claimed=false) the second time.
func runPoolClaim(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "pool")
	task.AssignmentMode = tasks.AssignmentModePool
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask(pool): %v", err)
	}

	claimed, err := store.ClaimPoolTask(ctx, task.TaskID, "worker-A")
	if err != nil {
		t.Fatalf("ClaimPoolTask first call: %v", err)
	}
	if !claimed {
		t.Fatalf("ClaimPoolTask first call: expected claimed=true")
	}

	claimedAgain, err := store.ClaimPoolTask(ctx, task.TaskID, "worker-B")
	if err != nil {
		t.Fatalf("ClaimPoolTask second call: %v", err)
	}
	if claimedAgain {
		t.Fatalf("ClaimPoolTask second call: expected claimed=false (already claimed)")
	}
}

// runAuditEvents: RecordAuditEvent then GetTaskAuditEvents returns it.
func runAuditEvents(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "audit")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	event := &tasks.TaskAuditEvent{
		TaskID:    task.TaskID,
		EventType: tasks.EventTypeCreated,
		EventData: map[string]interface{}{"hint": "conformance"},
		CreatedBy: "test",
	}
	if err := store.RecordAuditEvent(ctx, event); err != nil {
		t.Fatalf("RecordAuditEvent: %v", err)
	}

	events, err := store.GetTaskAuditEvents(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTaskAuditEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("GetTaskAuditEvents: got %d rows want 1", len(events))
	}
	if events[0].EventType != tasks.EventTypeCreated {
		t.Fatalf("unexpected event_type: got %q want %q", events[0].EventType, tasks.EventTypeCreated)
	}
}

// runAuditEventsTx: BeginTx, taskStore.RecordAuditEventTx, tx.Commit,
// GetTaskAuditEvents shows the event. Verifies the *sql.Tx-flavored
// interface method works end-to-end through the new interface.
func runAuditEventsTx(t *testing.T, store tasks.Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "audit-tx")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	event := &tasks.TaskAuditEvent{
		TaskID:    task.TaskID,
		EventType: tasks.EventTypeAssigned,
		EventData: map[string]interface{}{"hint": "conformance-tx"},
		CreatedBy: "test",
	}
	if err := store.RecordAuditEventTx(ctx, tx, event); err != nil {
		_ = tx.Rollback()
		t.Fatalf("RecordAuditEventTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}

	events, err := store.GetTaskAuditEvents(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTaskAuditEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("GetTaskAuditEvents (post-tx): got %d rows want 1", len(events))
	}
	if events[0].EventType != tasks.EventTypeAssigned {
		t.Fatalf("unexpected event_type: got %q want %q", events[0].EventType, tasks.EventTypeAssigned)
	}
}

// runTimers: CreateTimer → GetTimer → GetPendingTimers → MarkTimerFired →
// DeleteTimer.
func runTimers(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "timers")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	firesAt := time.Now().Add(-1 * time.Minute) // already due
	timer := &tasks.TimerRecord{
		TaskID:    task.TaskID,
		TimerType: tasks.TimerTypeHeartbeat,
		FiresAt:   firesAt,
		Metadata:  map[string]interface{}{"hint": "conformance"},
	}
	if err := store.CreateTimer(ctx, timer); err != nil {
		t.Fatalf("CreateTimer: %v", err)
	}
	if timer.TimerID == "" {
		t.Fatalf("CreateTimer did not stamp TimerID")
	}

	got, err := store.GetTimer(ctx, timer.TimerID)
	if err != nil {
		t.Fatalf("GetTimer: %v", err)
	}
	if got.TaskID != task.TaskID {
		t.Fatalf("GetTimer: task_id mismatch got %q want %q", got.TaskID, task.TaskID)
	}

	pending, err := store.GetPendingTimers(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("GetPendingTimers: %v", err)
	}
	if !containsTimer(pending, timer.TimerID) {
		t.Fatalf("GetPendingTimers: created timer %s not returned", timer.TimerID)
	}

	if err := store.MarkTimerFired(ctx, timer.TimerID); err != nil {
		t.Fatalf("MarkTimerFired: %v", err)
	}
	pendingAfter, err := store.GetPendingTimers(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("GetPendingTimers (post-fire): %v", err)
	}
	if containsTimer(pendingAfter, timer.TimerID) {
		t.Fatalf("GetPendingTimers (post-fire): fired timer %s still returned", timer.TimerID)
	}

	if err := store.DeleteTimer(ctx, timer.TimerID); err != nil {
		t.Fatalf("DeleteTimer: %v", err)
	}
}

// runCheckpoints: CreateCheckpoint → GetLatestCheckpoint returns it with the
// expected sequence number and payload.
func runCheckpoints(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "checkpoints")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	cp := &tasks.CheckpointRecord{
		TaskID:         task.TaskID,
		SequenceNumber: 1,
		CheckpointData: map[string]interface{}{"step": "one"},
		CreatedBy:      "test",
	}
	if err := store.CreateCheckpoint(ctx, cp); err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	got, err := store.GetLatestCheckpoint(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetLatestCheckpoint: %v", err)
	}
	if got.SequenceNumber != 1 {
		t.Fatalf("GetLatestCheckpoint: sequence_number got %d want 1", got.SequenceNumber)
	}
	if got.CheckpointData["step"] != "one" {
		t.Fatalf("GetLatestCheckpoint: payload step got %v want %q", got.CheckpointData["step"], "one")
	}
}

// =============================================================================
// Backend factories
// =============================================================================

// postgresFactory connects to the dev postgres instance via testutil. Skips
// when the dev infra isn't reachable.
func postgresFactory(t *testing.T) (tasks.Store, *sql.DB, func()) {
	t.Helper()
	testDB, cleanupDB := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, nil, func() {}
	}

	store := taskspg.New(testDB.DB)
	cleanup := func() {
		cleanupDB()
	}
	return store, testDB.DB, cleanup
}

// sqliteFactory opens a fresh temp-dir SQLite database via the sqlite_compat
// driver (so dbcompat handles the postgres-flavored SQL the legacy task store
// still emits in Stage 1), runs the sqlite migration set, and constructs a
// postgres-impl tasks.Store on top of that handle.
func sqliteFactory(t *testing.T) (tasks.Store, *sql.DB, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "tasks.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	// Single-writer pool to match aetherlite's aether.db semantics and avoid
	// SQLITE_BUSY in WAL mode.
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := applySQLiteMigrationsForTest(ctx, db, sqlitemigrations.MigrationFS); err != nil {
		_ = db.Close()
		t.Fatalf("apply sqlite migrations: %v", err)
	}

	store := taskspg.New(db)
	cleanup := func() {
		_ = db.Close()
	}
	return store, db, cleanup
}

// applySQLiteMigrationsForTest is a test-local copy of the
// cmd/aetherlite/main.go helper. We duplicate it here (rather than importing)
// because the conformance package shouldn't pull on cmd/* code just for
// migration plumbing.
func applySQLiteMigrationsForTest(ctx context.Context, db *sql.DB, fs embed.FS) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read embed fs: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version,
		).Scan(&count); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}
		content, err := fs.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("exec %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	return nil
}

// =============================================================================
// Helpers
// =============================================================================

// newTestTask builds a minimal valid Task. The TaskID is fresh per call so
// rows do not collide with prior runs or other concurrent tests sharing the
// dev database.
func newTestTask(t *testing.T, hint string) *tasks.Task {
	t.Helper()
	return &tasks.Task{
		TaskID:    fmt.Sprintf("conf-%s-%s-%s", hint, t.Name(), uuid.New().String()),
		TaskType:  "conformance",
		Workspace: "_test",
	}
}

func assertStatus(t *testing.T, store tasks.Store, taskID string, want tasks.TaskStatus) {
	t.Helper()
	got, err := store.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatalf("GetTask(%s): %v", taskID, err)
	}
	if got.Status != want {
		t.Fatalf("task %s: status got %q want %q", taskID, got.Status, want)
	}
}

func containsTimer(timers []*tasks.TimerRecord, timerID string) bool {
	for _, tm := range timers {
		if tm.TimerID == timerID {
			return true
		}
	}
	return false
}
