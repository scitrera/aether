package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	taskssqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/pkg/tasks"

	// Register bare "sqlite" driver for the native sqlite backend.
	_ "modernc.org/sqlite"
)

// newReaperStore opens a fresh temp-dir SQLite database and returns a Store
// with cleanup. Mirrors the waker test infra so the reaper guard is exercised
// against the same aetherlite-parity backend.
func newReaperStore(t *testing.T) (taskstore.Store, *sql.DB, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "tasks_reaper.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	store, err := taskssqlite.New(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("taskssqlite.New: %v", err)
	}
	cleanup := func() { _ = db.Close() }
	return store, db, cleanup
}

// forceDisconnectedRow inserts a synthetic disconnected row directly into the
// tasks table so ListDisconnectedTasks's status='running' filter can be
// bypassed for the negative-coverage test. The reaper guard must defensively
// skip waiting/hibernated rows even if such a row somehow appears in the
// listing.
func forceDisconnectedRow(t *testing.T, ctx context.Context, db *sql.DB, taskID string, status tasks.TaskStatus, disconnectedAt time.Time, graceMs int64) {
	t.Helper()
	// Force the status in-place AND ensure disconnected_at is populated. The
	// sqlite schema stores disconnected_at as RFC3339Nano string.
	_, err := db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, disconnected_at = ?, grace_window_ms = ? WHERE task_id = ?`,
		string(status), disconnectedAt.UTC().Format(time.RFC3339Nano), graceMs, taskID,
	)
	if err != nil {
		t.Fatalf("forceDisconnectedRow: %v", err)
	}
}

// listFromTableDirect queries the disconnected rows ignoring the status filter
// so the test can verify what the reaper actually sees. The reaper guard
// covers the case where the row IS listed but status indicates waiting.
func listFromTableDirect(t *testing.T, ctx context.Context, db *sql.DB) []*tasks.Task {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT task_id, status, grace_window_ms, disconnected_at FROM tasks WHERE disconnected_at IS NOT NULL`)
	if err != nil {
		t.Fatalf("listFromTableDirect: %v", err)
	}
	defer rows.Close()
	var out []*tasks.Task
	for rows.Next() {
		var taskID, status, discAt string
		var graceMs int64
		if err := rows.Scan(&taskID, &status, &graceMs, &discAt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		when, perr := time.Parse(time.RFC3339Nano, discAt)
		if perr != nil {
			t.Fatalf("parse disconnected_at: %v", perr)
		}
		out = append(out, &tasks.Task{
			TaskID:         taskID,
			Status:         tasks.TaskStatus(status),
			GraceWindowMs:  graceMs,
			DisconnectedAt: &when,
		})
	}
	return out
}

// buildRunningTaskForReaper transitions a task to running so the reaper's
// disconnect tracking applies. Mirrors the waker test helper but kept local.
func buildRunningTaskForReaper(t *testing.T, ctx context.Context, store taskstore.Store, hint string) *tasks.Task {
	t.Helper()
	task := &tasks.Task{
		TaskID:    fmt.Sprintf("reaper-%s-%s", hint, uuid.New().String()),
		TaskType:  "reaper-test",
		Workspace: "_test",
	}
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask(%s): %v", hint, err)
	}
	if err := store.AssignTask(ctx, task.TaskID, "worker-"+hint); err != nil {
		t.Fatalf("AssignTask(%s): %v", hint, err)
	}
	if err := store.StartTask(ctx, task.TaskID); err != nil {
		t.Fatalf("StartTask(%s): %v", hint, err)
	}
	return task
}

// reaperLivenessAllOffline implements SessionLivenessProbe: no worker is
// considered active. The reaper should proceed with its (now-guarded)
// disconnect-failure logic.
type reaperLivenessAllOffline struct{}

func (reaperLivenessAllOffline) HasActiveSessionForTask(ctx context.Context, taskID string) bool {
	return false
}

// TestDisconnectReaper_SkipsHibernatedTasks verifies the Phase 3 guard: a
// HIBERNATED task whose disconnected_at + grace_window has elapsed is NOT
// failed by the reaper. The worker was released as part of the hibernation
// protocol; the disconnect is a feature, not a failure.
func TestDisconnectReaper_SkipsHibernatedTasks(t *testing.T) {
	store, db, cleanup := newReaperStore(t)
	defer cleanup()
	ctx := context.Background()

	task := buildRunningTaskForReaper(t, ctx, store, "hib")
	// Park the task in HIBERNATED via the store's PauseTask (legal transition
	// from running).
	spec := &tasks.WaitSpec{
		Reason: tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{
			CheckpointKey: "ck-1",
		},
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusHibernated, spec); err != nil {
		t.Fatalf("PauseTask -> HIBERNATED: %v", err)
	}

	// Force a stale disconnected_at and short grace so the reaper would, in
	// absence of the guard, attempt to fail this row. The status is left as
	// HIBERNATED so the IsWaiting guard short-circuits.
	staleWhen := time.Now().Add(-1 * time.Hour)
	forceDisconnectedRow(t, ctx, db, task.TaskID, tasks.TaskStatusHibernated, staleWhen, 1000 /* 1s grace */)

	// Sanity: row is in the table with HIBERNATED status + stale disconnect marker.
	rows := listFromTableDirect(t, ctx, db)
	if len(rows) != 1 || rows[0].TaskID != task.TaskID {
		t.Fatalf("setup: expected one disconnected row for %s, got %+v", task.TaskID, rows)
	}
	if rows[0].Status != tasks.TaskStatusHibernated {
		t.Fatalf("setup: row status = %q, want %q", rows[0].Status, tasks.TaskStatusHibernated)
	}

	// Use a hand-rolled reaper that uses a fake Store wrapper feeding the
	// hibernated row directly, since ListDisconnectedTasks filters status='running'.
	// We exercise the guard by constructing a reaper that bypasses the SQL filter.
	svc := &TaskAssignmentService{taskStore: store}
	reaper := &DisconnectReaper{
		taskStore:   &fixedListStore{Store: store, fixed: rows},
		taskService: svc,
		sessions:    reaperLivenessAllOffline{},
		interval:    10 * time.Second,
		batchLimit:  500,
	}
	reaper.scan(ctx)

	// The hibernated task must NOT have been failed.
	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after reaper scan: %v", err)
	}
	if got.Status != tasks.TaskStatusHibernated {
		t.Errorf("guard failed: task status = %q, want %q (hibernated must not be reaped)", got.Status, tasks.TaskStatusHibernated)
	}
}

// TestDisconnectReaper_SkipsWaitingInputTasks: same defense applied to all
// waiting states. A waiting_input task whose worker has disconnected is also
// in a protocol-allowed disconnect state and must not be failed.
func TestDisconnectReaper_SkipsWaitingInputTasks(t *testing.T) {
	store, db, cleanup := newReaperStore(t)
	defer cleanup()
	ctx := context.Background()

	task := buildRunningTaskForReaper(t, ctx, store, "wi")
	spec := &tasks.WaitSpec{Reason: tasks.WaitReasonInput}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusWaitingInput, spec); err != nil {
		t.Fatalf("PauseTask -> WAITING_INPUT: %v", err)
	}

	staleWhen := time.Now().Add(-1 * time.Hour)
	forceDisconnectedRow(t, ctx, db, task.TaskID, tasks.TaskStatusWaitingInput, staleWhen, 1000)

	rows := listFromTableDirect(t, ctx, db)
	svc := &TaskAssignmentService{taskStore: store}
	reaper := &DisconnectReaper{
		taskStore:   &fixedListStore{Store: store, fixed: rows},
		taskService: svc,
		sessions:    reaperLivenessAllOffline{},
		interval:    10 * time.Second,
		batchLimit:  500,
	}
	reaper.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after reaper scan: %v", err)
	}
	if got.Status != tasks.TaskStatusWaitingInput {
		t.Errorf("guard failed: task status = %q, want %q (waiting_input must not be reaped)", got.Status, tasks.TaskStatusWaitingInput)
	}
}

// TestDisconnectReaper_StillFailsRunningTasks: positive coverage — the guard
// must NOT regress the original behavior. A RUNNING task past its grace
// window is still failed by the reaper.
func TestDisconnectReaper_StillFailsRunningTasks(t *testing.T) {
	store, db, cleanup := newReaperStore(t)
	defer cleanup()
	ctx := context.Background()

	task := buildRunningTaskForReaper(t, ctx, store, "run")

	staleWhen := time.Now().Add(-1 * time.Hour)
	forceDisconnectedRow(t, ctx, db, task.TaskID, tasks.TaskStatusRunning, staleWhen, 1000)

	// For this test, the production ListDisconnectedTasks path returns the
	// row because status='running'. Exercise the real path end-to-end.
	svc := &TaskAssignmentService{taskStore: store}
	reaper := NewDisconnectReaper(store, svc, reaperLivenessAllOffline{})
	reaper.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after reaper scan: %v", err)
	}
	if got.Status != tasks.TaskStatusFailed {
		t.Errorf("running task should be failed past grace window: status = %q, want %q", got.Status, tasks.TaskStatusFailed)
	}
}

// fixedListStore wraps a real Store but overrides ListDisconnectedTasks to
// return a fixed slice. Used by the waiting/hibernated tests because the real
// store's status='running' filter would exclude the rows we want the reaper
// to consider (and then defensively skip via the new IsWaiting guard).
type fixedListStore struct {
	taskstore.Store
	fixed []*tasks.Task
}

func (f *fixedListStore) ListDisconnectedTasks(ctx context.Context, limit int) ([]*tasks.Task, error) {
	return f.fixed, nil
}
