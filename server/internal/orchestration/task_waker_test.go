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

// newWakerStore opens a fresh temp-dir SQLite database and returns a Store
// (with cleanup). Mirrors sqliteNativeFactory from the storage conformance
// suite — aetherlite-parity is part of the contract.
func newWakerStore(t *testing.T) (taskstore.Store, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "tasks_waker.db")
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
	return store, cleanup
}

// newWakerService builds a minimal TaskAssignmentService wired to the given
// store. token store, dispatcher, grant service, and session registry are
// all nil — the waker only needs the store + the public state transitions
// on the service, which gracefully tolerate nil collaborators (CompleteTask
// / FailTask / ResumeTask skip token revoke and queue retirement when their
// optional collaborators are unset).
func newWakerService(store taskstore.Store) *TaskAssignmentService {
	return &TaskAssignmentService{
		taskStore: store,
	}
}

// buildRunningTask CreateTask → AssignTask → StartTask, leaving the task in
// running state ready to be paused.
func buildRunningTask(t *testing.T, ctx context.Context, store taskstore.Store, hint string) *tasks.Task {
	t.Helper()
	task := &tasks.Task{
		TaskID:    fmt.Sprintf("waker-%s-%s", hint, uuid.New().String()),
		TaskType:  "waker-test",
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

// TestTaskWaker_DependencyWake covers the canonical case: parent in
// WAITING_DEPENDENCY on a single child; child reaches terminal state via
// the store; scan resumes parent.
func TestTaskWaker_DependencyWake(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	child := buildRunningTask(t, ctx, store, "child")
	parent := buildRunningTask(t, ctx, store, "parent")

	// Park parent on child via the dependency wait spec.
	depSpec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonDependency,
		DependsOn: []string{child.TaskID},
	}
	if err := store.PauseTask(ctx, parent.TaskID, tasks.TaskStatusWaitingDependency, depSpec); err != nil {
		t.Fatalf("PauseTask(parent): %v", err)
	}

	// Complete the child via the store directly (NOT via the service —
	// otherwise CompleteTask would already fan out via wakeDependents and
	// we wouldn't be testing the waker in isolation).
	if err := store.CompleteTask(ctx, child.TaskID); err != nil {
		t.Fatalf("CompleteTask(child): %v", err)
	}

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, parent.TaskID)
	if err != nil {
		t.Fatalf("GetTask(parent): %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("parent status after waker scan: got %q want %q", got.Status, tasks.TaskStatusRunning)
	}
	if got.WaitSpec != nil {
		t.Errorf("expected WaitSpec cleared after resume, got %+v", got.WaitSpec)
	}
}

// TestTaskWaker_WakeOnAny: parent depends on [A, B], only A completes;
// scan wakes parent because WakeOnAny=true.
func TestTaskWaker_WakeOnAny(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	childA := buildRunningTask(t, ctx, store, "childA")
	childB := buildRunningTask(t, ctx, store, "childB")
	parent := buildRunningTask(t, ctx, store, "parent")

	depSpec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonDependency,
		DependsOn: []string{childA.TaskID, childB.TaskID},
		WakeOnAny: true,
	}
	if err := store.PauseTask(ctx, parent.TaskID, tasks.TaskStatusWaitingDependency, depSpec); err != nil {
		t.Fatalf("PauseTask(parent): %v", err)
	}

	// Complete only A.
	if err := store.CompleteTask(ctx, childA.TaskID); err != nil {
		t.Fatalf("CompleteTask(childA): %v", err)
	}

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, parent.TaskID)
	if err != nil {
		t.Fatalf("GetTask(parent): %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("WakeOnAny: parent status got %q want running (only A completed)", got.Status)
	}
}

// TestTaskWaker_WakeAll_BlocksUntilAll: parent depends on [A, B] with
// default semantics (all must be terminal); a scan after only A completes
// must NOT wake the parent.
func TestTaskWaker_WakeAll_BlocksUntilAll(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	childA := buildRunningTask(t, ctx, store, "childA")
	childB := buildRunningTask(t, ctx, store, "childB")
	parent := buildRunningTask(t, ctx, store, "parent")

	depSpec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonDependency,
		DependsOn: []string{childA.TaskID, childB.TaskID},
		// WakeOnAny intentionally false: ALL must complete.
	}
	if err := store.PauseTask(ctx, parent.TaskID, tasks.TaskStatusWaitingDependency, depSpec); err != nil {
		t.Fatalf("PauseTask(parent): %v", err)
	}

	if err := store.CompleteTask(ctx, childA.TaskID); err != nil {
		t.Fatalf("CompleteTask(childA): %v", err)
	}

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, parent.TaskID)
	if err != nil {
		t.Fatalf("GetTask(parent): %v", err)
	}
	if got.Status != tasks.TaskStatusWaitingDependency {
		t.Errorf("Wake-all: parent should still be waiting; got %q", got.Status)
	}

	// Now complete B; the second scan must wake the parent.
	if err := store.CompleteTask(ctx, childB.TaskID); err != nil {
		t.Fatalf("CompleteTask(childB): %v", err)
	}
	waker.scan(ctx)
	got, err = store.GetTask(ctx, parent.TaskID)
	if err != nil {
		t.Fatalf("GetTask(parent) after B: %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("after both deps complete: parent status got %q want running", got.Status)
	}
}

// TestTaskWaker_TimeoutWakeToFail: paused with TimeoutMs=1ms, PausedAt
// already past (set by PauseTask), scan transitions to FAILED.
func TestTaskWaker_TimeoutWakeToFail(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	task := buildRunningTask(t, ctx, store, "timeout")
	spec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonInput,
		TimeoutMs: 1,
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusWaitingInput, spec); err != nil {
		t.Fatalf("PauseTask: %v", err)
	}

	// Sleep just past the timeout window so paused_at + TimeoutMs is in the
	// past on next scan. 10ms is well over the 1ms TimeoutMs and below any
	// reasonable test-runtime floor.
	time.Sleep(10 * time.Millisecond)

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusFailed {
		t.Errorf("timeout: status got %q want failed", got.Status)
	}
	if got.ErrorMessage == "" {
		t.Errorf("timeout: expected non-empty error_message")
	}
}

// TestTaskWaker_ScheduledWake: HIBERNATED with ScheduledWakeUnixMs in the
// past — scan resumes.
func TestTaskWaker_ScheduledWake(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	task := buildRunningTask(t, ctx, store, "hibernated")
	// Past wake time guarantees the scan fires immediately.
	wakeAt := time.Now().Add(-1 * time.Second).UnixMilli()
	spec := &tasks.WaitSpec{
		Reason:              tasks.WaitReasonHibernation,
		ScheduledWakeUnixMs: wakeAt,
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusHibernated, spec); err != nil {
		t.Fatalf("PauseTask: %v", err)
	}

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("scheduled wake: status got %q want running", got.Status)
	}
}

// TestTaskWaker_NoOpWhenNothingPending: with no waiting tasks, scan
// completes without error and without modifying anything.
func TestTaskWaker_NoOpWhenNothingPending(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc)
	// Should be a complete no-op; we just want to confirm no panic / no
	// error escape.
	waker.scan(ctx)
}

// TestTaskWaker_NilTaskService_Skipped guards against a misconfigured
// gateway: a waker constructed with a nil service should silently no-op
// rather than panicking on the first scan.
func TestTaskWaker_NilTaskService_Skipped(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	// Put a task into waiting_dependency so ListWaitingTasks would return
	// something — proving the early-return path triggers before any
	// service call.
	child := buildRunningTask(t, ctx, store, "child")
	parent := buildRunningTask(t, ctx, store, "parent")
	depSpec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonDependency,
		DependsOn: []string{child.TaskID},
	}
	if err := store.PauseTask(ctx, parent.TaskID, tasks.TaskStatusWaitingDependency, depSpec); err != nil {
		t.Fatalf("PauseTask: %v", err)
	}

	waker := NewTaskWaker(store, nil)
	waker.scan(ctx) // must not panic
}

// TestTaskAssignmentService_WakeDependents_EventDriven verifies the
// event-driven path: CompleteTask on a child should resume a parent waiting
// on it WITHOUT the waker scanning, because CompleteTask now fans out via
// wakeDependents internally.
func TestTaskAssignmentService_WakeDependents_EventDriven(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	child := buildRunningTask(t, ctx, store, "child")
	parent := buildRunningTask(t, ctx, store, "parent")

	depSpec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonDependency,
		DependsOn: []string{child.TaskID},
	}
	if err := store.PauseTask(ctx, parent.TaskID, tasks.TaskStatusWaitingDependency, depSpec); err != nil {
		t.Fatalf("PauseTask(parent): %v", err)
	}

	svc := newWakerService(store)
	if err := svc.CompleteTask(ctx, child.TaskID); err != nil {
		t.Fatalf("CompleteTask via service: %v", err)
	}

	got, err := store.GetTask(ctx, parent.TaskID)
	if err != nil {
		t.Fatalf("GetTask(parent): %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("event-driven wake: parent got %q want running", got.Status)
	}
}
