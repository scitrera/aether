package cleanup

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/state"
	sqlitetasks "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// newReproBadgerRegistry stands up a fresh Badger-backed session registry
// (the aetherlite-standalone leader-election backend) in a temp dir.
func newReproBadgerRegistry(t *testing.T) *state.BadgerSessionRegistry {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return state.NewBadgerSessionRegistry(db)
}

// newReproSQLiteStore stands up a native-SQLite task store (the aetherlite
// task backend). Returns both the store and the raw *sql.DB so the test can
// backdate created_at to simulate a month-old task.
func newReproSQLiteStore(t *testing.T) (*sqlitetasks.Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	store, err := sqlitetasks.New(db)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return store, db
}

// insertStaleStartupTask inserts a pending agent_startup pool task and backdates
// its created_at to `age` ago, reproducing the month-old unclaimed startup task
// observed on the live aetherlite deployment.
func insertStaleStartupTask(t *testing.T, ctx context.Context, store *sqlitetasks.Store, db *sql.DB, id string, age time.Duration) {
	t.Helper()
	task := &tasks.Task{
		TaskID:               id,
		TaskType:             "agent_startup",
		Workspace:            "default",
		AssignmentMode:       tasks.AssignmentModePool,
		TaskCategory:         tasks.TaskCategoryOrchestrated,
		TargetImplementation: "sahara",
		Status:               tasks.TaskStatusPending,
	}
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("create stale startup task: %v", err)
	}
	old := time.Now().Add(-age).UTC().Format(time.RFC3339Nano)
	if _, err := db.ExecContext(ctx, "UPDATE tasks SET created_at = ? WHERE task_id = ?", old, id); err != nil {
		t.Fatalf("backdate created_at: %v", err)
	}
}

// waitForStatus polls the store until the task reaches `want` or the deadline
// elapses. Returns the final observed status.
func waitForStatus(t *testing.T, ctx context.Context, store *sqlitetasks.Store, id string, want tasks.TaskStatus, timeout time.Duration) tasks.TaskStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last tasks.TaskStatus
	for time.Now().Before(deadline) {
		got, err := store.GetTask(ctx, id)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if got != nil {
			last = got.Status
			if got.Status == want {
				return got.Status
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last
}

// TestStartBackground_Badger_RunsStartupSweep is the reproduction test for the
// aetherlite-standalone "cleanup never runs" failure. It stands up the exact
// single-node backends (Badger session registry + SQLite task store), seeds a
// month-old pending agent_startup task, starts the background cleanup runner,
// and asserts the stale startup sweep actually cancels it within a bounded time.
func TestStartBackground_Badger_RunsStartupSweep(t *testing.T) {
	ctx := context.Background()
	reg := newReproBadgerRegistry(t)
	store, db := newReproSQLiteStore(t)
	insertStaleStartupTask(t, ctx, store, db, "stale-startup-1", 40*24*time.Hour)

	tas := orchestration.NewTaskAssignmentService(store, nil, reg, nil, nil)

	cfg := &Config{
		// Only the startup sweep is enabled; short intervals so the test is fast.
		StartupTaskTTL:              30 * time.Minute,
		StartupTaskCancelInterval:   50 * time.Millisecond,
		LeaderElectionRetryInterval: 20 * time.Millisecond,
	}
	svc := NewService(store, tas, reg, cfg)

	runner := svc.StartBackground(ctx)
	defer runner.Stop()

	got := waitForStatus(t, ctx, store, "stale-startup-1", tasks.TaskStatusCancelled, 3*time.Second)
	if got != tasks.TaskStatusCancelled {
		t.Fatalf("stale startup task was not cancelled by background sweep; final status = %q (want %q)", got, tasks.TaskStatusCancelled)
	}
}

// TestStartBackground_Badger_ForeignLeaderLock_StarvesSweep pins the exact
// production failure mode: when the cleanup leader lock is already held by some
// other holder (a stuck/foreign lease), AcquireLock returns (false,nil)
// perpetually for the runner, startCleanupJobs never fires, and NONE of the
// sweeps run — the stale startup task lingers forever with zero cleanup logs.
func TestStartBackground_Badger_ForeignLeaderLock_StarvesSweep(t *testing.T) {
	ctx := context.Background()
	reg := newReproBadgerRegistry(t)
	store, db := newReproSQLiteStore(t)
	insertStaleStartupTask(t, ctx, store, db, "stale-startup-2", 40*24*time.Hour)

	// Simulate a stuck/foreign holder occupying the cleanup leader lock.
	leaderID := models.CleanupLeaderIdentity()
	acquired, err := reg.AcquireLock(ctx, leaderID, "foreign-session")
	if err != nil {
		t.Fatalf("seed foreign leader lock: %v", err)
	}
	if !acquired {
		t.Fatal("expected to seed foreign leader lock")
	}

	tas := orchestration.NewTaskAssignmentService(store, nil, reg, nil, nil)
	cfg := &Config{
		StartupTaskTTL:              30 * time.Minute,
		StartupTaskCancelInterval:   50 * time.Millisecond,
		LeaderElectionRetryInterval: 20 * time.Millisecond,
	}
	svc := NewService(store, tas, reg, cfg)

	runner := svc.StartBackground(ctx)
	defer runner.Stop()

	// With leader-election gating (SingleNode=false), the runner's AcquireLock
	// returns (false,nil) for the whole window because the foreign lease holds the
	// lock (LockTTL=30s >> window). startCleanupJobs never fires, so the stale
	// task stays pending. This is the exact production failure mode.
	got := waitForStatus(t, ctx, store, "stale-startup-2", tasks.TaskStatusCancelled, 800*time.Millisecond)
	if got != tasks.TaskStatusPending {
		t.Fatalf("expected stale-startup-2 to remain pending (starved by foreign leader lock), got %q", got)
	}
}

// TestStartBackground_Badger_SingleNode_RunsSweepDespiteForeignLock verifies the
// fix: with SingleNode=true, the sweeps run DIRECTLY without leader-election
// gating, so a stuck/foreign leader lease can no longer starve them. The stale
// startup task is cancelled within a bounded time even though the leader lock is
// held by another session.
func TestStartBackground_Badger_SingleNode_RunsSweepDespiteForeignLock(t *testing.T) {
	ctx := context.Background()
	reg := newReproBadgerRegistry(t)
	store, db := newReproSQLiteStore(t)
	insertStaleStartupTask(t, ctx, store, db, "stale-startup-3", 40*24*time.Hour)

	// Hold the cleanup leader lock with a foreign session — must NOT matter now.
	leaderID := models.CleanupLeaderIdentity()
	if acquired, err := reg.AcquireLock(ctx, leaderID, "foreign-session"); err != nil || !acquired {
		t.Fatalf("seed foreign leader lock: acquired=%v err=%v", acquired, err)
	}

	tas := orchestration.NewTaskAssignmentService(store, nil, reg, nil, nil)
	cfg := &Config{
		SingleNode:                true, // the fix: run directly, no election gate
		StartupTaskTTL:            30 * time.Minute,
		StartupTaskCancelInterval: 50 * time.Millisecond,
	}
	svc := NewService(store, tas, reg, cfg)

	runner := svc.StartBackground(ctx)
	defer runner.Stop()

	got := waitForStatus(t, ctx, store, "stale-startup-3", tasks.TaskStatusCancelled, 3*time.Second)
	if got != tasks.TaskStatusCancelled {
		t.Fatalf("single-node sweep did not cancel stale startup task despite foreign leader lock; final status = %q", got)
	}
}

// TestCancelStalePoolTasks_SweepsOnlyStaleRegularPool exercises the new pool
// sweep end-to-end against the native SQLite store: a stale regular pool task is
// cancelled, while a fresh regular pool task and a stale agent_startup pool task
// (owned by the startup sweep) are left untouched.
func TestCancelStalePoolTasks_SweepsOnlyStaleRegularPool(t *testing.T) {
	ctx := context.Background()
	store, db := newReproSQLiteStore(t)

	// Stale regular pool task — should be cancelled.
	stalePool := &tasks.Task{
		TaskID:               "pool-stale",
		TaskType:             "some_pool_job",
		Workspace:            "default",
		AssignmentMode:       tasks.AssignmentModePool,
		TaskCategory:         tasks.TaskCategoryRegular,
		TargetImplementation: "worker",
		Status:               tasks.TaskStatusPending,
	}
	if err := store.CreateTask(ctx, stalePool); err != nil {
		t.Fatalf("create stale pool task: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	if _, err := db.ExecContext(ctx, "UPDATE tasks SET created_at = ? WHERE task_id = ?", old, "pool-stale"); err != nil {
		t.Fatalf("backdate pool-stale: %v", err)
	}

	// Fresh regular pool task — should be left pending (younger than TTL).
	freshPool := &tasks.Task{
		TaskID:               "pool-fresh",
		TaskType:             "some_pool_job",
		Workspace:            "default",
		AssignmentMode:       tasks.AssignmentModePool,
		TaskCategory:         tasks.TaskCategoryRegular,
		TargetImplementation: "worker",
		Status:               tasks.TaskStatusPending,
	}
	if err := store.CreateTask(ctx, freshPool); err != nil {
		t.Fatalf("create fresh pool task: %v", err)
	}

	// Stale agent_startup pool task (orchestrated) — owned by the startup sweep,
	// must be excluded by the regular-category filter.
	insertStaleStartupTask(t, ctx, store, db, "startup-stale", 40*24*time.Hour)

	tas := orchestration.NewTaskAssignmentService(store, nil, nil, nil, nil)
	n, err := tas.CancelStalePoolTasks(ctx, 1*time.Hour)
	if err != nil {
		t.Fatalf("CancelStalePoolTasks: %v", err)
	}
	if n != 1 {
		t.Fatalf("CancelStalePoolTasks cancelled %d tasks, want 1", n)
	}

	assertStatus := func(id string, want tasks.TaskStatus) {
		got, err := store.GetTask(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got.Status != want {
			t.Errorf("%s status = %q, want %q", id, got.Status, want)
		}
	}
	assertStatus("pool-stale", tasks.TaskStatusCancelled)
	assertStatus("pool-fresh", tasks.TaskStatusPending)
	assertStatus("startup-stale", tasks.TaskStatusPending)
}
