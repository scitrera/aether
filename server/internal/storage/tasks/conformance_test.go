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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/storage/tasks"
	taskspg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	taskssqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/internal/testutil"

	// Register bare "sqlite" driver for the native sqlite backend.
	_ "modernc.org/sqlite"
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
		{name: "sqlite_native", factory: sqliteNativeFactory},
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
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAuditEventsTx(t, store)
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
			t.Run("QueuePollAndClaim", func(t *testing.T) {
				store, db, cleanup := b.factory(t)
				defer cleanup()
				runQueuePollAndClaim(t, store, db)
			})
			t.Run("QueueCompleteAndFail", func(t *testing.T) {
				store, db, cleanup := b.factory(t)
				defer cleanup()
				runQueueCompleteAndFail(t, store, db)
			})
			t.Run("QueueByTaskID", func(t *testing.T) {
				store, db, cleanup := b.factory(t)
				defer cleanup()
				runQueueByTaskID(t, store, db)
			})
			t.Run("QueueEntryDetails", func(t *testing.T) {
				store, db, cleanup := b.factory(t)
				defer cleanup()
				runQueueEntryDetails(t, store, db)
			})
			t.Run("QueueStaleClaimedEntries", func(t *testing.T) {
				store, db, cleanup := b.factory(t)
				defer cleanup()
				runQueueStaleClaimedEntries(t, store, db)
			})
			t.Run("AdminWorkspaceQueries", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAdminWorkspaceQueries(t, store)
			})
			t.Run("PausedStates", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runPausedStates(t, store)
			})
			t.Run("ContextAndDependencies", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runContextAndDependencies(t, store)
			})
			t.Run("NewFilters", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runNewFilters(t, store)
			})
			t.Run("Descendants", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runDescendants(t, store)
			})
			t.Run("Cursor", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runCursorPagination(t, store)
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

// runAuditEventsTx: Store.BeginTx → RecordAuditEventTx → tx.Commit →
// GetTaskAuditEvents shows the event. Verifies the StoreTx abstraction
// works end-to-end through the Store interface.
func runAuditEventsTx(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "audit-tx")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	tx, err := store.BeginTx(ctx)
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

// runQueuePollAndClaim: insert a pending queue entry, PollPendingQueueEntries
// returns it, ClaimQueueEntry succeeds first time and fails second time,
// CountPendingQueueEntries reflects the change.
func runQueuePollAndClaim(t *testing.T, store tasks.Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	// Create a task first (FK dependency)
	task := newTestTask(t, "queue-poll")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	queueID := uuid.New().String()
	insertQueueEntry(t, store, queueID, task.TaskID, "test-impl", "_test", "kubernetes")

	// CountPendingQueueEntries should include this entry
	count, err := store.CountPendingQueueEntries(ctx)
	if err != nil {
		t.Fatalf("CountPendingQueueEntries: %v", err)
	}
	if count < 1 {
		t.Fatalf("CountPendingQueueEntries: got %d, want >= 1", count)
	}

	// PollPendingQueueEntries should return the entry
	entries, err := store.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.QueueID == queueID {
			found = true
			if e.TaskID != task.TaskID {
				t.Fatalf("PollPendingQueueEntries: task_id got %q want %q", e.TaskID, task.TaskID)
			}
			if e.Profile != "kubernetes" {
				t.Fatalf("PollPendingQueueEntries: profile got %q want %q", e.Profile, "kubernetes")
			}
		}
	}
	if !found {
		t.Fatalf("PollPendingQueueEntries: queue entry %s not found in results", queueID)
	}

	// ClaimQueueEntry should succeed first time
	claimed, err := store.ClaimQueueEntry(ctx, queueID, "orchestrator-1")
	if err != nil {
		t.Fatalf("ClaimQueueEntry first call: %v", err)
	}
	if !claimed {
		t.Fatalf("ClaimQueueEntry first call: expected true")
	}

	// ClaimQueueEntry should fail second time (already claimed)
	claimed2, err := store.ClaimQueueEntry(ctx, queueID, "orchestrator-2")
	if err != nil {
		t.Fatalf("ClaimQueueEntry second call: %v", err)
	}
	if claimed2 {
		t.Fatalf("ClaimQueueEntry second call: expected false (already claimed)")
	}
}

// runQueueCompleteAndFail: insert queue entries, CompleteQueueEntry and
// FailQueueEntry transition them to terminal states.
func runQueueCompleteAndFail(t *testing.T, store tasks.Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	task1 := newTestTask(t, "queue-complete")
	task2 := newTestTask(t, "queue-fail")
	if err := store.CreateTask(ctx, task1); err != nil {
		t.Fatalf("CreateTask(1): %v", err)
	}
	if err := store.CreateTask(ctx, task2); err != nil {
		t.Fatalf("CreateTask(2): %v", err)
	}

	queueID1 := uuid.New().String()
	queueID2 := uuid.New().String()
	insertQueueEntry(t, store, queueID1, task1.TaskID, "impl-a", "_test", "docker")
	insertQueueEntry(t, store, queueID2, task2.TaskID, "impl-b", "_test", "docker")

	// Complete one
	if err := store.CompleteQueueEntry(ctx, queueID1); err != nil {
		t.Fatalf("CompleteQueueEntry: %v", err)
	}

	// Fail the other
	if err := store.FailQueueEntry(ctx, queueID2, "test error"); err != nil {
		t.Fatalf("FailQueueEntry: %v", err)
	}

	// Neither should appear in poll results
	entries, err := store.PollPendingQueueEntries(ctx, 100)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	for _, e := range entries {
		if e.QueueID == queueID1 || e.QueueID == queueID2 {
			t.Fatalf("PollPendingQueueEntries: completed/failed entry %s still returned", e.QueueID)
		}
	}
}

// runQueueByTaskID: insert queue entries, CompleteQueueEntryByTaskID and
// FailQueueEntryByTaskID transition them by task_id.
func runQueueByTaskID(t *testing.T, store tasks.Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	task1 := newTestTask(t, "queue-bytaskid-c")
	task2 := newTestTask(t, "queue-bytaskid-f")
	if err := store.CreateTask(ctx, task1); err != nil {
		t.Fatalf("CreateTask(1): %v", err)
	}
	if err := store.CreateTask(ctx, task2); err != nil {
		t.Fatalf("CreateTask(2): %v", err)
	}

	queueID1 := uuid.New().String()
	queueID2 := uuid.New().String()
	insertQueueEntry(t, store, queueID1, task1.TaskID, "impl-c", "_test", "local")
	insertQueueEntry(t, store, queueID2, task2.TaskID, "impl-d", "_test", "local")

	// Complete by task ID
	if err := store.CompleteQueueEntryByTaskID(ctx, task1.TaskID); err != nil {
		t.Fatalf("CompleteQueueEntryByTaskID: %v", err)
	}

	// Fail by task ID
	if err := store.FailQueueEntryByTaskID(ctx, task2.TaskID, "cancelled"); err != nil {
		t.Fatalf("FailQueueEntryByTaskID: %v", err)
	}

	// Neither should appear in poll results
	entries, err := store.PollPendingQueueEntries(ctx, 100)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	for _, e := range entries {
		if e.QueueID == queueID1 || e.QueueID == queueID2 {
			t.Fatalf("PollPendingQueueEntries: terminated entry %s still returned", e.QueueID)
		}
	}
}

// runQueueEntryDetails: insert a queue entry with launch_params, verify
// GetQueueEntryDetails returns them correctly.
func runQueueEntryDetails(t *testing.T, store tasks.Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	task := newTestTask(t, "queue-details")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	queueID := uuid.New().String()
	insertQueueEntryWithParams(t, store, queueID, task.TaskID, "test-impl", "_test", "k8s", `{"image":"test:latest","replicas":"1"}`)

	details, err := store.GetQueueEntryDetails(ctx, queueID)
	if err != nil {
		t.Fatalf("GetQueueEntryDetails: %v", err)
	}
	if details.TaskID != task.TaskID {
		t.Fatalf("GetQueueEntryDetails: task_id got %q want %q", details.TaskID, task.TaskID)
	}
	if details.Profile != "k8s" {
		t.Fatalf("GetQueueEntryDetails: profile got %q want %q", details.Profile, "k8s")
	}
	if details.LaunchParams == nil {
		t.Fatalf("GetQueueEntryDetails: launch_params is nil")
	}
	if details.LaunchParams["image"] != "test:latest" {
		t.Fatalf("GetQueueEntryDetails: launch_params[image] got %v want %q", details.LaunchParams["image"], "test:latest")
	}
}

// runQueueStaleClaimedEntries: insert a claimed queue entry with old
// claimed_at, verify ListStaleClaimedQueueEntries finds it.
//
// The sqlite (dbcompat) backend is expected to fail this test because the
// postgres impl's ListStaleClaimedQueueEntries uses $1::interval syntax
// that dbcompat cannot translate. The native sqlite impl computes the
// cutoff in Go and passes it as a plain parameter, which works correctly.
// This is a known dbcompat limitation (§15.4) and will be resolved when
// Stage 3 retires dbcompat from the lite path.
func runQueueStaleClaimedEntries(t *testing.T, store tasks.Store, db *sql.DB) {
	t.Helper()

	// The sqlite (dbcompat) backend runs the postgres impl which uses
	// $1::interval syntax. dbcompat cannot translate interval arithmetic,
	// so ListStaleClaimedQueueEntries fails silently (returns empty).
	// Skip this test for the dbcompat backend — the native sqlite impl
	// passes it correctly. This is a known dbcompat limitation (§15.4).
	//
	// Detection: if the store is the postgres impl (*taskspg.Store) but
	// the underlying database is NOT real PostgreSQL (no pg_catalog),
	// we're running through dbcompat and must skip.
	if _, ok := store.(*taskspg.Store); ok {
		var dummy int
		if err := db.QueryRow("SELECT 1 FROM pg_catalog.pg_class LIMIT 1").Scan(&dummy); err != nil {
			t.Skip("skipping: dbcompat cannot translate ::interval syntax; use sqlite_native backend")
		}
	}

	ctx := context.Background()

	task := newTestTask(t, "queue-stale")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	queueID := uuid.New().String()
	insertQueueEntry(t, store, queueID, task.TaskID, "stale-impl", "_test", "docker")

	// Claim it first
	claimed, err := store.ClaimQueueEntry(ctx, queueID, "stale-orchestrator")
	if err != nil {
		t.Fatalf("ClaimQueueEntry: %v", err)
	}
	if !claimed {
		t.Fatalf("ClaimQueueEntry: expected true")
	}

	// Backdate the claimed_at to make it stale. Use raw SQL since we need
	// to manipulate timestamps directly.
	staleTime := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if isPostgres(db) {
		_, err = db.ExecContext(ctx, `UPDATE orchestrated_task_queue SET claimed_at = $1 WHERE queue_id = $2`, staleTime, queueID)
	} else {
		_, err = db.ExecContext(ctx, `UPDATE orchestrated_task_queue SET claimed_at = ? WHERE queue_id = ?`, staleTime, queueID)
	}
	if err != nil {
		t.Fatalf("backdate claimed_at: %v", err)
	}

	// ListStaleClaimedQueueEntries with 1-hour threshold should find it
	staleIDs, err := store.ListStaleClaimedQueueEntries(ctx, 1*time.Hour, 50)
	if err != nil {
		t.Fatalf("ListStaleClaimedQueueEntries: %v", err)
	}
	found := false
	for _, id := range staleIDs {
		if id == queueID {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListStaleClaimedQueueEntries: stale entry %s not found (got %v)", queueID, staleIDs)
	}
}

// runAdminWorkspaceQueries: exercises ListDistinctTaskWorkspaces and
// GetWorkspaceTaskStats — the two new methods that replaced the raw SQL
// in admin_workspaces.go.
func runAdminWorkspaceQueries(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	workspace := fmt.Sprintf("ws-admin-%d", time.Now().UnixNano())
	const n = 3
	for i := 0; i < n; i++ {
		task := newTestTask(t, fmt.Sprintf("admin-ws-%d", i))
		task.Workspace = workspace
		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask[%d]: %v", i, err)
		}
	}

	// ListDistinctTaskWorkspaces should include our workspace
	summaries, err := store.ListDistinctTaskWorkspaces(ctx)
	if err != nil {
		t.Fatalf("ListDistinctTaskWorkspaces: %v", err)
	}
	var found *tasks.WorkspaceTaskSummary
	for _, s := range summaries {
		if s.Workspace == workspace {
			found = s
			break
		}
	}
	if found == nil {
		t.Fatalf("ListDistinctTaskWorkspaces: workspace %q not found in results", workspace)
	}
	if found.TaskCount != n {
		t.Fatalf("ListDistinctTaskWorkspaces: task_count got %d want %d", found.TaskCount, n)
	}
	if found.CreatedAt.IsZero() {
		t.Fatalf("ListDistinctTaskWorkspaces: created_at is zero")
	}

	// GetWorkspaceTaskStats should return correct stats
	stats, err := store.GetWorkspaceTaskStats(ctx, workspace)
	if err != nil {
		t.Fatalf("GetWorkspaceTaskStats: %v", err)
	}
	if stats.TaskCount != n {
		t.Fatalf("GetWorkspaceTaskStats: task_count got %d want %d", stats.TaskCount, n)
	}
	if stats.CreatedAt.IsZero() {
		t.Fatalf("GetWorkspaceTaskStats: created_at is zero")
	}

	// GetWorkspaceTaskStats for non-existent workspace returns zero values
	emptyStats, err := store.GetWorkspaceTaskStats(ctx, "nonexistent-workspace")
	if err != nil {
		t.Fatalf("GetWorkspaceTaskStats(nonexistent): %v", err)
	}
	if emptyStats.TaskCount != 0 {
		t.Fatalf("GetWorkspaceTaskStats(nonexistent): task_count got %d want 0", emptyStats.TaskCount)
	}

	// Empty-workspace tasks should NOT appear in ListDistinctTaskWorkspaces
	emptyWsTask := newTestTask(t, "admin-ws-empty")
	emptyWsTask.Workspace = ""
	if err := store.CreateTask(ctx, emptyWsTask); err != nil {
		t.Fatalf("CreateTask(empty-ws): %v", err)
	}
	summaries2, err := store.ListDistinctTaskWorkspaces(ctx)
	if err != nil {
		t.Fatalf("ListDistinctTaskWorkspaces (after empty): %v", err)
	}
	for _, s := range summaries2 {
		if s.Workspace == "" {
			t.Fatalf("ListDistinctTaskWorkspaces: empty workspace should be excluded")
		}
	}
}

// =============================================================================
// Queue entry test helpers
// =============================================================================

// insertQueueEntry inserts a pending queue entry via raw SQL. The Store
// interface does not expose queue-entry creation (that lives in the task
// assignment service), so tests must seed the table directly.
func insertQueueEntry(t *testing.T, store tasks.Store, queueID, taskID, impl, workspace, profile string) {
	t.Helper()
	insertQueueEntryWithParams(t, store, queueID, taskID, impl, workspace, profile, "")
}

func insertQueueEntryWithParams(t *testing.T, store tasks.Store, queueID, taskID, impl, workspace, profile, launchParamsJSON string) {
	t.Helper()
	var lp []byte
	if launchParamsJSON != "" {
		lp = []byte(launchParamsJSON)
	}
	if err := store.InsertQueueEntry(context.Background(), queueID, taskID, impl, workspace, profile, lp); err != nil {
		t.Fatalf("insert queue entry: %v", err)
	}
}

// isPostgres is a best-effort detection of whether the *sql.DB is backed by
// PostgreSQL (uses $N placeholders) vs SQLite (uses ?). It issues a cheap
// probe query with a $1 placeholder — postgres accepts it, sqlite rejects it.
func isPostgres(db *sql.DB) bool {
	_, err := db.Exec("SELECT 1 WHERE 1 = $1", 1)
	return err == nil
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

// sqliteNativeFactory opens a fresh temp-dir SQLite database via the bare
// "sqlite" driver (no dbcompat), constructs the native-sqlite tasks.Store
// which runs its own migrations, and returns the store. This factory
// exercises the Stage 2 native implementation that will replace the
// dbcompat path in AetherLite.
func sqliteNativeFactory(t *testing.T) (tasks.Store, *sql.DB, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "tasks_native.db")
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

	cleanup := func() {
		_ = db.Close()
	}
	return store, db, cleanup
}

// =============================================================================
// Helpers
// =============================================================================

// newTestTask builds a minimal valid Task. The TaskID is fresh per call so
// rows do not collide with prior runs or other concurrent tests sharing the
// dev database.
// runPausedStates: PauseTask transitions running -> waiting_* with WaitSpec,
// ResumeTask returns it to running, RejectTask is terminal, ListWaitingTasks
// finds paused tasks.
func runPausedStates(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	// Build a running task to pause.
	task := newTestTask(t, "paused")
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := store.AssignTask(ctx, task.TaskID, "worker-1"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	if err := store.StartTask(ctx, task.TaskID); err != nil {
		t.Fatalf("StartTask: %v", err)
	}
	assertStatus(t, store, task.TaskID, tasks.TaskStatusRunning)

	// PAUSE -> waiting_input with a typed WaitSpec.
	spec := &tasks.WaitSpec{
		Reason:            tasks.WaitReasonInput,
		ExpectedPrincipal: "user::alice",
		InputMatch:        map[string]string{"kind": "approval"},
		TimeoutMs:         60_000,
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusWaitingInput, spec); err != nil {
		t.Fatalf("PauseTask: %v", err)
	}
	assertStatus(t, store, task.TaskID, tasks.TaskStatusWaitingInput)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after pause: %v", err)
	}
	if got.WaitSpec == nil {
		t.Fatalf("expected WaitSpec to be persisted, got nil")
	}
	if got.WaitSpec.Reason != tasks.WaitReasonInput {
		t.Errorf("WaitSpec.Reason = %q, want %q", got.WaitSpec.Reason, tasks.WaitReasonInput)
	}
	if got.WaitSpec.ExpectedPrincipal != "user::alice" {
		t.Errorf("WaitSpec.ExpectedPrincipal = %q, want user::alice", got.WaitSpec.ExpectedPrincipal)
	}
	if got.WaitSpec.InputMatch["kind"] != "approval" {
		t.Errorf("WaitSpec.InputMatch[kind] = %q, want approval", got.WaitSpec.InputMatch["kind"])
	}
	if got.PausedAt == nil || got.PausedAt.IsZero() {
		t.Errorf("PausedAt should be set on pause, got %v", got.PausedAt)
	}

	// ListWaitingTasks must include the paused task.
	waiting, err := store.ListWaitingTasks(ctx, 100)
	if err != nil {
		t.Fatalf("ListWaitingTasks: %v", err)
	}
	if !containsTask(waiting, task.TaskID) {
		t.Errorf("ListWaitingTasks did not include paused task %s", task.TaskID)
	}

	// RESUME -> running, WaitSpec cleared.
	if err := store.ResumeTask(ctx, task.TaskID, tasks.TaskStatusRunning); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	assertStatus(t, store, task.TaskID, tasks.TaskStatusRunning)
	got, err = store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after resume: %v", err)
	}
	if got.WaitSpec != nil {
		t.Errorf("WaitSpec should be cleared after resume, got %+v", got.WaitSpec)
	}

	// Reject path: a fresh pending task can be rejected directly.
	rejTask := newTestTask(t, "reject")
	if err := store.CreateTask(ctx, rejTask); err != nil {
		t.Fatalf("CreateTask reject: %v", err)
	}
	if err := store.RejectTask(ctx, rejTask.TaskID, "policy violation"); err != nil {
		t.Fatalf("RejectTask: %v", err)
	}
	assertStatus(t, store, rejTask.TaskID, tasks.TaskStatusRejected)

	// Illegal: pausing a rejected (terminal) task must fail.
	if err := store.PauseTask(ctx, rejTask.TaskID, tasks.TaskStatusWaitingInput, &tasks.WaitSpec{Reason: tasks.WaitReasonInput}); err == nil {
		t.Errorf("PauseTask on rejected task should fail")
	}
}

// runContextAndDependencies: ContextID round-trip, ListTasksByContext filter,
// and dependency-reverse-lookup via ListTasksWaitingOnDependency.
func runContextAndDependencies(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	const sess = "session-conf-42"

	// Three tasks in the same context.
	parent := newTestTask(t, "ctx-parent")
	parent.ContextID = sess
	if err := store.CreateTask(ctx, parent); err != nil {
		t.Fatalf("CreateTask parent: %v", err)
	}

	childA := newTestTask(t, "ctx-childA")
	childA.ContextID = sess
	if err := store.CreateTask(ctx, childA); err != nil {
		t.Fatalf("CreateTask childA: %v", err)
	}

	childB := newTestTask(t, "ctx-childB")
	childB.ContextID = sess
	if err := store.CreateTask(ctx, childB); err != nil {
		t.Fatalf("CreateTask childB: %v", err)
	}

	// Context-scoped listing.
	listed, err := store.ListTasksByContext(ctx, sess, 100)
	if err != nil {
		t.Fatalf("ListTasksByContext: %v", err)
	}
	if len(listed) < 3 {
		t.Fatalf("ListTasksByContext(%s): got %d tasks, want >=3", sess, len(listed))
	}
	for _, want := range []string{parent.TaskID, childA.TaskID, childB.TaskID} {
		if !containsTask(listed, want) {
			t.Errorf("ListTasksByContext missing %s", want)
		}
	}

	// Make parent wait on the two children.
	if err := store.AssignTask(ctx, parent.TaskID, "worker-p"); err != nil {
		t.Fatalf("AssignTask parent: %v", err)
	}
	if err := store.StartTask(ctx, parent.TaskID); err != nil {
		t.Fatalf("StartTask parent: %v", err)
	}
	depSpec := &tasks.WaitSpec{
		Reason:    tasks.WaitReasonDependency,
		DependsOn: []string{childA.TaskID, childB.TaskID},
	}
	if err := store.PauseTask(ctx, parent.TaskID, tasks.TaskStatusWaitingDependency, depSpec); err != nil {
		t.Fatalf("PauseTask parent: %v", err)
	}

	// Reverse-lookup: tasks waiting on childA must include parent.
	depTasks, err := store.ListTasksWaitingOnDependency(ctx, childA.TaskID)
	if err != nil {
		t.Fatalf("ListTasksWaitingOnDependency: %v", err)
	}
	if !containsTask(depTasks, parent.TaskID) {
		t.Errorf("ListTasksWaitingOnDependency(%s) did not include parent %s", childA.TaskID, parent.TaskID)
	}
}

// =============================================================================
// Phase 4 — new-filter conformance subtests.
//
// Coverage: CreatorActorID, StatusTimestampAfterUnixMs, IncludeDescendants
// (recursive walk via CTE), and cursor-based pagination via ListTasksPage.
// =============================================================================

// runNewFilters exercises CreatorActorID + StatusTimestampAfterUnixMs.
//
// CreatorActorID is matched against the parent_agent_id column (the storage
// backing for TaskInfo.creator_actor_id). The status-timestamp filter
// uses updated_at >= timestamp(ms); we drive updated_at by transitioning
// task status (which bumps updated_at in both backends).
func runNewFilters(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	workspace := fmt.Sprintf("ws-newfilters-%d", time.Now().UnixNano())

	// 3 tasks for alice, 2 tasks for bob, all in the same workspace.
	const alice = "user::alice"
	const bob = "user::bob"
	const aliceCount = 3
	const bobCount = 2

	aliceIDs := make([]string, 0, aliceCount)
	for i := 0; i < aliceCount; i++ {
		task := newTestTask(t, fmt.Sprintf("alice-%d", i))
		task.Workspace = workspace
		task.ParentAgentID = alice
		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask alice[%d]: %v", i, err)
		}
		aliceIDs = append(aliceIDs, task.TaskID)
	}
	for i := 0; i < bobCount; i++ {
		task := newTestTask(t, fmt.Sprintf("bob-%d", i))
		task.Workspace = workspace
		task.ParentAgentID = bob
		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask bob[%d]: %v", i, err)
		}
	}

	// CreatorActorID filter narrows to alice's tasks only.
	gotAlice, _, err := store.ListTasksPage(ctx, &tasks.TaskFilter{
		Workspace:      workspace,
		CreatorActorID: alice,
		Limit:          100,
	})
	if err != nil {
		t.Fatalf("ListTasksPage(creator=alice): %v", err)
	}
	if len(gotAlice) != aliceCount {
		t.Fatalf("CreatorActorID=alice: got %d rows want %d", len(gotAlice), aliceCount)
	}
	for _, tsk := range gotAlice {
		if tsk.ParentAgentID != alice {
			t.Errorf("CreatorActorID=alice: row leaked from %q", tsk.ParentAgentID)
		}
	}

	// StatusTimestampAfterUnixMs: capture the "now" cutoff, then transition
	// one alice task to RUNNING which bumps its updated_at. After the
	// cutoff, only the transitioned task should match. (We use a 50ms
	// quiet window to ensure clock skew between rows doesn't bite.)
	time.Sleep(50 * time.Millisecond)
	cutoffMs := time.Now().UnixMilli()
	time.Sleep(50 * time.Millisecond)

	bumpID := aliceIDs[0]
	if err := store.AssignTask(ctx, bumpID, "worker-bump"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	if err := store.StartTask(ctx, bumpID); err != nil {
		t.Fatalf("StartTask: %v", err)
	}

	gotRecent, _, err := store.ListTasksPage(ctx, &tasks.TaskFilter{
		Workspace:                  workspace,
		StatusTimestampAfterUnixMs: cutoffMs,
		Limit:                      100,
	})
	if err != nil {
		t.Fatalf("ListTasksPage(status_timestamp_after): %v", err)
	}
	// At least the bumped task must be present and no rows older than
	// the cutoff are expected. We don't assert exact count to allow for
	// the very-slim window where another row also lands updated_at >=
	// cutoff (e.g. trigger-side timestamp resolution).
	foundBump := false
	for _, tsk := range gotRecent {
		if tsk.TaskID == bumpID {
			foundBump = true
		}
		if tsk.UpdatedAt.UnixMilli() < cutoffMs {
			t.Errorf("StatusTimestampAfter cutoff=%d returned older row updated_at=%d",
				cutoffMs, tsk.UpdatedAt.UnixMilli())
		}
	}
	if !foundBump {
		t.Errorf("StatusTimestampAfter: bumped task %s missing from results", bumpID)
	}
}

// runDescendants creates a 4-deep parent chain (A -> B -> C -> D) and
// verifies IncludeDescendants=true returns B, C, D when filtering by
// parent=A. With IncludeDescendants=false (default), only B is returned.
func runDescendants(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	workspace := fmt.Sprintf("ws-desc-%d", time.Now().UnixNano())

	taskA := newTestTask(t, "desc-A")
	taskA.Workspace = workspace
	if err := store.CreateTask(ctx, taskA); err != nil {
		t.Fatalf("CreateTask A: %v", err)
	}
	taskB := newTestTask(t, "desc-B")
	taskB.Workspace = workspace
	taskB.ParentTaskID = taskA.TaskID
	if err := store.CreateTask(ctx, taskB); err != nil {
		t.Fatalf("CreateTask B: %v", err)
	}
	taskC := newTestTask(t, "desc-C")
	taskC.Workspace = workspace
	taskC.ParentTaskID = taskB.TaskID
	if err := store.CreateTask(ctx, taskC); err != nil {
		t.Fatalf("CreateTask C: %v", err)
	}
	taskD := newTestTask(t, "desc-D")
	taskD.Workspace = workspace
	taskD.ParentTaskID = taskC.TaskID
	if err := store.CreateTask(ctx, taskD); err != nil {
		t.Fatalf("CreateTask D: %v", err)
	}

	// Default: direct children only.
	direct, _, err := store.ListTasksPage(ctx, &tasks.TaskFilter{
		Workspace:    workspace,
		ParentTaskID: taskA.TaskID,
		Limit:        100,
	})
	if err != nil {
		t.Fatalf("ListTasksPage(direct children): %v", err)
	}
	if len(direct) != 1 {
		t.Fatalf("direct children of A: got %d want 1", len(direct))
	}
	if direct[0].TaskID != taskB.TaskID {
		t.Errorf("direct children of A: got %q want %q", direct[0].TaskID, taskB.TaskID)
	}

	// IncludeDescendants=true: walks the chain via recursive CTE.
	all, _, err := store.ListTasksPage(ctx, &tasks.TaskFilter{
		Workspace:          workspace,
		ParentTaskID:       taskA.TaskID,
		IncludeDescendants: true,
		Limit:              100,
	})
	if err != nil {
		t.Fatalf("ListTasksPage(include_descendants): %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("descendants of A: got %d want 3 (B+C+D)", len(all))
	}
	want := map[string]bool{taskB.TaskID: false, taskC.TaskID: false, taskD.TaskID: false}
	for _, tsk := range all {
		if _, ok := want[tsk.TaskID]; !ok {
			t.Errorf("descendants of A: unexpected row %s", tsk.TaskID)
			continue
		}
		want[tsk.TaskID] = true
	}
	for id, found := range want {
		if !found {
			t.Errorf("descendants of A: missing %s", id)
		}
	}
}

// runCursorPagination creates N tasks (N > limit), pages through them with
// a small page size, and asserts: (a) no duplicates across pages, (b) no
// gaps (count(page1) + count(page2) + ... == N), (c) empty cursor on the
// final page, (d) invalid cursor returns an error.
func runCursorPagination(t *testing.T, store tasks.Store) {
	t.Helper()
	ctx := context.Background()

	workspace := fmt.Sprintf("ws-cursor-%d", time.Now().UnixNano())
	const total = 7
	const pageSize = 3

	for i := 0; i < total; i++ {
		task := newTestTask(t, fmt.Sprintf("cursor-%d", i))
		task.Workspace = workspace
		if err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("CreateTask[%d]: %v", i, err)
		}
		// Stagger updated_at so the (updated_at DESC, task_id DESC)
		// ordering is unambiguous; some sqlite versions tick at coarser
		// resolution than postgres NOW().
		time.Sleep(2 * time.Millisecond)
	}

	seen := map[string]int{}
	cursor := ""
	pages := 0
	for {
		pages++
		if pages > 100 {
			t.Fatalf("cursor pagination did not terminate after %d pages", pages)
		}
		rows, next, err := store.ListTasksPage(ctx, &tasks.TaskFilter{
			Workspace: workspace,
			PageToken: cursor,
			Limit:     pageSize,
		})
		if err != nil {
			t.Fatalf("ListTasksPage(page %d): %v", pages, err)
		}
		for _, r := range rows {
			seen[r.TaskID]++
		}
		if next == "" {
			break
		}
		cursor = next
	}

	if len(seen) != total {
		t.Fatalf("cursor pagination: saw %d unique rows want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("cursor pagination: row %s seen %d times (want exactly 1)", id, count)
		}
	}

	// Invalid cursor must be reported as such, not silently swallowed.
	_, _, err := store.ListTasksPage(ctx, &tasks.TaskFilter{
		Workspace: workspace,
		PageToken: "not-a-valid-token",
		Limit:     pageSize,
	})
	if err == nil {
		t.Errorf("ListTasksPage with malformed page_token: expected error, got nil")
	}
}

func containsTask(list []*tasks.Task, id string) bool {
	for _, x := range list {
		if x != nil && x.TaskID == id {
			return true
		}
	}
	return false
}

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
