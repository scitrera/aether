package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	aclstore "github.com/scitrera/aether/internal/storage/acl"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	taskssqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/pkg/tasks"

	// Register bare "sqlite" driver for the native sqlite backend.
	_ "modernc.org/sqlite"
)

// fakeAuthorityRequestSource is an in-memory stand-in for the *acl.Service /
// sqlite Store AuthorityRequestSource. The waker tests use it to drive
// per-status branches deterministically without spinning a real SQL-backed
// ACL service.
type fakeAuthorityRequestSource struct {
	requests   map[string]*aclstore.AuthorityRequest
	sweepCount int64
}

func newFakeAuthorityRequestSource() *fakeAuthorityRequestSource {
	return &fakeAuthorityRequestSource{
		requests: make(map[string]*aclstore.AuthorityRequest),
	}
}

func (f *fakeAuthorityRequestSource) put(req *aclstore.AuthorityRequest) {
	if req == nil || req.RequestID == "" {
		return
	}
	f.requests[req.RequestID] = req
}

func (f *fakeAuthorityRequestSource) GetAuthorityRequest(ctx context.Context, requestID string) (*aclstore.AuthorityRequest, error) {
	if req, ok := f.requests[requestID]; ok {
		return req, nil
	}
	return nil, nil
}

func (f *fakeAuthorityRequestSource) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*aclstore.AuthorityRequest, error) {
	atomic.AddInt64(&f.sweepCount, 1)
	return nil, nil
}

func (f *fakeAuthorityRequestSource) sweepCalls() int64 {
	return atomic.LoadInt64(&f.sweepCount)
}

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
	waker := NewTaskWaker(store, svc, nil)
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
	waker := NewTaskWaker(store, svc, nil)
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
	waker := NewTaskWaker(store, svc, nil)
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
	waker := NewTaskWaker(store, svc, nil)
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
// past — scan transitions the task to pending.
//
// Phase 3 Stage B note: HIBERNATED scheduled wakes route through
// WakeHibernatedTask which targets `pending`, not `running`, so the fresh
// worker can be spawned by the orchestrator with the prior checkpoint
// rehydrated. Other waiting states (INPUT/AUTHORITY/DEPENDENCY) still
// resume directly to `running` because they retain their worker. The
// task here is non-orchestrated (no TargetImplementation / profile), so
// it lands in WakeHibernatedTask's "left pending without re-queue"
// branch — the status flip still happens but no queue entry is inserted.
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
	waker := NewTaskWaker(store, svc, nil)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Errorf("scheduled wake: status got %q want pending (Stage B: hibernated wakes -> pending for orchestrator re-spawn)", got.Status)
	}
}

// TestTaskWaker_NoOpWhenNothingPending: with no waiting tasks, scan
// completes without error and without modifying anything.
func TestTaskWaker_NoOpWhenNothingPending(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, nil)
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

	waker := NewTaskWaker(store, nil, nil)
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

// =============================================================================
// Phase 2 Stage C: WAITING_AUTHORITY reconciliation
// =============================================================================

// buildWaitingAuthorityTask CreateTask → AssignTask → StartTask → PauseTask
// with a WAITING_AUTHORITY wait spec keyed on the supplied request id.
func buildWaitingAuthorityTask(t *testing.T, ctx context.Context, store taskstore.Store, hint, requestID string) *tasks.Task {
	t.Helper()
	task := buildRunningTask(t, ctx, store, hint)
	spec := &tasks.WaitSpec{
		Reason:             tasks.WaitReasonAuthority,
		AuthorityRequestID: requestID,
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusWaitingAuthority, spec); err != nil {
		t.Fatalf("PauseTask(%s) waiting_authority: %v", hint, err)
	}
	return task
}

// TestTaskWaker_AuthorityApproved_WakesTask: PENDING request keeps the task
// paused; once the request flips to APPROVED, the next scan transitions the
// task back to running.
func TestTaskWaker_AuthorityApproved_WakesTask(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	requestID := "ar-approve"
	task := buildWaitingAuthorityTask(t, ctx, store, "approve", requestID)

	fake := newFakeAuthorityRequestSource()
	fake.put(&aclstore.AuthorityRequest{
		RequestID: requestID,
		Status:    aclstore.AuthorityRequestStatusPending,
	})

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, fake)

	// First scan: request still pending — task must remain paused.
	waker.scan(ctx)
	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after pending scan: %v", err)
	}
	if got.Status != tasks.TaskStatusWaitingAuthority {
		t.Fatalf("after pending scan: task got %q want waiting_authority", got.Status)
	}

	// Flip to approved; the next scan resumes.
	fake.put(&aclstore.AuthorityRequest{
		RequestID:      requestID,
		Status:         aclstore.AuthorityRequestStatusApproved,
		GrantedGrantID: "grant-abc",
	})
	waker.scan(ctx)
	got, err = store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after approved scan: %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("after approved scan: task got %q want running", got.Status)
	}
}

// TestTaskWaker_AuthorityDenied_FailsTask: request DENIED + reason → task
// transitions to FAILED with an error message containing "denied:<reason>".
func TestTaskWaker_AuthorityDenied_FailsTask(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	requestID := "ar-deny"
	task := buildWaitingAuthorityTask(t, ctx, store, "deny", requestID)

	fake := newFakeAuthorityRequestSource()
	fake.put(&aclstore.AuthorityRequest{
		RequestID:        requestID,
		Status:           aclstore.AuthorityRequestStatusDenied,
		ResolutionReason: "policy",
	})

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, fake)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after denied scan: %v", err)
	}
	if got.Status != tasks.TaskStatusFailed {
		t.Errorf("after denied scan: task got %q want failed", got.Status)
	}
	if !strings.Contains(got.ErrorMessage, "denied") || !strings.Contains(got.ErrorMessage, "policy") {
		t.Errorf("after denied scan: error_message=%q want substring 'denied' AND 'policy'", got.ErrorMessage)
	}
}

// TestTaskWaker_AuthorityExpired_FailsTask: same as denied but with status
// EXPIRED and an empty resolution reason — the failure message still names
// the status so operators can disambiguate.
func TestTaskWaker_AuthorityExpired_FailsTask(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	requestID := "ar-expire"
	task := buildWaitingAuthorityTask(t, ctx, store, "expire", requestID)

	fake := newFakeAuthorityRequestSource()
	fake.put(&aclstore.AuthorityRequest{
		RequestID: requestID,
		Status:    aclstore.AuthorityRequestStatusExpired,
	})

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, fake)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after expired scan: %v", err)
	}
	if got.Status != tasks.TaskStatusFailed {
		t.Errorf("after expired scan: task got %q want failed", got.Status)
	}
	if !strings.Contains(got.ErrorMessage, "expired") {
		t.Errorf("after expired scan: error_message=%q want substring 'expired'", got.ErrorMessage)
	}
}

// TestTaskWaker_SweepCalledOnce confirms the per-scan SweepExpired*
// invocation runs exactly once per tick regardless of the waiting-task
// population.
func TestTaskWaker_SweepCalledOnce(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	// Park one task on a pending request so the per-task loop has work to do.
	requestID := "ar-sweep"
	_ = buildWaitingAuthorityTask(t, ctx, store, "sweep", requestID)

	fake := newFakeAuthorityRequestSource()
	fake.put(&aclstore.AuthorityRequest{
		RequestID: requestID,
		Status:    aclstore.AuthorityRequestStatusPending,
	})

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, fake)

	waker.scan(ctx)
	if got := fake.sweepCalls(); got != 1 {
		t.Errorf("after first scan: sweep call count=%d want 1", got)
	}
	waker.scan(ctx)
	if got := fake.sweepCalls(); got != 2 {
		t.Errorf("after second scan: sweep call count=%d want 2", got)
	}
}

// =============================================================================
// Phase 3 Stage B: Hibernation wake (re-queue + handoff metadata)
// =============================================================================

// buildHibernatedOrchestratedTask seeds an orchestrated task that has been
// paused into HIBERNATED with the supplied hibernation descriptor. The task is
// stamped with TargetImplementation + LaunchParams.profile so
// WakeHibernatedTask considers it orchestrator-spawnable and re-queues it.
func buildHibernatedOrchestratedTask(t *testing.T, ctx context.Context, store taskstore.Store, hint string, hib *tasks.HibernationDescriptor, scheduledWakeMs int64, timeoutMs int64) *tasks.Task {
	t.Helper()
	task := &tasks.Task{
		TaskID:               fmt.Sprintf("hib-%s-%s", hint, uuid.New().String()),
		TaskType:             "agent_startup",
		Workspace:            "_test",
		AssignmentMode:       tasks.AssignmentModePool,
		TaskCategory:         tasks.TaskCategoryOrchestrated,
		TargetImplementation: "test-worker",
		LaunchParams: map[string]interface{}{
			"profile": "kubernetes",
		},
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
	spec := &tasks.WaitSpec{
		Reason:              tasks.WaitReasonHibernation,
		Hibernation:         hib,
		ScheduledWakeUnixMs: scheduledWakeMs,
		TimeoutMs:           timeoutMs,
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusHibernated, spec); err != nil {
		t.Fatalf("PauseTask(%s) -> HIBERNATED: %v", hint, err)
	}
	return task
}

// TestWakeHibernatedTask_HappyPath: a HIBERNATED orchestrated task with a
// valid descriptor wakes back to pending, captures the descriptor into
// reserved metadata keys, clears its WaitSpec, and gets a fresh
// orchestrated_task_queue entry.
func TestWakeHibernatedTask_HappyPath(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	hib := &tasks.HibernationDescriptor{
		CheckpointKey:   "ckpt-happy",
		ResumeSessionID: "sess-happy",
	}
	wakeAt := time.Now().Add(-1 * time.Second).UnixMilli()
	task := buildHibernatedOrchestratedTask(t, ctx, store, "happy", hib, wakeAt, 0)

	svc := newWakerService(store)
	if err := svc.WakeHibernatedTask(ctx, task.TaskID); err != nil {
		t.Fatalf("WakeHibernatedTask: %v", err)
	}

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask after wake: %v", err)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Errorf("status after wake: got %q want %q", got.Status, tasks.TaskStatusPending)
	}
	if got.WaitSpec != nil {
		t.Errorf("WaitSpec should be cleared after wake; got %+v", got.WaitSpec)
	}
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationCheckpointKey].(string); v != "ckpt-happy" {
		t.Errorf("metadata[%q] = %q want %q", tasks.MetadataKeyHibernationCheckpointKey, v, "ckpt-happy")
	}
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationResumeSessionID].(string); v != "sess-happy" {
		t.Errorf("metadata[%q] = %q want %q", tasks.MetadataKeyHibernationResumeSessionID, v, "sess-happy")
	}

	// Verify queue re-insertion: PollPendingQueueEntries should yield a row
	// pointing at our task.
	entries, err := store.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	var found bool
	for _, e := range entries {
		if e.TaskID == task.TaskID {
			found = true
			if e.Profile != "kubernetes" {
				t.Errorf("queue entry profile = %q want %q", e.Profile, "kubernetes")
			}
			if e.TargetImplementation != "test-worker" {
				t.Errorf("queue entry impl = %q want %q", e.TargetImplementation, "test-worker")
			}
			break
		}
	}
	if !found {
		t.Errorf("expected new orchestrated_task_queue entry for %s, got %d entries", task.TaskID, len(entries))
	}
}

// TestWakeHibernatedTask_RejectsNonHibernated: a running (non-hibernated)
// task is a silent no-op for WakeHibernatedTask. Tests the idempotency
// guard against concurrent waker/admin races.
func TestWakeHibernatedTask_RejectsNonHibernated(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	task := buildRunningTask(t, ctx, store, "running")
	svc := newWakerService(store)

	if err := svc.WakeHibernatedTask(ctx, task.TaskID); err != nil {
		t.Fatalf("WakeHibernatedTask on running task should be a no-op, got err: %v", err)
	}
	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Errorf("non-hibernated guard: status got %q want running", got.Status)
	}
}

// TestWakeHibernatedTask_NonOrchestrated_LeftPending: a hibernated task with
// no TargetImplementation / profile cannot be re-queued for the orchestrator.
// WakeHibernatedTask should flip it to pending and log a warning but not
// insert a queue entry.
func TestWakeHibernatedTask_NonOrchestrated_LeftPending(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	// Build a task without LaunchParams / TargetImplementation, then put it
	// into HIBERNATED via the legal running->hibernated transition.
	task := buildRunningTask(t, ctx, store, "self-hib")
	spec := &tasks.WaitSpec{
		Reason: tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{
			CheckpointKey: "ckpt-self",
		},
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusHibernated, spec); err != nil {
		t.Fatalf("PauseTask -> HIBERNATED: %v", err)
	}

	svc := newWakerService(store)
	if err := svc.WakeHibernatedTask(ctx, task.TaskID); err != nil {
		t.Fatalf("WakeHibernatedTask should warn-but-not-error for non-orchestrated, got: %v", err)
	}
	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Errorf("status after wake: got %q want %q", got.Status, tasks.TaskStatusPending)
	}
	// Metadata still captures the descriptor for audit/debug visibility.
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationCheckpointKey].(string); v != "ckpt-self" {
		t.Errorf("metadata[%q] = %q want %q", tasks.MetadataKeyHibernationCheckpointKey, v, "ckpt-self")
	}
	// No queue entries should have been inserted.
	entries, err := store.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	for _, e := range entries {
		if e.TaskID == task.TaskID {
			t.Errorf("non-orchestrated task should not have a queue entry; found %+v", e)
		}
	}
}

// TestTaskWaker_HibernatedScheduledWake_RoutesToPending exercises the waker's
// scheduled-wake branch for HIBERNATED rows: it must call WakeHibernatedTask
// (i.e. flip to pending + capture handoff metadata + re-queue) rather than
// the plain ResumeTask used for other waiting states.
func TestTaskWaker_HibernatedScheduledWake_RoutesToPending(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	hib := &tasks.HibernationDescriptor{
		CheckpointKey:   "ckpt-sched",
		ResumeSessionID: "sess-sched",
	}
	wakeAt := time.Now().Add(-1 * time.Second).UnixMilli()
	task := buildHibernatedOrchestratedTask(t, ctx, store, "sched", hib, wakeAt, 0)

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, nil)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Errorf("status after scan: got %q want %q (hibernated wake routes through pending, not running)",
			got.Status, tasks.TaskStatusPending)
	}
	if got.WaitSpec != nil {
		t.Errorf("WaitSpec should be cleared after wake; got %+v", got.WaitSpec)
	}
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationCheckpointKey].(string); v != "ckpt-sched" {
		t.Errorf("metadata[%q] = %q want ckpt-sched", tasks.MetadataKeyHibernationCheckpointKey, v)
	}
	// And the row should be back on the orchestrated_task_queue.
	entries, err := store.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	var found bool
	for _, e := range entries {
		if e.TaskID == task.TaskID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected new orchestrated_task_queue entry after scheduled wake, got none")
	}
}

// TestTaskWaker_HibernatedTimeout_FailsByDefault: a HIBERNATED row whose
// PausedAt + TimeoutMs has elapsed and no EscalationPolicy (or "fail") set
// must transition to FAILED via the default branch.
func TestTaskWaker_HibernatedTimeout_FailsByDefault(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	hib := &tasks.HibernationDescriptor{
		CheckpointKey:    "ckpt-to-fail",
		EscalationPolicy: "", // default = fail
	}
	// TimeoutMs=1, no scheduled wake — let timeout fire.
	task := buildHibernatedOrchestratedTask(t, ctx, store, "tofail", hib, 0, 1)
	time.Sleep(10 * time.Millisecond)

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, nil)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusFailed {
		t.Errorf("default hibernation-timeout policy: got %q want failed", got.Status)
	}
	if got.ErrorMessage == "" {
		t.Errorf("expected non-empty error_message on hibernation timeout fail")
	}
}

// TestTaskWaker_HibernatedTimeout_RetryEscalation: a HIBERNATED row whose
// TimeoutMs has elapsed AND EscalationPolicy="retry" must be routed through
// WakeHibernatedTask (re-queue) instead of FailTask.
func TestTaskWaker_HibernatedTimeout_RetryEscalation(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	hib := &tasks.HibernationDescriptor{
		CheckpointKey:    "ckpt-retry",
		ResumeSessionID:  "sess-retry",
		EscalationPolicy: "retry",
	}
	task := buildHibernatedOrchestratedTask(t, ctx, store, "retry", hib, 0, 1)
	time.Sleep(10 * time.Millisecond)

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, nil)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Errorf("retry policy: status got %q want pending (re-queued)", got.Status)
	}
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationCheckpointKey].(string); v != "ckpt-retry" {
		t.Errorf("retry policy: metadata[%q] = %q want ckpt-retry",
			tasks.MetadataKeyHibernationCheckpointKey, v)
	}
	entries, err := store.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	var found bool
	for _, e := range entries {
		if e.TaskID == task.TaskID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("retry policy: expected new orchestrated_task_queue entry, got none")
	}
}

// TestTaskWaker_HibernatedTimeout_AlertEscalation: a HIBERNATED row whose
// TimeoutMs has elapsed AND EscalationPolicy="alert" must remain hibernated
// (Stage B logs only; no external alerting integration yet).
func TestTaskWaker_HibernatedTimeout_AlertEscalation(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	hib := &tasks.HibernationDescriptor{
		CheckpointKey:    "ckpt-alert",
		EscalationPolicy: "alert",
	}
	task := buildHibernatedOrchestratedTask(t, ctx, store, "alert", hib, 0, 1)
	time.Sleep(10 * time.Millisecond)

	svc := newWakerService(store)
	waker := NewTaskWaker(store, svc, nil)
	waker.scan(ctx)

	got, err := store.GetTask(ctx, task.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != tasks.TaskStatusHibernated {
		t.Errorf("alert policy: status got %q want hibernated (no state change)", got.Status)
	}
}
