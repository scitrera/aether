package orchestration

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// startTestNATSServer boots an in-process NATS server with JetStream enabled
// on an ephemeral port and returns a connected JetStream context.
func startTestNATSServer(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()

	opts := &natsserver.Options{
		Port:               -1,
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 64 * 1024 * 1024,
		JetStreamMaxStore:  256 * 1024 * 1024,
		NoSigs:             true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server new: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not become ready")
	}

	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		t.Fatalf("jetstream new: %v", err)
	}

	stop := func() {
		_ = conn.Drain()
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	}
	return js, stop
}

// ---------------------------------------------------------------------------
// Minimal fake task store
// ---------------------------------------------------------------------------

// fakeTaskStore implements taskstore.Store with only the methods called by
// JetStreamTaskDispatcher; all others panic to catch unexpected calls.
type fakeTaskStore struct {
	mu    sync.Mutex
	queue map[string]*fakeQueueRow
}

type fakeQueueRow struct {
	taskID     string
	workspace  string
	retryCount int
	maxRetries int
	status     string
	claimedBy  string
}

func newFakeTaskStore() *fakeTaskStore {
	return &fakeTaskStore{queue: make(map[string]*fakeQueueRow)}
}

// --- Methods used by JetStreamTaskDispatcher ---

func (f *fakeTaskStore) ClaimQueueEntry(ctx context.Context, queueID, claimedBy string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.queue[queueID]
	if !ok {
		return false, nil
	}
	if row.status != "pending" {
		return false, nil
	}
	row.status = "claimed"
	row.claimedBy = claimedBy
	return true, nil
}

func (f *fakeTaskStore) CompleteQueueEntry(ctx context.Context, queueID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row, ok := f.queue[queueID]; ok {
		row.status = "completed"
	}
	return nil
}

func (f *fakeTaskStore) FailQueueEntry(ctx context.Context, queueID, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row, ok := f.queue[queueID]; ok {
		row.status = "failed"
	}
	return nil
}

func (f *fakeTaskStore) GetQueueEntryDetails(ctx context.Context, queueID string) (*taskstore.QueueEntryDetails, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.queue[queueID]
	if !ok {
		return nil, fmt.Errorf("not found: %s", queueID)
	}
	return &taskstore.QueueEntryDetails{
		TaskID:               row.taskID,
		TargetImplementation: "test-impl",
		Workspace:            row.workspace,
		Profile:              "test-profile",
	}, nil
}

func (f *fakeTaskStore) ListStaleClaimedQueueEntries(ctx context.Context, threshold time.Duration, limit int) ([]string, error) {
	return nil, nil
}

func (f *fakeTaskStore) BeginTx(ctx context.Context) (taskstore.StoreTx, error) {
	return &fakeTx{}, nil
}

func (f *fakeTaskStore) QueryQueueEntryForUnclaimTx(ctx context.Context, tx taskstore.StoreTx, queueID string) (string, string, int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.queue[queueID]
	if !ok {
		return "", "", 0, 0, fmt.Errorf("not found: %s", queueID)
	}
	return row.taskID, row.workspace, row.retryCount, row.maxRetries, nil
}

func (f *fakeTaskStore) UpdateQueueEntryForRetryTx(ctx context.Context, tx taskstore.StoreTx, queueID string, newRetryCount, backoffSeconds int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row, ok := f.queue[queueID]; ok {
		row.retryCount = newRetryCount
		row.status = "pending"
	}
	return nil
}

func (f *fakeTaskStore) MarkQueueEntryFailedTx(ctx context.Context, tx taskstore.StoreTx, queueID, errorMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row, ok := f.queue[queueID]; ok {
		row.status = "failed"
	}
	return nil
}

func (f *fakeTaskStore) InsertDLQEntryTx(ctx context.Context, tx taskstore.StoreTx, taskID, workspace, reason string, attemptCount int) error {
	return nil
}

func (f *fakeTaskStore) RecordAuditEventTx(ctx context.Context, tx taskstore.StoreTx, event *taskstore.TaskAuditEvent) error {
	return nil
}

func (f *fakeTaskStore) CountPendingQueueEntries(ctx context.Context) (int, error) { return 0, nil }
func (f *fakeTaskStore) CompleteQueueEntryByTaskID(ctx context.Context, taskID string) error {
	return nil
}
func (f *fakeTaskStore) FailQueueEntryByTaskID(ctx context.Context, taskID, errorMsg string) error {
	return nil
}

// --- Unused methods (panic to surface unexpected calls) ---

func (f *fakeTaskStore) InsertQueueEntry(_ context.Context, _, _, _, _, _ string, _ []byte) error {
	panic("InsertQueueEntry unexpected in jetstream dispatcher tests")
}
func (f *fakeTaskStore) PollPendingQueueEntries(_ context.Context, _ int) ([]*taskstore.QueueEntryNotification, error) {
	panic("PollPendingQueueEntries unexpected")
}
func (f *fakeTaskStore) CreateTask(_ context.Context, _ *taskstore.Task) error { panic("unexpected") }
func (f *fakeTaskStore) GetTask(_ context.Context, _ string) (*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) UpdateTaskStatus(_ context.Context, _ string, _ taskstore.TaskStatus) error {
	panic("unexpected")
}
func (f *fakeTaskStore) UpdateTaskMetadata(_ context.Context, _ string, _ map[string]interface{}) error {
	panic("unexpected")
}
func (f *fakeTaskStore) UpdateTaskAuthority(_ context.Context, _ string, _ taskstore.TaskAuthorityInfo, _ map[string]interface{}) error {
	panic("unexpected")
}
func (f *fakeTaskStore) AssignTask(_ context.Context, _, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) StartingTask(_ context.Context, _ string) error  { panic("unexpected") }
func (f *fakeTaskStore) StartTask(_ context.Context, _ string) error     { panic("unexpected") }
func (f *fakeTaskStore) StartTaskWithAgent(_ context.Context, _, _ string) error {
	panic("unexpected")
}
func (f *fakeTaskStore) CompleteTask(_ context.Context, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) FailTask(_ context.Context, _, _ string) error  { panic("unexpected") }
func (f *fakeTaskStore) FailTaskWithRetry(_ context.Context, _, _, _ string, _ *time.Time) error {
	panic("unexpected")
}
func (f *fakeTaskStore) CancelTask(_ context.Context, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) RetryTask(_ context.Context, _ string) error  { panic("unexpected") }
func (f *fakeTaskStore) RescheduleTaskAt(_ context.Context, _ string, _ time.Time) error {
	panic("unexpected")
}
func (f *fakeTaskStore) ListTasks(_ context.Context, _ *taskstore.TaskFilter) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) ListTasksPage(_ context.Context, _ *taskstore.TaskFilter) ([]*taskstore.Task, string, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetTasksByStatus(_ context.Context, _ taskstore.TaskStatus, _ int) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetQueuedTasksForAgent(_ context.Context, _ string) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetWorkspaceTasks(_ context.Context, _ string, _ bool) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetAgentTasks(_ context.Context, _ string) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetTasksNeedingRetry(_ context.Context, _ time.Time, _ int) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetTaskCounts(_ context.Context) (*taskstore.TaskCounts, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) MarkTaskNotQueued(_ context.Context, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) ClaimPoolTask(_ context.Context, _, _ string) (bool, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) UnassignPoolTask(_ context.Context, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) GetPendingPoolTasks(_ context.Context, _, _ string) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) HasActiveStartupTask(_ context.Context, _, _, _ string) (bool, string, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) UpdateCheckpoint(_ context.Context, _ string, _ map[string]interface{}) error {
	panic("unexpected")
}
func (f *fakeTaskStore) UpdateHeartbeat(_ context.Context, _ string, _ map[string]interface{}) error {
	panic("unexpected")
}
func (f *fakeTaskStore) CreateCheckpoint(_ context.Context, _ *taskstore.CheckpointRecord) error {
	panic("unexpected")
}
func (f *fakeTaskStore) GetLatestCheckpoint(_ context.Context, _ string) (*taskstore.CheckpointRecord, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) CreateTimer(_ context.Context, _ *taskstore.TimerRecord) error {
	panic("unexpected")
}
func (f *fakeTaskStore) GetTimer(_ context.Context, _ string) (*taskstore.TimerRecord, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetTimersForTask(_ context.Context, _ string) ([]*taskstore.TimerRecord, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetPendingTimers(_ context.Context, _ time.Time, _ int) ([]*taskstore.TimerRecord, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) MarkTimerFired(_ context.Context, _ string) error      { panic("unexpected") }
func (f *fakeTaskStore) DeleteTimer(_ context.Context, _ string) error         { panic("unexpected") }
func (f *fakeTaskStore) DeleteTimersForTask(_ context.Context, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) RecordAssignment(_ context.Context, _ *taskstore.AssignmentRecord) error {
	panic("unexpected")
}
func (f *fakeTaskStore) GetAssignmentHistory(_ context.Context, _ string) ([]*taskstore.AssignmentRecord, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) RecordAuditEvent(_ context.Context, _ *taskstore.TaskAuditEvent) error {
	panic("unexpected")
}
func (f *fakeTaskStore) GetTaskAuditEvents(_ context.Context, _ string) ([]*taskstore.TaskAuditEvent, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) MarkTaskDisconnected(_ context.Context, _ string, _ time.Time) error {
	panic("unexpected")
}
func (f *fakeTaskStore) ClearTaskDisconnected(_ context.Context, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) ListDisconnectedTasks(_ context.Context, _ int) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) PauseTask(_ context.Context, _ string, _ taskstore.TaskStatus, _ *taskstore.WaitSpec) error {
	panic("unexpected")
}
func (f *fakeTaskStore) ResumeTask(_ context.Context, _ string, _ taskstore.TaskStatus) error {
	panic("unexpected")
}
func (f *fakeTaskStore) RejectTask(_ context.Context, _ string, _ string) error { panic("unexpected") }
func (f *fakeTaskStore) ListWaitingTasks(_ context.Context, _ int) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) ListTasksWaitingOnDependency(_ context.Context, _ string) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) ListTasksByContext(_ context.Context, _ string, _ int) ([]*taskstore.Task, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) WriteToDLQ(_ context.Context, _ *taskstore.DLQRecord) error {
	panic("unexpected")
}
func (f *fakeTaskStore) GetDLQTasks(_ context.Context, _, _ string, _, _ int) ([]*taskstore.DLQRecord, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) ListDistinctTaskWorkspaces(_ context.Context) ([]*taskstore.WorkspaceTaskSummary, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) GetWorkspaceTaskStats(_ context.Context, _ string) (*taskstore.WorkspaceTaskStats, error) {
	panic("unexpected")
}
func (f *fakeTaskStore) PurgeOldTasks(_ context.Context, _, _, _ time.Duration) (*taskstore.PurgeResult, error) {
	panic("unexpected")
}

// fakeTx is a no-op StoreTx used by the fake store.
type fakeTx struct{}

func (t *fakeTx) Commit() error   { return nil }
func (t *fakeTx) Rollback() error { return nil }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestJetStreamDispatcher_SingleTask_Delivered publishes one task notification
// and asserts the callback is invoked exactly once.
func TestJetStreamDispatcher_SingleTask_Delivered(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store := newFakeTaskStore()
	d, err := NewJetStreamTaskDispatcher(ctx, js, "gw-test-single", 1, store)
	if err != nil {
		t.Fatalf("NewJetStreamTaskDispatcher: %v", err)
	}

	var received atomic.Int64
	done := make(chan struct{})
	d.SetCallback(func(task *OrchestrationTaskNotification) {
		if received.Add(1) == 1 {
			close(done)
		}
	})

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	task := &OrchestrationTaskNotification{
		QueueID:              "q-001",
		TaskID:               "t-001",
		Profile:              "k8s",
		Workspace:            "ws-a",
		TargetImplementation: "my-agent",
	}
	if err := d.PublishTask(ctx, task); err != nil {
		t.Fatalf("PublishTask: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for task delivery")
	}

	if n := received.Load(); n != 1 {
		t.Errorf("expected exactly 1 delivery, got %d", n)
	}
}

// TestJetStreamDispatcher_TwoDispatchersOnSameStream_ExactlyOnce spins up two
// dispatchers with different gateway IDs (= different durable consumer names)
// against the same work-queue stream and verifies that 100 published tasks are
// each consumed exactly once across both dispatchers combined.
func TestJetStreamDispatcher_TwoDispatchersOnSameStream_ExactlyOnce(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := newFakeTaskStore()

	dA, err := NewJetStreamTaskDispatcher(ctx, js, "gw-a", 1, store)
	if err != nil {
		t.Fatalf("dispatcher A: %v", err)
	}
	dB, err := NewJetStreamTaskDispatcher(ctx, js, "gw-b", 1, store)
	if err != nil {
		t.Fatalf("dispatcher B: %v", err)
	}

	const total = 100
	var (
		mu      sync.Mutex
		seen    = make(map[string]int)
		counter atomic.Int64
	)

	handler := func(task *OrchestrationTaskNotification) {
		mu.Lock()
		seen[task.QueueID]++
		mu.Unlock()
		counter.Add(1)
	}
	dA.SetCallback(handler)
	dB.SetCallback(handler)

	if err := dA.Start(ctx); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer dA.Stop()
	if err := dB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer dB.Stop()

	for i := 0; i < total; i++ {
		task := &OrchestrationTaskNotification{
			QueueID:   fmt.Sprintf("q-%04d", i),
			TaskID:    fmt.Sprintf("t-%04d", i),
			Profile:   "k8s",
			Workspace: "ws-b",
		}
		if err := dA.PublishTask(ctx, task); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Wait for all 100 to be consumed.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if counter.Load() >= int64(total) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := counter.Load(); got != int64(total) {
		t.Errorf("expected %d total deliveries, got %d", total, got)
	}

	mu.Lock()
	defer mu.Unlock()
	for queueID, count := range seen {
		if count != 1 {
			t.Errorf("task %s delivered %d times (want 1)", queueID, count)
		}
	}
	if len(seen) != total {
		t.Errorf("expected %d distinct tasks, got %d", total, len(seen))
	}
}

// TestJetStreamDispatcher_GracefulShutdown starts a dispatcher, sends 10
// tasks, cancels the context, and verifies the dispatcher's own goroutine
// (watchStop) exits cleanly. We measure the delta introduced by Start() and
// verify it returns to zero after Stop().
func TestJetStreamDispatcher_GracefulShutdown(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	store := newFakeTaskStore()
	d, err := NewJetStreamTaskDispatcher(ctx, js, "gw-shutdown", 1, store)
	if err != nil {
		cancel()
		t.Fatalf("NewJetStreamTaskDispatcher: %v", err)
	}

	var received atomic.Int64
	d.SetCallback(func(task *OrchestrationTaskNotification) {
		received.Add(1)
	})

	// Take baseline before Start() so we can detect goroutines added by the
	// dispatcher itself (the watchStop goroutine).
	beforeStart := runtime.NumGoroutine()

	if err := d.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}

	// After Start(), at minimum the watchStop goroutine is running.
	afterStart := runtime.NumGoroutine()
	if afterStart <= beforeStart {
		t.Logf("note: goroutine count did not increase after Start (%d→%d); NATS may pool goroutines", beforeStart, afterStart)
	}

	const count = 10
	for i := 0; i < count; i++ {
		task := &OrchestrationTaskNotification{
			QueueID: fmt.Sprintf("q-sd-%d", i),
			TaskID:  fmt.Sprintf("t-sd-%d", i),
			Profile: "k8s",
		}
		if err := d.PublishTask(ctx, task); err != nil {
			t.Logf("publish %d: %v (context may have been cancelled)", i, err)
		}
	}

	// Allow messages to be delivered before cancelling.
	time.Sleep(300 * time.Millisecond)

	cancel()
	d.Stop()

	// The dispatcher's watchStop goroutine must have exited (d.wg.Wait() in
	// Stop() guarantees this). Verify goroutine count returns to ≤ beforeStart+5
	// within a generous window (NATS client may have its own background goroutines
	// that are independent of the dispatcher and persist for the connection lifetime).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= beforeStart+5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	afterStop := runtime.NumGoroutine()
	if afterStop > beforeStart+5 {
		t.Errorf("dispatcher goroutine leak: before Start=%d after Stop=%d (delta %d, want ≤5)",
			beforeStart, afterStop, afterStop-beforeStart)
	}
}
