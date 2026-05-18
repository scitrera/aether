package integration

import (
	"context"
	"time"

	taskstore "github.com/scitrera/aether/internal/storage/tasks"
)

// fakeTaskStore is a minimal taskstore.Store implementation used by the
// integration tests in this package. The JetStreamTaskDispatcher's
// Publish→Consume happy path does not touch the Store at all — only
// ClaimTask / UnclaimTask / CompleteTask / FailTask / GetTaskDetails /
// RecoverStaleClaims do. The tests in this package never exercise those
// paths (they verify message delivery and split semantics), so any unused
// method panics if called to surface unexpected interactions early.
//
// This mirrors the fake in internal/orchestration/jetstream_dispatcher_test.go
// — we duplicate the boilerplate rather than depend on it because that
// helper is in an internal test package and integration tests cannot
// import _test.go files.
type fakeTaskStore struct {
	queue map[string]*fakeQueueRow
}

type fakeQueueRow struct{}

func newFakeTaskStore() *fakeTaskStore {
	return &fakeTaskStore{queue: make(map[string]*fakeQueueRow)}
}

// --- Methods plausibly reachable from JetStreamTaskDispatcher in tests ---
// These return benign defaults so the dispatcher's housekeeping paths don't
// fail noisily even if they happen to fire under contention.

func (f *fakeTaskStore) ClaimQueueEntry(ctx context.Context, queueID, claimedBy string) (bool, error) {
	return true, nil
}
func (f *fakeTaskStore) CompleteQueueEntry(_ context.Context, _ string) error { return nil }
func (f *fakeTaskStore) FailQueueEntry(_ context.Context, _, _ string) error  { return nil }
func (f *fakeTaskStore) CompleteQueueEntryByTaskID(_ context.Context, _ string) error {
	return nil
}
func (f *fakeTaskStore) FailQueueEntryByTaskID(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeTaskStore) GetQueueEntryDetails(_ context.Context, queueID string) (*taskstore.QueueEntryDetails, error) {
	return &taskstore.QueueEntryDetails{
		TaskID:               queueID,
		TargetImplementation: "test-impl",
		Workspace:            "test-ws",
		Profile:              "test-profile",
	}, nil
}
func (f *fakeTaskStore) ListStaleClaimedQueueEntries(_ context.Context, _ time.Duration, _ int) ([]string, error) {
	return nil, nil
}
func (f *fakeTaskStore) BeginTx(_ context.Context) (taskstore.StoreTx, error) {
	return &noopTx{}, nil
}
func (f *fakeTaskStore) QueryQueueEntryForUnclaimTx(_ context.Context, _ taskstore.StoreTx, _ string) (string, string, int, int, error) {
	return "", "", 0, 3, nil
}
func (f *fakeTaskStore) UpdateQueueEntryForRetryTx(_ context.Context, _ taskstore.StoreTx, _ string, _, _ int) error {
	return nil
}
func (f *fakeTaskStore) MarkQueueEntryFailedTx(_ context.Context, _ taskstore.StoreTx, _, _ string) error {
	return nil
}
func (f *fakeTaskStore) InsertDLQEntryTx(_ context.Context, _ taskstore.StoreTx, _, _, _ string, _ int) error {
	return nil
}
func (f *fakeTaskStore) RecordAuditEventTx(_ context.Context, _ taskstore.StoreTx, _ *taskstore.TaskAuditEvent) error {
	return nil
}
func (f *fakeTaskStore) CountPendingQueueEntries(_ context.Context) (int, error) { return 0, nil }

// --- Methods that should never be touched in these tests; panic to surface
//     unexpected calls during integration ---

func (f *fakeTaskStore) InsertQueueEntry(_ context.Context, _, _, _, _, _ string, _ []byte) error {
	panic("InsertQueueEntry unexpected in cluster integration tests")
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

// noopTx implements taskstore.StoreTx with no-op commit/rollback.
type noopTx struct{}

func (n *noopTx) Commit() error   { return nil }
func (n *noopTx) Rollback() error { return nil }

// Compile-time assertion that fakeTaskStore satisfies the taskstore.Store
// interface. If new methods are added to the interface, this will fail to
// compile so we know to extend the fake.
var _ taskstore.Store = (*fakeTaskStore)(nil)
