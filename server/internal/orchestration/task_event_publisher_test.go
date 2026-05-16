// Phase 4 Stage B: unit tests for the task-event publisher wiring on
// TaskAssignmentService. Uses a fake TaskEventPublisher to record emitted
// events without requiring a router. Backed by a sqlite task store for
// realistic transition behavior.

package orchestration

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	taskssqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/pkg/tasks"

	_ "modernc.org/sqlite"
)

// fakeTaskEventPublisher records every published event. Thread-safe so
// publishing from goroutines doesn't race the test assertions.
type fakeTaskEventPublisher struct {
	mu     sync.Mutex
	events []*pb.TaskEvent
}

func (f *fakeTaskEventPublisher) PublishTaskEvent(_ context.Context, _, _ string, event *pb.TaskEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}

func (f *fakeTaskEventPublisher) snapshot() []*pb.TaskEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.TaskEvent, len(f.events))
	copy(out, f.events)
	return out
}

// newPublisherTestService spins up a sqlite-backed TaskAssignmentService with
// a recording publisher attached. Caller owns the cleanup.
func newPublisherTestService(t *testing.T) (*TaskAssignmentService, *fakeTaskEventPublisher, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	store, err := taskssqlite.New(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("taskssqlite.New: %v", err)
	}
	pub := &fakeTaskEventPublisher{}
	svc := NewTaskAssignmentService(store, nil, nil, nil, nil)
	svc.SetEventPublisher(pub)
	return svc, pub, func() { _ = db.Close() }
}

func mustCreateAndStart(t *testing.T, svc *TaskAssignmentService, taskID, workspace string) {
	t.Helper()
	ctx := context.Background()
	row := &tasks.ExtendedTask{
		TaskID:    taskID,
		TaskType:  "test",
		Workspace: workspace,
		Status:    tasks.TaskStatusPending,
	}
	if err := svc.taskStore.CreateTask(ctx, row); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.taskStore.AssignTask(ctx, taskID, "ag::"+workspace+"::worker::v1"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	if err := svc.taskStore.StartTask(ctx, taskID); err != nil {
		t.Fatalf("StartTask: %v", err)
	}
}

// TestTaskEventPublisher_CompleteEmitsStatusChanged verifies CompleteTask
// publishes a TaskStatusChangedEvent{from=running, to=completed}.
func TestTaskEventPublisher_CompleteEmitsStatusChanged(t *testing.T) {
	svc, pub, cleanup := newPublisherTestService(t)
	defer cleanup()

	mustCreateAndStart(t, svc, "task-complete", "ws1")

	if err := svc.CompleteTask(context.Background(), "task-complete"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	events := pub.snapshot()
	if len(events) == 0 {
		t.Fatal("expected at least one published event")
	}
	// Find a status_changed event for our task.
	var found *pb.TaskStatusChangedEvent
	for _, e := range events {
		if e.GetTaskId() != "task-complete" {
			continue
		}
		if sc := e.GetStatusChanged(); sc != nil {
			found = sc
			break
		}
	}
	if found == nil {
		t.Fatal("expected a TaskStatusChangedEvent for task-complete")
	}
	if found.GetFromStatus() != pb.TaskStatus_TASK_STATUS_RUNNING {
		t.Errorf("from_status: got %v, want RUNNING", found.GetFromStatus())
	}
	if found.GetToStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("to_status: got %v, want COMPLETED", found.GetToStatus())
	}
}

// TestTaskEventPublisher_FailEmitsStatusChanged checks the failure path.
func TestTaskEventPublisher_FailEmitsStatusChanged(t *testing.T) {
	svc, pub, cleanup := newPublisherTestService(t)
	defer cleanup()

	mustCreateAndStart(t, svc, "task-fail", "ws1")

	if err := svc.FailTask(context.Background(), "task-fail", "boom"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
	events := pub.snapshot()
	var found *pb.TaskStatusChangedEvent
	for _, e := range events {
		if e.GetTaskId() == "task-fail" {
			if sc := e.GetStatusChanged(); sc != nil {
				found = sc
				break
			}
		}
	}
	if found == nil {
		t.Fatal("expected status_changed event for task-fail")
	}
	if found.GetToStatus() != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Errorf("to_status: got %v, want FAILED", found.GetToStatus())
	}
	if found.GetReason() != "boom" {
		t.Errorf("reason: got %q, want %q", found.GetReason(), "boom")
	}
}

// TestTaskEventPublisher_CancelEmitsStatusChanged checks cancel emission.
func TestTaskEventPublisher_CancelEmitsStatusChanged(t *testing.T) {
	svc, pub, cleanup := newPublisherTestService(t)
	defer cleanup()

	mustCreateAndStart(t, svc, "task-cancel", "ws1")

	if err := svc.CancelTask(context.Background(), "task-cancel"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	events := pub.snapshot()
	var found *pb.TaskStatusChangedEvent
	for _, e := range events {
		if e.GetTaskId() == "task-cancel" {
			if sc := e.GetStatusChanged(); sc != nil {
				found = sc
				break
			}
		}
	}
	if found == nil {
		t.Fatal("expected status_changed event for task-cancel")
	}
	if found.GetToStatus() != pb.TaskStatus_TASK_STATUS_CANCELLED {
		t.Errorf("to_status: got %v, want CANCELLED", found.GetToStatus())
	}
}

// TestTaskEventPublisher_RejectEmitsStatusChanged covers REJECT.
// Reject is "declined before processing", so we stay in assigned state and
// don't call StartTask — running -> rejected is not a legal transition.
func TestTaskEventPublisher_RejectEmitsStatusChanged(t *testing.T) {
	svc, pub, cleanup := newPublisherTestService(t)
	defer cleanup()

	ctx := context.Background()
	row := &tasks.ExtendedTask{
		TaskID:    "task-reject",
		TaskType:  "test",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
	}
	if err := svc.taskStore.CreateTask(ctx, row); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.taskStore.AssignTask(ctx, "task-reject", "ag::ws1::worker::v1"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}

	if err := svc.RejectTask(ctx, "task-reject", "no thanks"); err != nil {
		t.Fatalf("RejectTask: %v", err)
	}
	events := pub.snapshot()
	var found *pb.TaskStatusChangedEvent
	for _, e := range events {
		if e.GetTaskId() == "task-reject" {
			if sc := e.GetStatusChanged(); sc != nil {
				found = sc
				break
			}
		}
	}
	if found == nil {
		t.Fatal("expected status_changed event for task-reject")
	}
	if found.GetToStatus() != pb.TaskStatus_TASK_STATUS_REJECTED {
		t.Errorf("to_status: got %v, want REJECTED", found.GetToStatus())
	}
	if found.GetReason() != "no thanks" {
		t.Errorf("reason: got %q, want %q", found.GetReason(), "no thanks")
	}
}

// TestTaskEventPublisher_PauseResumeEmitsStatusChanged covers PAUSE + RESUME.
func TestTaskEventPublisher_PauseResumeEmitsStatusChanged(t *testing.T) {
	svc, pub, cleanup := newPublisherTestService(t)
	defer cleanup()

	mustCreateAndStart(t, svc, "task-pause", "ws1")

	spec := &tasks.WaitSpec{
		Reason:     tasks.WaitReasonInput,
		TimeoutMs:  0,
		DependsOn:  nil,
		WakeOnAny:  false,
		InputMatch: map[string]string{"key": "v"},
	}
	if err := svc.PauseTask(context.Background(), "task-pause", tasks.TaskStatusWaitingInput, spec); err != nil {
		t.Fatalf("PauseTask: %v", err)
	}
	// Resume back to running.
	if err := svc.ResumeTask(context.Background(), "task-pause", tasks.TaskStatusRunning); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}

	events := pub.snapshot()
	var pause, resume *pb.TaskStatusChangedEvent
	for _, e := range events {
		if e.GetTaskId() != "task-pause" {
			continue
		}
		sc := e.GetStatusChanged()
		if sc == nil {
			continue
		}
		if sc.GetToStatus() == pb.TaskStatus_TASK_STATUS_WAITING_INPUT && pause == nil {
			pause = sc
		}
		if sc.GetToStatus() == pb.TaskStatus_TASK_STATUS_RUNNING && resume == nil {
			resume = sc
		}
	}
	if pause == nil {
		t.Error("expected pause status_changed event")
	}
	if resume == nil {
		t.Error("expected resume status_changed event")
	}
}

// TestTaskEventPublisher_NilPublisherNoPanic ensures publishers are optional —
// a nil publisher must not break lifecycle operations.
func TestTaskEventPublisher_NilPublisherNoPanic(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nilpub.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	store, err := taskssqlite.New(db)
	if err != nil {
		t.Fatalf("taskssqlite.New: %v", err)
	}
	svc := NewTaskAssignmentService(store, nil, nil, nil, nil)
	// publisher intentionally not set

	mustCreateAndStart(t, svc, "no-pub", "ws1")
	if err := svc.CompleteTask(context.Background(), "no-pub"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
}

// TestTaskEventPublisher_ChildLifecycleEmittedOnTerminal verifies that a
// terminal transition on a child task fans a child_lifecycle event onto the
// parent task's event topic (recorded by the publisher with task_id=parent).
func TestTaskEventPublisher_ChildLifecycleEmittedOnTerminal(t *testing.T) {
	svc, pub, cleanup := newPublisherTestService(t)
	defer cleanup()

	ctx := context.Background()
	parent := &tasks.ExtendedTask{
		TaskID:    "parent-1",
		TaskType:  "test",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
	}
	if err := svc.taskStore.CreateTask(ctx, parent); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	child := &tasks.ExtendedTask{
		TaskID:       "child-1",
		TaskType:     "test",
		Workspace:    "ws1",
		Status:       tasks.TaskStatusPending,
		ParentTaskID: "parent-1",
	}
	if err := svc.taskStore.CreateTask(ctx, child); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if err := svc.taskStore.AssignTask(ctx, "child-1", "ag::ws1::worker::v1"); err != nil {
		t.Fatalf("AssignTask child: %v", err)
	}
	if err := svc.taskStore.StartTask(ctx, "child-1"); err != nil {
		t.Fatalf("StartTask child: %v", err)
	}

	if err := svc.CompleteTask(ctx, "child-1"); err != nil {
		t.Fatalf("CompleteTask child: %v", err)
	}

	events := pub.snapshot()
	var childLifecycleFound bool
	for _, e := range events {
		if e.GetTaskId() == "parent-1" {
			if cl := e.GetChildLifecycle(); cl != nil {
				if cl.GetChildTaskId() == "child-1" &&
					cl.GetChildStatus() == pb.TaskStatus_TASK_STATUS_COMPLETED &&
					cl.GetLifecycle() == "completed" {
					childLifecycleFound = true
					break
				}
			}
		}
	}
	if !childLifecycleFound {
		t.Errorf("expected child_lifecycle event on parent for completed child; got %d events total", len(events))
		for _, e := range events {
			t.Logf("  event: task_id=%s type=%T", e.GetTaskId(), e.GetEvent())
		}
	}
}
