// Feed B: unit tests for completion_event domain event emission on terminal
// transitions. Uses a recording DomainEventPublisher and the same sqlite-backed
// TaskAssignmentService harness as task_event_publisher_test.go.

package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	taskssqlite "github.com/scitrera/aether/server/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/server/pkg/tasks"

	_ "modernc.org/sqlite"
)

// fakeDomainEventPublisher records every PublishDomainEvent call (raw JSON
// EventPayload bytes). Thread-safe so emission from goroutines doesn't race the
// test assertions.
type fakeDomainEventPublisher struct {
	mu       sync.Mutex
	payloads [][]byte
}

func (f *fakeDomainEventPublisher) PublishDomainEvent(_ context.Context, _ string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	f.payloads = append(f.payloads, cp)
	return nil
}

func (f *fakeDomainEventPublisher) snapshot() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.payloads))
	copy(out, f.payloads)
	return out
}

// capturedEventPayload mirrors the workflow engine's Router.EventPayload shape;
// the JSON field tags must match exactly so a feed-B payload round-trips.
type capturedEventPayload struct {
	SourceAgent string         `json:"source_agent"`
	EventNames  []string       `json:"event_names"`
	Data        map[string]any `json:"data"`
	Workspace   string         `json:"workspace"`
}

// newCompletionEventTestService builds a sqlite-backed TaskAssignmentService
// with a recording DomainEventPublisher attached.
func newCompletionEventTestService(t *testing.T) (*TaskAssignmentService, *fakeDomainEventPublisher, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "feedb.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	store, err := taskssqlite.New(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("taskssqlite.New: %v", err)
	}
	pub := &fakeDomainEventPublisher{}
	svc := NewTaskAssignmentService(store, nil, nil, nil, nil)
	svc.SetDomainEventPublisher(pub)
	return svc, pub, func() { _ = db.Close() }
}

// createStartWithCompletion creates a task (carrying the given completion config,
// correlation/root ids) and drives it to running so the terminal call is legal.
func createStartWithCompletion(t *testing.T, svc *TaskAssignmentService, taskID, workspace string, cfg *tasks.TaskCompletionConfig) {
	t.Helper()
	ctx := context.Background()
	row := &tasks.ExtendedTask{
		TaskID:          taskID,
		TaskType:        "test",
		Workspace:       workspace,
		Status:          tasks.TaskStatusPending,
		CorrelationID:   "corr-" + taskID,
		RootTaskID:      "root-" + taskID,
		CompletionEvent: cfg,
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

// TestCompletionEvent_EmitsOnTerminal asserts an enabled completion_event on a
// terminal (completed) transition produces exactly one PublishDomainEvent call
// with the expected EventPayload shape.
func TestCompletionEvent_EmitsOnTerminal(t *testing.T) {
	svc, pub, cleanup := newCompletionEventTestService(t)
	defer cleanup()

	createStartWithCompletion(t, svc, "fb-complete", "ws1", &tasks.TaskCompletionConfig{
		Enabled:   true,
		EventName: "task.done",
	})

	if err := svc.CompleteTask(context.Background(), "fb-complete"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	payloads := pub.snapshot()
	if len(payloads) != 1 {
		t.Fatalf("expected exactly 1 domain event, got %d", len(payloads))
	}
	var ep capturedEventPayload
	if err := json.Unmarshal(payloads[0], &ep); err != nil {
		t.Fatalf("unmarshal EventPayload: %v", err)
	}
	if ep.SourceAgent != "orchestrator" {
		t.Errorf("source_agent: got %q, want %q", ep.SourceAgent, "orchestrator")
	}
	if ep.Workspace != "ws1" {
		t.Errorf("workspace: got %q, want %q", ep.Workspace, "ws1")
	}
	if len(ep.EventNames) != 1 || ep.EventNames[0] != "task.done" {
		t.Errorf("event_names: got %v, want [task.done]", ep.EventNames)
	}
	if got := ep.Data["task_id"]; got != "fb-complete" {
		t.Errorf("data.task_id: got %v, want fb-complete", got)
	}
	if got := ep.Data["status"]; got != "completed" {
		t.Errorf("data.status: got %v, want completed", got)
	}
	if got := ep.Data["correlation_id"]; got != "corr-fb-complete" {
		t.Errorf("data.correlation_id: got %v, want corr-fb-complete", got)
	}
	if got := ep.Data["root_task_id"]; got != "root-fb-complete" {
		t.Errorf("data.root_task_id: got %v, want root-fb-complete", got)
	}
}

// TestCompletionEvent_DefaultEventName checks the fallback name "task.<status>"
// when EventName is empty.
func TestCompletionEvent_DefaultEventName(t *testing.T) {
	svc, pub, cleanup := newCompletionEventTestService(t)
	defer cleanup()

	createStartWithCompletion(t, svc, "fb-default", "ws1", &tasks.TaskCompletionConfig{
		Enabled: true,
	})

	if err := svc.CompleteTask(context.Background(), "fb-default"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	payloads := pub.snapshot()
	if len(payloads) != 1 {
		t.Fatalf("expected exactly 1 domain event, got %d", len(payloads))
	}
	var ep capturedEventPayload
	if err := json.Unmarshal(payloads[0], &ep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(ep.EventNames) != 1 || ep.EventNames[0] != "task.completed" {
		t.Errorf("event_names: got %v, want [task.completed]", ep.EventNames)
	}
}

// TestCompletionEvent_NilDisabledNoEmit ensures a nil or disabled
// completion_event produces no domain event.
func TestCompletionEvent_NilDisabledNoEmit(t *testing.T) {
	svc, pub, cleanup := newCompletionEventTestService(t)
	defer cleanup()

	// nil completion_event
	createStartWithCompletion(t, svc, "fb-nil", "ws1", nil)
	if err := svc.CompleteTask(context.Background(), "fb-nil"); err != nil {
		t.Fatalf("CompleteTask nil: %v", err)
	}
	// disabled completion_event
	createStartWithCompletion(t, svc, "fb-disabled", "ws1", &tasks.TaskCompletionConfig{Enabled: false, EventName: "x"})
	if err := svc.CompleteTask(context.Background(), "fb-disabled"); err != nil {
		t.Fatalf("CompleteTask disabled: %v", err)
	}

	if got := len(pub.snapshot()); got != 0 {
		t.Fatalf("expected no domain events for nil/disabled config, got %d", got)
	}
}

// TestCompletionEvent_OnStatusesFilter asserts the on_statuses filter is
// respected: with OnStatuses=[COMPLETED], a FAILED transition does not emit and
// a COMPLETED transition does.
func TestCompletionEvent_OnStatusesFilter(t *testing.T) {
	svc, pub, cleanup := newCompletionEventTestService(t)
	defer cleanup()

	cfg := func() *tasks.TaskCompletionConfig {
		return &tasks.TaskCompletionConfig{
			Enabled:    true,
			EventName:  "task.gathered",
			OnStatuses: []tasks.TaskStatus{tasks.TaskStatusCompleted},
		}
	}

	// FAILED transition: filtered out, no emit.
	createStartWithCompletion(t, svc, "fb-fail", "ws1", cfg())
	if err := svc.FailTask(context.Background(), "fb-fail", "boom"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
	if got := len(pub.snapshot()); got != 0 {
		t.Fatalf("FAILED transition should not emit (filter=[completed]); got %d", got)
	}

	// COMPLETED transition: passes filter, exactly one emit.
	createStartWithCompletion(t, svc, "fb-ok", "ws1", cfg())
	if err := svc.CompleteTask(context.Background(), "fb-ok"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	payloads := pub.snapshot()
	if len(payloads) != 1 {
		t.Fatalf("expected exactly 1 domain event after COMPLETED, got %d", len(payloads))
	}
	var ep capturedEventPayload
	if err := json.Unmarshal(payloads[0], &ep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := ep.Data["task_id"]; got != "fb-ok" {
		t.Errorf("data.task_id: got %v, want fb-ok", got)
	}
	if got := ep.Data["status"]; got != "completed" {
		t.Errorf("data.status: got %v, want completed", got)
	}
}
