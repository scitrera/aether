package gateway

// Tests for idempotent task creation (CreateTaskRequest.idempotency_key).
// A non-empty key dedups creation: the first create persists a task, any
// subsequent create with the same key is suppressed and echoes the original
// task's identity. An empty key preserves the prior behavior (no dedup).

import (
	"context"
	"errors"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/orchestration"
	"github.com/scitrera/aether/server/pkg/tasks"
)

var errTestKV = errors.New("simulated kv outage")

// newIdemTestServer builds a gateway with a real (sqlite-backed) self-assign
// task service and a fresh mock KV so handleCreateTask exercises the actual
// CreateTask persistence + idempotency ledger paths. SessionRegistry is nil:
// the self_assign path never derefs it.
func newIdemTestServer(t *testing.T) (*GatewayServer, *mockKVReadWriter, func()) {
	t.Helper()
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	store := newMockKVReadWriter()
	s.kv = store
	s.orchestration = &OrchestrationServices{
		TaskService: orchestration.NewTaskAssignmentService(s.taskStore, nil, nil, nil, nil),
	}
	return s, store, cleanup
}

func countWorkspaceTasks(t *testing.T, s *GatewayServer, workspace string) int {
	t.Helper()
	list, err := s.taskStore.ListTasks(context.Background(), &tasks.TaskFilter{Workspace: workspace})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	return len(list)
}

// TestHandleCreateTask_IdempotencyKey_DedupsCreation asserts that two creates
// carrying the SAME non-empty idempotency_key produce exactly ONE task, and
// the duplicate's response echoes the original task_id.
func TestHandleCreateTask_IdempotencyKey_DedupsCreation(t *testing.T) {
	s, _, cleanup := newIdemTestServer(t)
	defer cleanup()

	caller := callerIdentity("worker", "alice")
	ctx := context.Background()

	newReq := func() *pb.CreateTaskRequest {
		return &pb.CreateTaskRequest{
			TaskType:       "test",
			Workspace:      "ws1",
			AssignmentMode: pb.TaskAssignmentMode_SELF_ASSIGN,
			IdempotencyKey: "join-fire-abc",
			RequestId:      "req-1",
		}
	}

	// First create: proceeds, persists one task.
	stream1 := &mockStream{}
	client1 := newTaskTestClient(stream1, caller)
	if err := s.handleCreateTask(ctx, client1, caller, newReq()); err != nil {
		t.Fatalf("first handleCreateTask: %v", err)
	}
	resp1 := stream1.sent[0].GetCreateTask()
	if resp1 == nil || !resp1.Success || resp1.TaskId == "" {
		t.Fatalf("first create: expected success with task_id, got %+v", resp1)
	}
	if n := countWorkspaceTasks(t, s, "ws1"); n != 1 {
		t.Fatalf("after first create: expected 1 task, got %d", n)
	}

	// Second create with the SAME key: suppressed, no second task, echoes id.
	stream2 := &mockStream{}
	client2 := newTaskTestClient(stream2, caller)
	dupReq := newReq()
	dupReq.RequestId = "req-2"
	if err := s.handleCreateTask(ctx, client2, caller, dupReq); err != nil {
		t.Fatalf("duplicate handleCreateTask: %v", err)
	}
	if n := countWorkspaceTasks(t, s, "ws1"); n != 1 {
		t.Fatalf("after duplicate create: expected still 1 task, got %d", n)
	}
	resp2 := stream2.sent[0].GetCreateTask()
	if resp2 == nil || !resp2.Success {
		t.Fatalf("duplicate create: expected success response, got %+v", resp2)
	}
	if resp2.TaskId != resp1.TaskId {
		t.Errorf("duplicate create: expected echoed task_id %q, got %q", resp1.TaskId, resp2.TaskId)
	}
	if resp2.RequestId != "req-2" {
		t.Errorf("duplicate create: expected RequestId echoed, got %q", resp2.RequestId)
	}
}

// TestHandleCreateTask_DistinctIdempotencyKeys_BothCreate confirms that two
// creates with DIFFERENT keys each produce their own task.
func TestHandleCreateTask_DistinctIdempotencyKeys_BothCreate(t *testing.T) {
	s, _, cleanup := newIdemTestServer(t)
	defer cleanup()

	caller := callerIdentity("worker", "alice")
	ctx := context.Background()

	for i, key := range []string{"key-a", "key-b"} {
		stream := &mockStream{}
		client := newTaskTestClient(stream, caller)
		req := &pb.CreateTaskRequest{
			TaskType:       "test",
			Workspace:      "ws1",
			AssignmentMode: pb.TaskAssignmentMode_SELF_ASSIGN,
			IdempotencyKey: key,
		}
		if err := s.handleCreateTask(ctx, client, caller, req); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	if n := countWorkspaceTasks(t, s, "ws1"); n != 2 {
		t.Fatalf("distinct keys: expected 2 tasks, got %d", n)
	}
}

// TestHandleCreateTask_EmptyIdempotencyKey_NoDedup confirms empty keys are not
// deduped: two creates yield two tasks, matching the pre-feature behavior.
func TestHandleCreateTask_EmptyIdempotencyKey_NoDedup(t *testing.T) {
	s, _, cleanup := newIdemTestServer(t)
	defer cleanup()

	caller := callerIdentity("worker", "alice")
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		stream := &mockStream{}
		client := newTaskTestClient(stream, caller)
		req := &pb.CreateTaskRequest{
			TaskType:       "test",
			Workspace:      "ws1",
			AssignmentMode: pb.TaskAssignmentMode_SELF_ASSIGN,
			// IdempotencyKey intentionally empty
		}
		if err := s.handleCreateTask(ctx, client, caller, req); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	if n := countWorkspaceTasks(t, s, "ws1"); n != 2 {
		t.Fatalf("empty key: expected 2 tasks (no dedup), got %d", n)
	}
}

// TestHandleCreateTask_IdempotencyKVError_FailsOpen confirms a KV SetNX failure
// does NOT block task creation (fail-open posture).
func TestHandleCreateTask_IdempotencyKVError_FailsOpen(t *testing.T) {
	s, store, cleanup := newIdemTestServer(t)
	defer cleanup()
	store.setErr = errTestKV

	caller := callerIdentity("worker", "alice")
	ctx := context.Background()

	stream := &mockStream{}
	client := newTaskTestClient(stream, caller)
	req := &pb.CreateTaskRequest{
		TaskType:       "test",
		Workspace:      "ws1",
		AssignmentMode: pb.TaskAssignmentMode_SELF_ASSIGN,
		IdempotencyKey: "kv-down",
		RequestId:      "req-fo",
	}
	if err := s.handleCreateTask(ctx, client, caller, req); err != nil {
		t.Fatalf("handleCreateTask (KV down): %v", err)
	}
	resp := stream.sent[0].GetCreateTask()
	if resp == nil || !resp.Success || resp.TaskId == "" {
		t.Fatalf("fail-open: expected successful create despite KV error, got %+v", resp)
	}
	if n := countWorkspaceTasks(t, s, "ws1"); n != 1 {
		t.Fatalf("fail-open: expected 1 task, got %d", n)
	}
}
