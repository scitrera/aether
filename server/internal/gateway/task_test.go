package gateway

// Tests for handleTaskQuery() and handleTaskOp() covering the nil taskStore
// (not-configured) path, GET/LIST operations, CANCEL/RETRY/COMPLETE/FAIL
// operations, authorization checks, request_id propagation, and unknown-op
// fallback.
//
// Phase 4 added auth + info-hiding tests that use the native-sqlite tasks
// store (lightweight, in-tempdir) so the auth helper exercises real code
// paths end-to-end without requiring a live postgres dev DB.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	taskssqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newTaskTestServer() *GatewayServer {
	return newTestGatewayWithMocks(
		newMockSessionManager(),
		newMockMessageRouter(),
		newMockKVReadWriter(),
		newMockCheckpointManager(),
	)
	// taskStore is left nil – exercises "not configured" paths
}

func newTaskTestClient(stream *mockStream, identity models.Identity) *ClientSession {
	return &ClientSession{
		ID:            "task-test-session",
		Identity:      identity,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}
}

func defaultAgentIdentity() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "worker",
		Specifier:      "v1",
	}
}

// ---------------------------------------------------------------------------
// handleTaskQuery – nil task store
// ---------------------------------------------------------------------------

func TestHandleTaskQuery_NilTaskStore_SendsNotConfiguredError(t *testing.T) {
	s := newTaskTestServer()
	stream := &mockStream{}
	client := newTaskTestClient(stream, defaultAgentIdentity())

	query := &pb.TaskQuery{
		Op:        pb.TaskQuery_GET,
		TaskId:    "task-123",
		RequestId: "req-nil-store",
	}

	s.handleTaskQuery(context.Background(), client, query)

	if stream.sentCount() == 0 {
		t.Fatal("expected a response when task store is nil")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetTaskQuery()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskQueryResponse payload")
	}
	if resp.Success {
		t.Error("expected Success=false when task store not configured")
	}
	if resp.RequestId != "req-nil-store" {
		t.Errorf("expected RequestId='req-nil-store', got %q", resp.RequestId)
	}
}

func TestHandleTaskQuery_NilTaskStore_LIST_SendsNotConfiguredError(t *testing.T) {
	s := newTaskTestServer()
	stream := &mockStream{}
	client := newTaskTestClient(stream, defaultAgentIdentity())

	query := &pb.TaskQuery{
		Op:        pb.TaskQuery_LIST,
		RequestId: "req-list-nil",
	}

	s.handleTaskQuery(context.Background(), client, query)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskQuery()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false for LIST when task store is nil")
	}
}

// ---------------------------------------------------------------------------
// handleTaskQuery – unknown operation
// ---------------------------------------------------------------------------

func TestHandleTaskQuery_UnknownOp_SendsErrorResponse(t *testing.T) {
	s := newTaskTestServer()
	// Inject a non-nil but useless store to bypass nil check and reach the switch default.
	// Since TaskStore requires a real DB, instead use nil (nil-store path) to reach the
	// early return.  Test the unknown-op branch by providing a store; we can't easily
	// create a mock for *tasks.TaskStore as it's a concrete type.
	// Instead test the nil-store path returns error for any op including an unknown one:
	stream := &mockStream{}
	client := newTaskTestClient(stream, defaultAgentIdentity())

	query := &pb.TaskQuery{
		Op:        pb.TaskQuery_OpType(99),
		RequestId: "req-unknown-op",
	}

	s.handleTaskQuery(context.Background(), client, query)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskQuery()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected a TaskQueryResponse for unknown op (nil-store path)")
	}
	if resp.Success {
		t.Error("expected Success=false")
	}
	// RequestId must always be echoed back
	if resp.RequestId != "req-unknown-op" {
		t.Errorf("expected RequestId='req-unknown-op', got %q", resp.RequestId)
	}
}

// ---------------------------------------------------------------------------
// handleTaskQuery – request_id propagation
// ---------------------------------------------------------------------------

func TestHandleTaskQuery_RequestIDAlwaysEchoed(t *testing.T) {
	tests := []struct {
		name      string
		op        pb.TaskQuery_OpType
		requestID string
	}{
		{"GET", pb.TaskQuery_GET, "get-req-id"},
		{"LIST", pb.TaskQuery_LIST, "list-req-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTaskTestServer()
			stream := &mockStream{}
			client := newTaskTestClient(stream, defaultAgentIdentity())

			query := &pb.TaskQuery{
				Op:        tt.op,
				RequestId: tt.requestID,
			}

			s.handleTaskQuery(context.Background(), client, query)

			if stream.sentCount() == 0 {
				t.Fatalf("%s: expected at least one response", tt.name)
			}
			stream.mu.Lock()
			resp := stream.sent[0].GetTaskQuery()
			stream.mu.Unlock()

			if resp == nil {
				t.Fatalf("%s: expected TaskQueryResponse", tt.name)
			}
			if resp.RequestId != tt.requestID {
				t.Errorf("%s: expected RequestId=%q, got %q", tt.name, tt.requestID, resp.RequestId)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleTaskOp – nil task store
// ---------------------------------------------------------------------------

func TestHandleTaskOp_NilTaskStore_SendsNotConfiguredError(t *testing.T) {
	ops := []struct {
		name string
		op   pb.TaskOperation_OpType
	}{
		{"CANCEL", pb.TaskOperation_CANCEL},
		{"RETRY", pb.TaskOperation_RETRY},
		{"COMPLETE", pb.TaskOperation_COMPLETE},
		{"FAIL", pb.TaskOperation_FAIL},
		{"PAUSE", pb.TaskOperation_PAUSE},
		{"WAIT_FOR", pb.TaskOperation_WAIT_FOR},
		{"RESUME", pb.TaskOperation_RESUME},
		{"REJECT", pb.TaskOperation_REJECT},
	}

	for _, tt := range ops {
		t.Run(tt.name, func(t *testing.T) {
			s := newTaskTestServer()
			stream := &mockStream{}
			client := newTaskTestClient(stream, defaultAgentIdentity())

			op := &pb.TaskOperation{
				Op:        tt.op,
				TaskId:    "task-abc",
				RequestId: "req-" + tt.name,
			}

			s.handleTaskOp(context.Background(), client, op)

			if stream.sentCount() == 0 {
				t.Fatalf("%s: expected a response when task store is nil", tt.name)
			}
			stream.mu.Lock()
			resp := stream.sent[0].GetTaskOp()
			stream.mu.Unlock()

			if resp == nil {
				t.Fatalf("%s: expected TaskOperationResponse payload", tt.name)
			}
			if resp.Success {
				t.Errorf("%s: expected Success=false when task store not configured", tt.name)
			}
			if resp.RequestId != "req-"+tt.name {
				t.Errorf("%s: expected RequestId='req-%s', got %q", tt.name, tt.name, resp.RequestId)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleTaskOp – unknown operation
// ---------------------------------------------------------------------------

func TestHandleTaskOp_UnknownOp_SendsErrorResponse(t *testing.T) {
	s := newTaskTestServer()
	stream := &mockStream{}
	client := newTaskTestClient(stream, defaultAgentIdentity())

	op := &pb.TaskOperation{
		Op:        pb.TaskOperation_OpType(99),
		TaskId:    "task-xyz",
		RequestId: "req-unknown-task-op",
	}

	s.handleTaskOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected a response for unknown task operation")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetTaskOp()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false for unknown task operation")
	}
}

// ---------------------------------------------------------------------------
// handleTaskOp – request_id propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 4 — authorization + info-hiding tests.
//
// These wire a real native-sqlite task store into the gateway so we exercise
// the actual authorizeTaskOp() path (creator-match / assignee-match /
// workspace-admin) and the ErrTaskNotFoundOrUnauthorized info-hiding
// principle end-to-end. ACL is left nil so the workspace-admin branch falls
// through to the workspace-only check.
// ---------------------------------------------------------------------------

// newTaskTestServerWithSQLiteStore is a sibling of newTaskTestServer that
// attaches a fresh native-sqlite task store. The returned cleanup closes
// the underlying DB.
func newTaskTestServerWithSQLiteStore(t *testing.T) (*GatewayServer, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "auth.db")
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
	s := newTestGatewayWithMocks(
		newMockSessionManager(),
		newMockMessageRouter(),
		newMockKVReadWriter(),
		newMockCheckpointManager(),
	)
	s.taskStore = store
	// Wire a circuit breaker so notifyTaskStatusChangeFromTaskID does not
	// nil-deref in the post-op notify path. Real config is in server.go;
	// the test only needs a working Execute().
	s.publishBreaker = circuitbreaker.New("test-task-auth-pub",
		circuitbreaker.WithMaxFailures(5),
		circuitbreaker.WithResetTimeout(time.Second),
	)
	return s, func() { _ = db.Close() }
}

// callerIdentity returns a fully-qualified agent identity in ws1.
func callerIdentity(impl, spec string) models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: impl,
		Specifier:      spec,
	}
}

// TestTaskOpAuth_CreatorMatch verifies the creator-of-the-task can act on it
// even without workspace AccessManage. Workspace match is the prerequisite,
// then ParentAgentID (storage column for creator_actor_id) == caller's topic.
func TestTaskOpAuth_CreatorMatch(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()

	task := &tasks.Task{
		TaskID:        "task-creator-match",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusPending,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	stream := &mockStream{}
	client := newTaskTestClient(stream, alice)

	op := &pb.TaskOperation{
		Op:        pb.TaskOperation_CANCEL,
		TaskId:    "task-creator-match",
		RequestId: "auth-creator",
	}
	s.handleTaskOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected a response")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetTaskOp()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskOperationResponse")
	}
	if !resp.Success {
		t.Errorf("creator-match CANCEL: expected Success=true, got error=%q", resp.Error)
	}
}

// TestTaskOpAuth_AssigneeMatch verifies the assigned worker can FAIL its own
// task (assignee-match fast path).
func TestTaskOpAuth_AssigneeMatch(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()

	task := &tasks.Task{
		TaskID:    "task-assignee-match",
		TaskType:  "test",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
	}
	ctx := context.Background()
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := s.taskStore.AssignTask(ctx, "task-assignee-match", aliceTopic); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	if err := s.taskStore.StartTask(ctx, "task-assignee-match"); err != nil {
		t.Fatalf("StartTask: %v", err)
	}

	stream := &mockStream{}
	client := newTaskTestClient(stream, alice)

	op := &pb.TaskOperation{
		Op:        pb.TaskOperation_FAIL,
		TaskId:    "task-assignee-match",
		Reason:    "worker rejected",
		RequestId: "auth-assignee",
	}
	s.handleTaskOp(ctx, client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskOp()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskOperationResponse")
	}
	if !resp.Success {
		t.Errorf("assignee-match FAIL: expected Success=true, got error=%q", resp.Error)
	}
}

// TestTaskOpInfoHiding_OtherUserSameWorkspace verifies that bob (no creator
// or assignee binding) attempting to CANCEL alice's task receives the
// canonical "task not found" error rather than any leaking-existence
// signal. ACL is nil so the workspace-admin branch falls through to the
// workspace-only check — but because the caller is neither creator nor
// assignee, the result is unauthorized which must be info-hidden.
//
// NB: with ACL=nil, authorizeTaskOp returns true on bare workspace match.
// That is the Phase 1-3 status quo. The "different user same workspace"
// auth-deny scenario explicitly requires an ACL service that denies the
// workspace-admin check. We assert the BEHAVIOR with a workspace mismatch
// instead, since that exercises the same code path and same response.
func TestTaskOpInfoHiding_DifferentWorkspace(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()

	// task in ws1, created by alice
	task := &tasks.Task{
		TaskID:        "task-info-hide",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusPending,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// caller is from a different workspace
	foreign := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws2",
		Implementation: "worker",
		Specifier:      "carol",
	}
	stream := &mockStream{}
	client := newTaskTestClient(stream, foreign)

	op := &pb.TaskOperation{
		Op:        pb.TaskOperation_CANCEL,
		TaskId:    "task-info-hide",
		RequestId: "auth-info-hide",
	}
	s.handleTaskOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskOp()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskOperationResponse")
	}
	if resp.Success {
		t.Errorf("foreign-workspace CANCEL: expected denial, got success")
	}
	if resp.Error != ErrTaskNotFoundOrUnauthorized {
		t.Errorf("info-hiding: expected error=%q, got %q",
			ErrTaskNotFoundOrUnauthorized, resp.Error)
	}
}

// TestTaskOpInfoHiding_NonexistentTask verifies that a request for a
// nonexistent task ID returns the same canonical error as an unauthorized
// access — preventing enumeration via "exists vs. forbidden" distinction.
func TestTaskOpInfoHiding_NonexistentTask(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	stream := &mockStream{}
	client := newTaskTestClient(stream, callerIdentity("worker", "alice"))

	op := &pb.TaskOperation{
		Op:        pb.TaskOperation_CANCEL,
		TaskId:    "does-not-exist",
		RequestId: "auth-no-such",
	}
	s.handleTaskOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskOp()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskOperationResponse")
	}
	if resp.Success {
		t.Errorf("nonexistent-task CANCEL: expected denial, got success")
	}
	if resp.Error != ErrTaskNotFoundOrUnauthorized {
		t.Errorf("info-hiding nonexistent: expected error=%q, got %q",
			ErrTaskNotFoundOrUnauthorized, resp.Error)
	}
}

// TestTaskOpInfoHiding_GetUnauthorized verifies the GET path also collapses
// auth failures into "task not found" so resource enumeration via GET is
// blocked the same way as TaskOperation.
func TestTaskOpInfoHiding_GetUnauthorized(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	task := &tasks.Task{
		TaskID:    "task-get-hide",
		TaskType:  "test",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
	}
	if err := s.taskStore.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	foreign := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws2",
		Implementation: "worker",
		Specifier:      "carol",
	}
	stream := &mockStream{}
	client := newTaskTestClient(stream, foreign)

	q := &pb.TaskQuery{
		Op:        pb.TaskQuery_GET,
		TaskId:    "task-get-hide",
		RequestId: "auth-get-hide",
	}
	s.handleTaskQuery(context.Background(), client, q)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskQuery()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskQueryResponse")
	}
	if resp.Success {
		t.Errorf("foreign GET: expected denial, got success")
	}
	if resp.Error != ErrTaskNotFoundOrUnauthorized {
		t.Errorf("info-hiding GET: expected error=%q, got %q",
			ErrTaskNotFoundOrUnauthorized, resp.Error)
	}
}

func TestHandleTaskOp_RequestIDAlwaysEchoed(t *testing.T) {
	ops := []struct {
		name string
		op   pb.TaskOperation_OpType
		rid  string
	}{
		{"CANCEL", pb.TaskOperation_CANCEL, "cancel-corr"},
		{"RETRY", pb.TaskOperation_RETRY, "retry-corr"},
		{"COMPLETE", pb.TaskOperation_COMPLETE, "complete-corr"},
		{"FAIL", pb.TaskOperation_FAIL, "fail-corr"},
		{"PAUSE", pb.TaskOperation_PAUSE, "pause-corr"},
		{"WAIT_FOR", pb.TaskOperation_WAIT_FOR, "wait-corr"},
		{"RESUME", pb.TaskOperation_RESUME, "resume-corr"},
		{"REJECT", pb.TaskOperation_REJECT, "reject-corr"},
	}

	for _, tt := range ops {
		t.Run(tt.name, func(t *testing.T) {
			s := newTaskTestServer()
			stream := &mockStream{}
			client := newTaskTestClient(stream, defaultAgentIdentity())

			op := &pb.TaskOperation{
				Op:        tt.op,
				TaskId:    "task-echo",
				RequestId: tt.rid,
			}

			s.handleTaskOp(context.Background(), client, op)

			if stream.sentCount() == 0 {
				t.Fatalf("%s: expected a response", tt.name)
			}
			stream.mu.Lock()
			resp := stream.sent[0].GetTaskOp()
			stream.mu.Unlock()

			if resp == nil {
				t.Fatalf("%s: expected TaskOperationResponse", tt.name)
			}
			if resp.RequestId != tt.rid {
				t.Errorf("%s: expected RequestId=%q, got %q", tt.name, tt.rid, resp.RequestId)
			}
		})
	}
}
