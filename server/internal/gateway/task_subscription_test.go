// Phase 4 Stage B: gateway-side tests for the task subscription primitive.
//
// These exercise handleTaskSubscriptionOp end-to-end with a sqlite task store
// and an in-process router that fans Publish bytes back to registered
// subscription handlers. The router substitute is local to this file so we
// don't disturb the broader connect_test mocks.

package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	taskssqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"

	_ "modernc.org/sqlite"
)

// inProcessRouter is a MessageRouter that fans bytes published on a topic to
// every handler subscribed against the same topic. Mirrors the gateway's
// real-world delivery semantics closely enough for end-to-end subscription
// testing.
type inProcessRouter struct {
	mu       sync.Mutex
	handlers map[string][]func([]byte) // topic -> handlers
}

func newInProcessRouter() *inProcessRouter {
	return &inProcessRouter{handlers: make(map[string][]func([]byte))}
}

func (r *inProcessRouter) Publish(_ context.Context, topic string, payload []byte) error {
	r.mu.Lock()
	hs := append([]func([]byte){}, r.handlers[topic]...)
	r.mu.Unlock()
	for _, h := range hs {
		// Copy payload so handlers can't accidentally share mutable state.
		buf := make([]byte, len(payload))
		copy(buf, payload)
		h(buf)
	}
	return nil
}

func (r *inProcessRouter) subscribeInternal(topic string, handler func([]byte)) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[topic] = append(r.handlers[topic], handler)
	idx := len(r.handlers[topic]) - 1
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		hs := r.handlers[topic]
		if idx >= len(hs) {
			return
		}
		r.handlers[topic] = append(hs[:idx], hs[idx+1:]...)
	}, nil
}

func (r *inProcessRouter) Subscribe(topic string, handler func([]byte)) (func(), error) {
	return r.subscribeInternal(topic, handler)
}

func (r *inProcessRouter) SubscribeExclusive(topic string, _ string, handler func([]byte)) (func(), error) {
	return r.subscribeInternal(topic, handler)
}

func (r *inProcessRouter) SubscribeExclusiveFromNow(topic string, _ string, handler func([]byte)) (func(), error) {
	return r.subscribeInternal(topic, handler)
}

func (r *inProcessRouter) SubscribeExclusiveFromTimestamp(topic string, _ string, _ int64, handler func([]byte)) (func(), error) {
	return r.subscribeInternal(topic, handler)
}

// newSubscriptionTestServer builds a gateway with the in-process router and
// a sqlite task store. Returns a cleanup closer.
func newSubscriptionTestServer(t *testing.T) (*GatewayServer, *inProcessRouter, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "sub.db")
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
	router := newInProcessRouter()
	s := &GatewayServer{
		sessions:      newMockSessionManager(),
		router:        router,
		kv:            newMockKVReadWriter(),
		checkpoints:   newMockCheckpointManager(),
		gatewayID:     "test-sub-gw",
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
	}
	s.taskStore = store
	s.publishBreaker = circuitbreaker.New("test-task-sub-pub",
		circuitbreaker.WithMaxFailures(5),
		circuitbreaker.WithResetTimeout(time.Second),
	)
	return s, router, func() { _ = db.Close() }
}

// newSubscriptionTestClient builds a ClientSession backed by a deliveryCh.
// We start the delivery loop with a context that the test cancels in cleanup.
func newSubscriptionTestClient(stream *mockStream, identity models.Identity) (*ClientSession, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &ClientSession{
		ID:            "test-sub-session",
		Identity:      identity,
		Stream:        stream,
		subscriptions: make(map[string]func()),
		deliveryCh:    make(chan *pb.DownstreamMessage, 32),
	}
	client.startDeliveryLoop(ctx)
	return client, cancel
}

// waitForTaskEvent polls mockStream for a TaskEvent with task_id matching
// `taskID` and the given variant predicate. Returns the event or nil on
// timeout.
func waitForTaskEvent(stream *mockStream, taskID string, want func(*pb.TaskEvent) bool, timeout time.Duration) *pb.TaskEvent {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stream.mu.Lock()
		for _, msg := range stream.sent {
			evt := msg.GetTaskEvent()
			if evt == nil {
				continue
			}
			if evt.GetTaskId() != taskID {
				continue
			}
			if want == nil || want(evt) {
				stream.mu.Unlock()
				return evt
			}
		}
		stream.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// findSubscriptionResponse pulls the first TaskSubscriptionOperationResponse
// from a mockStream's sent buffer.
func findSubscriptionResponse(stream *mockStream) *pb.TaskSubscriptionOperationResponse {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for _, msg := range stream.sent {
		if resp := msg.GetTaskSubscriptionResponse(); resp != nil {
			return resp
		}
	}
	return nil
}

// TestTaskSubscription_SubscribeAndReceiveStatusChange verifies the happy
// path: subscribe to a task, drive a status transition via the task store,
// publish a TaskEvent on the topic, and observe it downstream.
func TestTaskSubscription_SubscribeAndReceiveStatusChange(t *testing.T) {
	s, _, cleanup := newSubscriptionTestServer(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()

	ctx := context.Background()
	task := &tasks.Task{
		TaskID:        "task-sub-status",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusRunning,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	stream := &mockStream{}
	client, cancel := newSubscriptionTestClient(stream, alice)
	defer cancel()

	subOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_SUBSCRIBE,
		TaskId:          "task-sub-status",
		ClientRequestId: "sub-1",
	}
	s.handleTaskSubscriptionOp(ctx, client, subOp)

	resp := findSubscriptionResponse(stream)
	if resp == nil {
		t.Fatal("expected subscription response")
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %q", resp.Error)
	}
	if resp.SubscriptionId == "" {
		t.Error("expected non-empty subscription_id")
	}

	// Publish a TaskEvent directly to the topic (simulating a transition).
	evt := &pb.TaskEvent{
		TaskId:          "task-sub-status",
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       "ws1",
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_RUNNING,
				ToStatus:   pb.TaskStatus_TASK_STATUS_COMPLETED,
			},
		},
	}
	if err := s.publishTaskEventBytes(ctx, "ws1", "task-sub-status", evt); err != nil {
		t.Fatalf("publishTaskEventBytes: %v", err)
	}

	observed := waitForTaskEvent(stream, "task-sub-status", func(e *pb.TaskEvent) bool {
		return e.GetStatusChanged() != nil
	}, 2*time.Second)
	if observed == nil {
		t.Fatal("did not observe TaskEvent.status_changed downstream within timeout")
	}
	sc := observed.GetStatusChanged()
	if sc.GetToStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Errorf("to_status: got %v, want COMPLETED", sc.GetToStatus())
	}
	if observed.GetSubscriptionId() != resp.SubscriptionId {
		t.Errorf("subscription_id stamping: got %q, want %q", observed.GetSubscriptionId(), resp.SubscriptionId)
	}
}

// TestTaskSubscription_RecursiveDeliversChildEvents verifies recursive=true
// subscribes to descendants discovered at SUBSCRIBE time, so publishes on a
// known child topic flow to the parent's subscriber.
func TestTaskSubscription_RecursiveDeliversChildEvents(t *testing.T) {
	s, _, cleanup := newSubscriptionTestServer(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()

	ctx := context.Background()
	parent := &tasks.Task{
		TaskID:        "parent-rec",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusRunning,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(ctx, parent); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	child := &tasks.Task{
		TaskID:        "child-rec",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusPending,
		ParentAgentID: aliceTopic,
		ParentTaskID:  "parent-rec",
	}
	if err := s.taskStore.CreateTask(ctx, child); err != nil {
		t.Fatalf("create child: %v", err)
	}

	stream := &mockStream{}
	client, cancel := newSubscriptionTestClient(stream, alice)
	defer cancel()

	subOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_SUBSCRIBE,
		TaskId:          "parent-rec",
		Recursive:       true,
		ClientRequestId: "sub-rec",
	}
	s.handleTaskSubscriptionOp(ctx, client, subOp)
	resp := findSubscriptionResponse(stream)
	if resp == nil || !resp.Success {
		t.Fatalf("expected subscription success, got %+v", resp)
	}

	// Publish a TaskEvent on the CHILD's topic. Recursive subscribe should
	// pick it up because we walked descendants at SUBSCRIBE time.
	evt := &pb.TaskEvent{
		TaskId:          "child-rec",
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       "ws1",
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_RUNNING,
				ToStatus:   pb.TaskStatus_TASK_STATUS_COMPLETED,
			},
		},
	}
	if err := s.publishTaskEventBytes(ctx, "ws1", "child-rec", evt); err != nil {
		t.Fatalf("publishTaskEventBytes: %v", err)
	}

	observed := waitForTaskEvent(stream, "child-rec", nil, 2*time.Second)
	if observed == nil {
		t.Fatal("recursive subscribe did not receive child task event")
	}
}

// TestTaskSubscription_SubscribeNonexistentTask returns the canonical "task
// not found" error.
func TestTaskSubscription_SubscribeNonexistentTask(t *testing.T) {
	s, _, cleanup := newSubscriptionTestServer(t)
	defer cleanup()

	stream := &mockStream{}
	client, cancel := newSubscriptionTestClient(stream, callerIdentity("worker", "alice"))
	defer cancel()

	subOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_SUBSCRIBE,
		TaskId:          "does-not-exist",
		ClientRequestId: "sub-missing",
	}
	s.handleTaskSubscriptionOp(context.Background(), client, subOp)

	resp := findSubscriptionResponse(stream)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Success {
		t.Error("expected Success=false")
	}
	if resp.Error != ErrTaskNotFoundOrUnauthorized {
		t.Errorf("expected error=%q, got %q", ErrTaskNotFoundOrUnauthorized, resp.Error)
	}
}

// TestTaskSubscription_SubscribeUnauthorized info-hides the unauthorized case
// behind the same "task not found" string.
func TestTaskSubscription_SubscribeUnauthorized(t *testing.T) {
	s, _, cleanup := newSubscriptionTestServer(t)
	defer cleanup()

	task := &tasks.Task{
		TaskID:    "task-foreign",
		TaskType:  "test",
		Workspace: "ws1",
		Status:    tasks.TaskStatusRunning,
		// no ParentAgentID / AssignedTo, foreign caller -> unauthorized
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
	client, cancel := newSubscriptionTestClient(stream, foreign)
	defer cancel()

	subOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_SUBSCRIBE,
		TaskId:          "task-foreign",
		ClientRequestId: "sub-foreign",
	}
	s.handleTaskSubscriptionOp(context.Background(), client, subOp)

	resp := findSubscriptionResponse(stream)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Success {
		t.Error("expected Success=false for unauthorized subscribe")
	}
	if resp.Error != ErrTaskNotFoundOrUnauthorized {
		t.Errorf("expected error=%q, got %q", ErrTaskNotFoundOrUnauthorized, resp.Error)
	}
}

// TestTaskSubscription_UnsubscribeStopsFlow verifies that UNSUBSCRIBE cancels
// router subscriptions: a publish after UNSUBSCRIBE does NOT reach the client.
func TestTaskSubscription_UnsubscribeStopsFlow(t *testing.T) {
	s, _, cleanup := newSubscriptionTestServer(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()

	task := &tasks.Task{
		TaskID:        "task-unsub",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusRunning,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	stream := &mockStream{}
	client, cancel := newSubscriptionTestClient(stream, alice)
	defer cancel()

	subOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_SUBSCRIBE,
		TaskId:          "task-unsub",
		ClientRequestId: "sub-cycle",
	}
	s.handleTaskSubscriptionOp(context.Background(), client, subOp)
	resp := findSubscriptionResponse(stream)
	if resp == nil || !resp.Success {
		t.Fatalf("subscribe failed: %+v", resp)
	}
	subscriptionID := resp.SubscriptionId

	// Unsubscribe, then publish: no TaskEvent should arrive.
	unsubOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_UNSUBSCRIBE,
		TaskId:          "task-unsub",
		SubscriptionId:  subscriptionID,
		ClientRequestId: "unsub-1",
	}
	s.handleTaskSubscriptionOp(context.Background(), client, unsubOp)

	// Note the pre-unsub event count so we can detect any further events.
	preCount := stream.sentCount()

	evt := &pb.TaskEvent{
		TaskId:    "task-unsub",
		Workspace: "ws1",
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				ToStatus: pb.TaskStatus_TASK_STATUS_COMPLETED,
			},
		},
	}
	if err := s.publishTaskEventBytes(context.Background(), "ws1", "task-unsub", evt); err != nil {
		t.Fatalf("publishTaskEventBytes: %v", err)
	}
	// Wait a beat to let any erroneous delivery happen.
	time.Sleep(200 * time.Millisecond)

	stream.mu.Lock()
	for i := preCount; i < len(stream.sent); i++ {
		if stream.sent[i].GetTaskEvent() != nil {
			stream.mu.Unlock()
			t.Fatalf("expected no TaskEvent after UNSUBSCRIBE; saw one at index %d", i)
		}
	}
	stream.mu.Unlock()
}

// TestTaskSubscription_SessionCleanupCancelsSubscription verifies that
// UnsubscribeAll on session close cancels the router subscription, so the
// session does not leak handlers after disconnect.
func TestTaskSubscription_SessionCleanupCancelsSubscription(t *testing.T) {
	s, router, cleanup := newSubscriptionTestServer(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()
	task := &tasks.Task{
		TaskID:        "task-cleanup",
		TaskType:      "test",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusRunning,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	stream := &mockStream{}
	client, cancel := newSubscriptionTestClient(stream, alice)
	defer cancel()

	subOp := &pb.TaskSubscriptionOperation{
		Op:              pb.TaskSubscriptionOperation_SUBSCRIBE,
		TaskId:          "task-cleanup",
		ClientRequestId: "sub-cleanup",
	}
	s.handleTaskSubscriptionOp(context.Background(), client, subOp)

	topic := models.MustTaskEventsTopic("ws1", "task-cleanup")
	router.mu.Lock()
	pre := len(router.handlers[topic])
	router.mu.Unlock()
	if pre == 0 {
		t.Fatal("expected at least one router handler after SUBSCRIBE")
	}

	// Trigger the session-close cleanup path (mirrors connect.go's
	// defer cleanupSession path: UnsubscribeAll is the relevant step).
	client.UnsubscribeAll()

	router.mu.Lock()
	post := len(router.handlers[topic])
	router.mu.Unlock()
	if post != 0 {
		t.Errorf("expected 0 router handlers after cleanup, got %d", post)
	}
}
