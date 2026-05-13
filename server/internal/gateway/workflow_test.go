package gateway

// Tests for workflow handler functions:
//   - handleWorkflowOp: no engine connected → error response
//   - handleWorkflowOp: engine connected → op forwarded, pending request stored
//   - handleWorkflowResponse: routes response back to original client
//   - handleWorkflowResponse: unknown request_id → no panic, warning only
//   - sweepTimedOutWorkflowRequests: old requests receive timeout error
//   - cleanupPendingWorkflowRequests: client disconnect cleans its requests
//   - findWorkflowEngineClient: returns nil when no engine, returns client when present

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newWorkflowTestServer() *GatewayServer {
	return &GatewayServer{
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
	}
}

func newWorkflowClient(stream *mockStream, principalType models.PrincipalType) *ClientSession {
	return &ClientSession{
		ID: "wf-client-" + string(principalType),
		Identity: models.Identity{
			Type:      principalType,
			Workspace: "ws1",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}
}

// ---------------------------------------------------------------------------
// findWorkflowEngineClient
// ---------------------------------------------------------------------------

func TestFindWorkflowEngineClient_NoClientsConnected_ReturnsNil(t *testing.T) {
	s := newWorkflowTestServer()

	result := s.findWorkflowEngineClient()
	if result != nil {
		t.Error("expected nil when no clients are connected")
	}
}

func TestFindWorkflowEngineClient_OnlyAgentConnected_ReturnsNil(t *testing.T) {
	s := newWorkflowTestServer()
	agentStream := &mockStream{}
	agentClient := newWorkflowClient(agentStream, models.PrincipalAgent)
	s.activeStreams.Store(agentClient.ID, agentClient)

	result := s.findWorkflowEngineClient()
	if result != nil {
		t.Error("expected nil when only an agent is connected")
	}
}

func TestFindWorkflowEngineClient_WorkflowEngineConnected_ReturnsIt(t *testing.T) {
	s := newWorkflowTestServer()
	wfStream := &mockStream{}
	wfClient := newWorkflowClient(wfStream, models.PrincipalWorkflowEngine)
	s.activeStreams.Store(wfClient.ID, wfClient)

	result := s.findWorkflowEngineClient()
	if result == nil {
		t.Fatal("expected workflow engine client, got nil")
	}
	if result.Identity.Type != models.PrincipalWorkflowEngine {
		t.Errorf("expected PrincipalWorkflowEngine, got %s", result.Identity.Type)
	}
}

func TestFindWorkflowEngineClient_MixedClients_ReturnsWorkflowEngine(t *testing.T) {
	s := newWorkflowTestServer()

	agentStream := &mockStream{}
	agentClient := newWorkflowClient(agentStream, models.PrincipalAgent)
	s.activeStreams.Store(agentClient.ID, agentClient)

	wfStream := &mockStream{}
	wfClient := newWorkflowClient(wfStream, models.PrincipalWorkflowEngine)
	s.activeStreams.Store(wfClient.ID, wfClient)

	result := s.findWorkflowEngineClient()
	if result == nil {
		t.Fatal("expected workflow engine client from mixed pool, got nil")
	}
	if result.Identity.Type != models.PrincipalWorkflowEngine {
		t.Errorf("expected PrincipalWorkflowEngine, got %s", result.Identity.Type)
	}
}

// ---------------------------------------------------------------------------
// handleWorkflowOp – no workflow engine connected
// ---------------------------------------------------------------------------

func TestHandleWorkflowOp_NoEngineConnected_SendsErrorResponse(t *testing.T) {
	s := newWorkflowTestServer()
	stream := &mockStream{}
	client := newWorkflowClient(stream, models.PrincipalAgent)

	op := &pb.WorkflowOperation{
		RequestId: "req-no-engine",
	}

	s.handleWorkflowOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response when no workflow engine is connected")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetWorkflowResponse()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected WorkflowResponse payload")
	}
	if resp.Success {
		t.Error("expected Success=false when no workflow engine is connected")
	}
	if resp.RequestId != "req-no-engine" {
		t.Errorf("expected RequestId='req-no-engine', got %q", resp.RequestId)
	}
}

func TestHandleWorkflowOp_NoEngineConnected_NoPendingRequestStored(t *testing.T) {
	s := newWorkflowTestServer()
	stream := &mockStream{}
	client := newWorkflowClient(stream, models.PrincipalAgent)

	op := &pb.WorkflowOperation{
		RequestId: "req-not-stored",
	}

	s.handleWorkflowOp(context.Background(), client, op)

	// No pending request should have been stored since there was no engine to forward to.
	var count int
	s.pendingWorkflowRequests.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 pending workflow requests, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// handleWorkflowOp – workflow engine connected
// ---------------------------------------------------------------------------

func TestHandleWorkflowOp_EngineConnected_ForwardsOpToEngine(t *testing.T) {
	s := newWorkflowTestServer()

	// Register a workflow engine.
	wfStream := &mockStream{}
	wfClient := newWorkflowClient(wfStream, models.PrincipalWorkflowEngine)
	s.activeStreams.Store(wfClient.ID, wfClient)

	// Requesting client.
	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)

	op := &pb.WorkflowOperation{
		RequestId: "req-forward",
	}

	s.handleWorkflowOp(context.Background(), callerClient, op)

	// Workflow engine should have received exactly one message.
	if wfStream.sentCount() == 0 {
		t.Fatal("expected workflow op to be forwarded to workflow engine stream")
	}
	wfStream.mu.Lock()
	msg := wfStream.sent[0]
	wfStream.mu.Unlock()

	if msg.GetWorkflowOp() == nil {
		t.Error("expected WorkflowOp payload forwarded to workflow engine")
	}
}

func TestHandleWorkflowOp_EngineConnected_StoredInPendingRequests(t *testing.T) {
	s := newWorkflowTestServer()

	wfStream := &mockStream{}
	wfClient := newWorkflowClient(wfStream, models.PrincipalWorkflowEngine)
	s.activeStreams.Store(wfClient.ID, wfClient)

	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)

	op := &pb.WorkflowOperation{
		RequestId: "req-pending",
	}

	s.handleWorkflowOp(context.Background(), callerClient, op)

	val, ok := s.pendingWorkflowRequests.Load("req-pending")
	if !ok {
		t.Fatal("expected pending workflow request to be stored under request_id")
	}
	pending, ok := val.(*pendingWorkflowRequest)
	if !ok {
		t.Fatal("expected *pendingWorkflowRequest value")
	}
	if pending.client != callerClient {
		t.Error("expected pending request to reference the original caller client")
	}
}

func TestHandleWorkflowOp_EmptyRequestID_GeneratesOneAutomatically(t *testing.T) {
	s := newWorkflowTestServer()

	wfStream := &mockStream{}
	wfClient := newWorkflowClient(wfStream, models.PrincipalWorkflowEngine)
	s.activeStreams.Store(wfClient.ID, wfClient)

	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)

	op := &pb.WorkflowOperation{
		RequestId: "", // empty – server should generate one
	}

	s.handleWorkflowOp(context.Background(), callerClient, op)

	// A pending request must exist under a non-empty auto-generated id.
	var count int
	s.pendingWorkflowRequests.Range(func(k, _ interface{}) bool {
		id, _ := k.(string)
		if id != "" {
			count++
		}
		return true
	})
	if count == 0 {
		t.Error("expected a pending request stored with an auto-generated request_id")
	}
}

// ---------------------------------------------------------------------------
// handleWorkflowResponse
// ---------------------------------------------------------------------------

func TestHandleWorkflowResponse_KnownRequestID_RoutesResponseToOriginalClient(t *testing.T) {
	s := newWorkflowTestServer()

	// Set up a pending request.
	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)
	s.pendingWorkflowRequests.Store("resp-req-id", &pendingWorkflowRequest{
		client:    callerClient,
		createdAt: time.Now(),
	})

	wfEngineStream := &mockStream{}
	wfEngineClient := newWorkflowClient(wfEngineStream, models.PrincipalWorkflowEngine)

	resp := &pb.WorkflowResponse{
		RequestId: "resp-req-id",
		Success:   true,
	}

	s.handleWorkflowResponse(context.Background(), wfEngineClient, resp)

	// Original caller must have received the response.
	if callerStream.sentCount() == 0 {
		t.Fatal("expected workflow response to be relayed to original caller")
	}
	callerStream.mu.Lock()
	msg := callerStream.sent[0]
	callerStream.mu.Unlock()

	wfResp := msg.GetWorkflowResponse()
	if wfResp == nil {
		t.Fatal("expected WorkflowResponse payload in relayed message")
	}
	if !wfResp.Success {
		t.Error("expected Success=true in relayed response")
	}
	if wfResp.RequestId != "resp-req-id" {
		t.Errorf("expected RequestId='resp-req-id', got %q", wfResp.RequestId)
	}
}

func TestHandleWorkflowResponse_KnownRequestID_RemovedFromPendingMap(t *testing.T) {
	s := newWorkflowTestServer()

	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)
	s.pendingWorkflowRequests.Store("rm-req-id", &pendingWorkflowRequest{
		client:    callerClient,
		createdAt: time.Now(),
	})

	wfClient := newWorkflowClient(&mockStream{}, models.PrincipalWorkflowEngine)
	resp := &pb.WorkflowResponse{RequestId: "rm-req-id", Success: true}

	s.handleWorkflowResponse(context.Background(), wfClient, resp)

	if _, ok := s.pendingWorkflowRequests.Load("rm-req-id"); ok {
		t.Error("expected pending request to be removed after response received")
	}
}

func TestHandleWorkflowResponse_UnknownRequestID_NoMessageSentNoPanic(t *testing.T) {
	s := newWorkflowTestServer()
	wfClient := newWorkflowClient(&mockStream{}, models.PrincipalWorkflowEngine)

	// Should not panic even with an unknown request_id.
	s.handleWorkflowResponse(context.Background(), wfClient, &pb.WorkflowResponse{
		RequestId: "totally-unknown",
		Success:   true,
	})
}

// ---------------------------------------------------------------------------
// sweepTimedOutWorkflowRequests
// ---------------------------------------------------------------------------

func TestSweepTimedOutWorkflowRequests_OldRequest_SendsTimeoutErrorAndRemoves(t *testing.T) {
	s := newWorkflowTestServer()

	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)

	// Store a request that is 60 seconds old (well past the 30-second cutoff).
	s.pendingWorkflowRequests.Store("old-req", &pendingWorkflowRequest{
		client:    callerClient,
		createdAt: time.Now().Add(-60 * time.Second),
	})

	s.sweepTimedOutWorkflowRequests()

	// Caller should have received a timeout error.
	if callerStream.sentCount() == 0 {
		t.Fatal("expected timeout error response sent to caller for old pending request")
	}
	callerStream.mu.Lock()
	msg := callerStream.sent[0]
	callerStream.mu.Unlock()

	wfResp := msg.GetWorkflowResponse()
	if wfResp == nil {
		t.Fatal("expected WorkflowResponse payload in timeout error")
	}
	if wfResp.Success {
		t.Error("expected Success=false in timeout error response")
	}
	if wfResp.RequestId != "old-req" {
		t.Errorf("expected RequestId='old-req', got %q", wfResp.RequestId)
	}

	// Request must be removed.
	if _, ok := s.pendingWorkflowRequests.Load("old-req"); ok {
		t.Error("expected timed-out request to be removed from pending map")
	}
}

func TestSweepTimedOutWorkflowRequests_RecentRequest_NotSwept(t *testing.T) {
	s := newWorkflowTestServer()

	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)

	// Store a request created just 5 seconds ago (within the 30s cutoff).
	s.pendingWorkflowRequests.Store("fresh-req", &pendingWorkflowRequest{
		client:    callerClient,
		createdAt: time.Now().Add(-5 * time.Second),
	})

	s.sweepTimedOutWorkflowRequests()

	// Caller should NOT have received any timeout error.
	if callerStream.sentCount() != 0 {
		t.Errorf("expected no messages for a fresh pending request, got %d", callerStream.sentCount())
	}

	// Request must still be present.
	if _, ok := s.pendingWorkflowRequests.Load("fresh-req"); !ok {
		t.Error("expected fresh request to remain in pending map after sweep")
	}
}

// ---------------------------------------------------------------------------
// cleanupPendingWorkflowRequests
// ---------------------------------------------------------------------------

func TestCleanupPendingWorkflowRequests_RemovesRequestsForClient(t *testing.T) {
	s := newWorkflowTestServer()

	callerStream := &mockStream{}
	callerClient := newWorkflowClient(callerStream, models.PrincipalAgent)

	otherStream := &mockStream{}
	otherClient := newWorkflowClient(otherStream, models.PrincipalUser)

	s.pendingWorkflowRequests.Store("caller-req-1", &pendingWorkflowRequest{client: callerClient, createdAt: time.Now()})
	s.pendingWorkflowRequests.Store("caller-req-2", &pendingWorkflowRequest{client: callerClient, createdAt: time.Now()})
	s.pendingWorkflowRequests.Store("other-req", &pendingWorkflowRequest{client: otherClient, createdAt: time.Now()})

	s.cleanupPendingWorkflowRequests(callerClient)

	if _, ok := s.pendingWorkflowRequests.Load("caller-req-1"); ok {
		t.Error("expected caller-req-1 to be removed")
	}
	if _, ok := s.pendingWorkflowRequests.Load("caller-req-2"); ok {
		t.Error("expected caller-req-2 to be removed")
	}
	// Other client's request must remain.
	if _, ok := s.pendingWorkflowRequests.Load("other-req"); !ok {
		t.Error("expected other-req to remain after cleanup of different client")
	}
}

func TestCleanupPendingWorkflowRequests_EmptyMap_NoPanic(t *testing.T) {
	s := newWorkflowTestServer()
	client := newWorkflowClient(&mockStream{}, models.PrincipalAgent)
	// Should not panic on an empty pending map.
	s.cleanupPendingWorkflowRequests(client)
}
