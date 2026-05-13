package gateway

// Tests for handleCheckpointOp() covering all four operations (SAVE, LOAD,
// DELETE, LIST), the nil-checkpoints path, TTL semantics, and the unknown-op
// fallback. No external services are required - uses the mockCheckpointManager
// and mockStream already defined in connect_test.go.

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

// newCheckpointTestServer returns a GatewayServer with a mock checkpoint store.
func newCheckpointTestServer(cp *mockCheckpointManager) *GatewayServer {
	return newTestGatewayWithMocks(
		newMockSessionManager(),
		newMockMessageRouter(),
		newMockKVReadWriter(),
		cp,
	)
}

// newCheckpointTestClient returns a ClientSession wired to a mockStream.
func newCheckpointTestClient(stream *mockStream) *ClientSession {
	return &ClientSession{
		ID:            "cp-test-session",
		Identity:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}
}

// ---------------------------------------------------------------------------
// nil checkpoint store
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_NilCheckpoints_SendsNotConfiguredError(t *testing.T) {
	s := newCheckpointTestServer(nil)
	s.checkpoints = nil // ensure nil
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_SAVE,
		Key:       "state",
		Data:      []byte("data"),
		RequestId: "req-nil",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected a response when checkpoint store is nil")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected CheckpointResponse, got nil")
	}
	if resp.Success {
		t.Error("expected Success=false when checkpoint store not configured")
	}
	if resp.RequestId != "req-nil" {
		t.Errorf("expected RequestId='req-nil', got %q", resp.RequestId)
	}
}

// ---------------------------------------------------------------------------
// SAVE operation
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_Save_SuccessSendsResponseWithSavedAt(t *testing.T) {
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_SAVE,
		Key:       "model-state",
		Data:      []byte("binary-state-data"),
		RequestId: "req-save-1",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected response for SAVE operation")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected CheckpointResponse payload")
	}
	if !resp.Success {
		t.Errorf("expected Success=true for SAVE, got error: %s", resp.Error)
	}
	if resp.SavedAt == 0 {
		t.Error("expected non-zero SavedAt timestamp for SAVE response")
	}
	if resp.RequestId != "req-save-1" {
		t.Errorf("expected RequestId='req-save-1', got %q", resp.RequestId)
	}
}

func TestHandleCheckpointOp_Save_DataStoredInMock(t *testing.T) {
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:   pb.CheckpointOperation_SAVE,
		Key:  "my-key",
		Data: []byte("my-value"),
	}

	s.handleCheckpointOp(context.Background(), client, op)

	cp.mu.Lock()
	data, ok := cp.savedData["my-key"]
	cp.mu.Unlock()

	if !ok {
		t.Fatal("expected data to be stored in checkpoint manager")
	}
	if string(data) != "my-value" {
		t.Errorf("expected stored data 'my-value', got %q", data)
	}
}

func TestHandleCheckpointOp_Save_Error_SendsFailureResponse(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.saveErr = context.DeadlineExceeded
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_SAVE,
		Key:       "fail-key",
		Data:      []byte("data"),
		RequestId: "req-save-err",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected response when SAVE fails")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false when SAVE fails")
	}
}

func TestHandleCheckpointOp_Save_NegativeTTL_UsesServerDefault(t *testing.T) {
	// TTL=-1 means "use server default". We verify no panic and success response.
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	s.checkpointDefaultTTL = 10 * time.Minute
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:   pb.CheckpointOperation_SAVE,
		Key:  "default-ttl-key",
		Data: []byte("data"),
		Ttl:  -1,
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected response for SAVE with TTL=-1")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()
	if resp == nil || !resp.Success {
		t.Error("expected Success=true for SAVE with server default TTL")
	}
}

func TestHandleCheckpointOp_Save_ZeroTTL_MeansNoExpiration(t *testing.T) {
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:   pb.CheckpointOperation_SAVE,
		Key:  "no-expiry-key",
		Data: []byte("data"),
		Ttl:  0,
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()
	if resp == nil || !resp.Success {
		t.Error("expected Success=true for SAVE with TTL=0 (no expiration)")
	}
}

func TestHandleCheckpointOp_Save_PositiveTTL_UsedDirectly(t *testing.T) {
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:   pb.CheckpointOperation_SAVE,
		Key:  "ttl-key",
		Data: []byte("data"),
		Ttl:  300, // 5 minutes
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()
	if resp == nil || !resp.Success {
		t.Error("expected Success=true for SAVE with explicit TTL")
	}
}

// ---------------------------------------------------------------------------
// LOAD operation
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_Load_ExistingKey_SendsDataAndSavedAt(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.savedData["existing-key"] = []byte("saved-state")
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_LOAD,
		Key:       "existing-key",
		RequestId: "req-load-1",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected response for LOAD operation")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || !resp.Success {
		t.Fatalf("expected Success=true for LOAD of existing key, resp=%v", resp)
	}
	if string(resp.Data) != "saved-state" {
		t.Errorf("expected Data='saved-state', got %q", resp.Data)
	}
	if resp.RequestId != "req-load-1" {
		t.Errorf("expected RequestId='req-load-1', got %q", resp.RequestId)
	}
}

func TestHandleCheckpointOp_Load_MissingKey_SendsSuccessWithNilData(t *testing.T) {
	cp := newMockCheckpointManager()
	// savedData is empty - key does not exist
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:  pb.CheckpointOperation_LOAD,
		Key: "missing-key",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || !resp.Success {
		t.Error("expected Success=true (not found is not an error) for missing key")
	}
	if len(resp.Data) != 0 {
		t.Errorf("expected nil Data for missing key, got %v", resp.Data)
	}
}

func TestHandleCheckpointOp_Load_Error_SendsFailureResponse(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.loadErr = context.DeadlineExceeded
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:  pb.CheckpointOperation_LOAD,
		Key: "any-key",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false when LOAD fails")
	}
}

// ---------------------------------------------------------------------------
// DELETE operation
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_Delete_ExistingKey_SendsSuccessResponse(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.savedData["del-key"] = []byte("data-to-delete")
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_DELETE,
		Key:       "del-key",
		RequestId: "req-del-1",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || !resp.Success {
		t.Errorf("expected Success=true for DELETE, got %v", resp)
	}
	if resp.RequestId != "req-del-1" {
		t.Errorf("expected RequestId='req-del-1', got %q", resp.RequestId)
	}

	// Verify data was actually removed.
	cp.mu.Lock()
	_, stillExists := cp.savedData["del-key"]
	cp.mu.Unlock()
	if stillExists {
		t.Error("expected key to be removed from checkpoint store after DELETE")
	}
}

func TestHandleCheckpointOp_Delete_Error_SendsFailureResponse(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.deleteErr = context.DeadlineExceeded
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:  pb.CheckpointOperation_DELETE,
		Key: "any-key",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false when DELETE fails")
	}
}

// ---------------------------------------------------------------------------
// LIST operation
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_List_ReturnsAllKeys(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.savedData["k1"] = []byte("v1")
	cp.savedData["k2"] = []byte("v2")
	cp.savedData["k3"] = []byte("v3")
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_LIST,
		RequestId: "req-list-1",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected response for LIST operation")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || !resp.Success {
		t.Fatalf("expected Success=true for LIST, got %v", resp)
	}
	if len(resp.Keys) != 3 {
		t.Errorf("expected 3 keys from LIST, got %d: %v", len(resp.Keys), resp.Keys)
	}
	if resp.RequestId != "req-list-1" {
		t.Errorf("expected RequestId='req-list-1', got %q", resp.RequestId)
	}
}

func TestHandleCheckpointOp_List_EmptyStore_ReturnsEmptyKeys(t *testing.T) {
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op: pb.CheckpointOperation_LIST,
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || !resp.Success {
		t.Error("expected Success=true for LIST on empty store")
	}
	if len(resp.Keys) != 0 {
		t.Errorf("expected 0 keys for empty store, got %d", len(resp.Keys))
	}
}

func TestHandleCheckpointOp_List_Error_SendsFailureResponse(t *testing.T) {
	cp := newMockCheckpointManager()
	cp.listErr = context.DeadlineExceeded
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op: pb.CheckpointOperation_LIST,
	}

	s.handleCheckpointOp(context.Background(), client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false when LIST fails")
	}
}

// ---------------------------------------------------------------------------
// Unknown operation
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_UnknownOp_SendsErrorResponse(t *testing.T) {
	cp := newMockCheckpointManager()
	s := newCheckpointTestServer(cp)
	stream := &mockStream{}
	client := newCheckpointTestClient(stream)

	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_OpType(99),
		RequestId: "req-unknown",
	}

	s.handleCheckpointOp(context.Background(), client, op)

	if stream.sentCount() == 0 {
		t.Fatal("expected response for unknown checkpoint operation")
	}
	stream.mu.Lock()
	resp := stream.sent[0].GetCheckpoint()
	stream.mu.Unlock()

	if resp == nil || resp.Success {
		t.Error("expected Success=false for unknown checkpoint operation")
	}
	if resp.RequestId != "req-unknown" {
		t.Errorf("expected RequestId='req-unknown', got %q", resp.RequestId)
	}
}

// ---------------------------------------------------------------------------
// Request ID correlation
// ---------------------------------------------------------------------------

func TestHandleCheckpointOp_ResponseAlwaysEchoesRequestID(t *testing.T) {
	tests := []struct {
		name      string
		op        pb.CheckpointOperation_OpType
		requestID string
	}{
		{"SAVE", pb.CheckpointOperation_SAVE, "save-corr-id"},
		{"LOAD", pb.CheckpointOperation_LOAD, "load-corr-id"},
		{"DELETE", pb.CheckpointOperation_DELETE, "del-corr-id"},
		{"LIST", pb.CheckpointOperation_LIST, "list-corr-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := newMockCheckpointManager()
			s := newCheckpointTestServer(cp)
			stream := &mockStream{}
			client := newCheckpointTestClient(stream)

			op := &pb.CheckpointOperation{
				Op:        tt.op,
				Key:       "test-key",
				Data:      []byte("data"),
				RequestId: tt.requestID,
			}

			s.handleCheckpointOp(context.Background(), client, op)

			if stream.sentCount() == 0 {
				t.Fatalf("%s: expected a response message", tt.name)
			}
			stream.mu.Lock()
			resp := stream.sent[0].GetCheckpoint()
			stream.mu.Unlock()

			if resp == nil {
				t.Fatalf("%s: expected CheckpointResponse payload", tt.name)
			}
			if resp.RequestId != tt.requestID {
				t.Errorf("%s: expected RequestId=%q, got %q", tt.name, tt.requestID, resp.RequestId)
			}
		})
	}
}
