package gateway

import (
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
)

// lastKV returns the KVResponse from the most recent captured downstream msg.
func lastKV(t *testing.T, msgs []*pb.DownstreamMessage) *pb.KVResponse {
	t.Helper()
	if len(msgs) == 0 {
		t.Fatal("no downstream message captured")
	}
	kvr := msgs[len(msgs)-1].GetKv()
	if kvr == nil {
		t.Fatalf("last message is not a KVResponse: %T", msgs[len(msgs)-1].GetPayload())
	}
	return kvr
}

// TestKVHandler_SetNX_AcquireThenContend verifies SET_NX acquires once and the
// second attempt reports applied=false (key already held).
func TestKVHandler_SetNX_AcquireThenContend(t *testing.T) {
	store := newMockKVReadWriter()
	h := newTestKVHandler(store)

	op := &pb.KVOperation{
		Op:        pb.KVOperation_SET_NX,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "lock",
		Value:     []byte("owner-a"),
		Workspace: "ws1",
	}
	cb, msgs := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb); err != nil {
		t.Fatalf("SET_NX: %v", err)
	}
	if r := lastKV(t, *msgs); !r.Success || !r.Applied {
		t.Fatalf("first SET_NX success=%v applied=%v, want true/true", r.Success, r.Applied)
	}

	op2 := &pb.KVOperation{
		Op:        pb.KVOperation_SET_NX,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "lock",
		Value:     []byte("owner-b"),
		Workspace: "ws1",
	}
	cb2, msgs2 := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op2, cb2); err != nil {
		t.Fatalf("SET_NX 2: %v", err)
	}
	if r := lastKV(t, *msgs2); !r.Success || r.Applied {
		t.Fatalf("second SET_NX success=%v applied=%v, want true/false", r.Success, r.Applied)
	}
}

// TestKVHandler_CompareAndSet_MatchAndMismatch verifies CAS routes the
// expected_value and reports applied correctly.
func TestKVHandler_CompareAndSet_MatchAndMismatch(t *testing.T) {
	store := newMockKVReadWriter()
	h := newTestKVHandler(store)
	id := uuid.New()

	// Seed via SET_NX.
	seed := &pb.KVOperation{Op: pb.KVOperation_SET_NX, Scope: pb.KVOperation_GLOBAL, Key: "k", Value: []byte("owner-a"), Workspace: "ws1"}
	cb, _ := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, seed, cb); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Mismatch.
	mis := &pb.KVOperation{Op: pb.KVOperation_COMPARE_AND_SET, Scope: pb.KVOperation_GLOBAL, Key: "k", ExpectedValue: []byte("owner-x"), Value: []byte("owner-b"), Workspace: "ws1"}
	cbM, msgsM := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, mis, cbM); err != nil {
		t.Fatalf("CAS mismatch: %v", err)
	}
	if r := lastKV(t, *msgsM); !r.Success || r.Applied {
		t.Fatalf("CAS mismatch success=%v applied=%v, want true/false", r.Success, r.Applied)
	}

	// Match.
	mat := &pb.KVOperation{Op: pb.KVOperation_COMPARE_AND_SET, Scope: pb.KVOperation_GLOBAL, Key: "k", ExpectedValue: []byte("owner-a"), Value: []byte("owner-b"), Workspace: "ws1"}
	cbA, msgsA := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, mat, cbA); err != nil {
		t.Fatalf("CAS match: %v", err)
	}
	if r := lastKV(t, *msgsA); !r.Success || !r.Applied {
		t.Fatalf("CAS match success=%v applied=%v, want true/true", r.Success, r.Applied)
	}
}

// TestKVHandler_CompareAndDelete_MatchReleases verifies CAD releases only on a
// matching expected_value.
func TestKVHandler_CompareAndDelete_MatchReleases(t *testing.T) {
	store := newMockKVReadWriter()
	h := newTestKVHandler(store)
	id := uuid.New()

	seed := &pb.KVOperation{Op: pb.KVOperation_SET_NX, Scope: pb.KVOperation_GLOBAL, Key: "k", Value: []byte("owner-a"), Workspace: "ws1"}
	cb, _ := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, seed, cb); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Wrong owner cannot release.
	wrong := &pb.KVOperation{Op: pb.KVOperation_COMPARE_AND_DELETE, Scope: pb.KVOperation_GLOBAL, Key: "k", ExpectedValue: []byte("owner-x"), Workspace: "ws1"}
	cbW, msgsW := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, wrong, cbW); err != nil {
		t.Fatalf("CAD wrong: %v", err)
	}
	if r := lastKV(t, *msgsW); !r.Success || r.Applied {
		t.Fatalf("CAD wrong success=%v applied=%v, want true/false", r.Success, r.Applied)
	}

	// Owner releases; key is then re-acquirable.
	right := &pb.KVOperation{Op: pb.KVOperation_COMPARE_AND_DELETE, Scope: pb.KVOperation_GLOBAL, Key: "k", ExpectedValue: []byte("owner-a"), Workspace: "ws1"}
	cbR, msgsR := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, right, cbR); err != nil {
		t.Fatalf("CAD right: %v", err)
	}
	if r := lastKV(t, *msgsR); !r.Success || !r.Applied {
		t.Fatalf("CAD right success=%v applied=%v, want true/true", r.Success, r.Applied)
	}

	reacq := &pb.KVOperation{Op: pb.KVOperation_SET_NX, Scope: pb.KVOperation_GLOBAL, Key: "k", Value: []byte("owner-c"), Workspace: "ws1"}
	cbX, msgsX := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, reacq, cbX); err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if r := lastKV(t, *msgsX); !r.Success || !r.Applied {
		t.Fatalf("re-acquire after release success=%v applied=%v, want true/true", r.Success, r.Applied)
	}
}

// TestKVHandler_UserIdentity_DeniedForSetNX confirms the principal-type gate
// still rejects non-agent/task/service callers for the new ops.
func TestKVHandler_UserIdentity_DeniedForSetNX(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	op := &pb.KVOperation{Op: pb.KVOperation_SET_NX, Scope: pb.KVOperation_GLOBAL, Key: "lock", Value: []byte("x")}
	cb, _ := captureResponses()
	if err := h.HandleKVOperation(context.Background(), userIdentity, uuid.New(), nil, op, cb); err == nil {
		t.Fatal("expected permission denied for User principal on SET_NX")
	}
}
