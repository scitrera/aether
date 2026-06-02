package gateway

import (
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
)

var workflowEngineIdentity = models.Identity{Type: models.PrincipalWorkflowEngine}

// TestKVHandler_WorkflowEngine_AllowedOnCoordKey verifies the WorkflowEngine
// principal — previously blocked from KV entirely — can operate on the reserved
// coordination namespace, and that the infra fast-path bypasses a denying ACL
// for those keys.
func TestKVHandler_WorkflowEngine_AllowedOnCoordKey(t *testing.T) {
	aclMock := newMockACLChecker()
	aclMock.setScopeRule(string(kv.ScopeGlobal), false, 0) // deny global scope broadly
	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)

	op := &pb.KVOperation{
		Op:    pb.KVOperation_SET_NX,
		Scope: pb.KVOperation_GLOBAL,
		Key:   kv.ReservedCoordKeyPrefix + "workflow/leader",
		Value: []byte("owner-a"),
	}
	cb, msgs := captureResponses()
	if err := h.HandleKVOperation(context.Background(), workflowEngineIdentity, uuid.New(), nil, op, cb); err != nil {
		t.Fatalf("WFE SET_NX on coord key should be allowed via fast-path, got: %v", err)
	}
	if r := lastKV(t, *msgs); !r.Success || !r.Applied {
		t.Fatalf("WFE coord SET_NX success=%v applied=%v, want true/true", r.Success, r.Applied)
	}
}

// TestKVHandler_WorkflowEngine_DeniedOffCoordKey verifies the fast-path is
// scoped to the reserved namespace: a WFE op on a non-reserved key still goes
// through ACL and is denied when no grant exists.
func TestKVHandler_WorkflowEngine_DeniedOffCoordKey(t *testing.T) {
	aclMock := newMockACLChecker()
	aclMock.setScopeRule(string(kv.ScopeGlobal), false, 0)
	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)

	op := &pb.KVOperation{
		Op:    pb.KVOperation_SET_NX,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "app/data/key", // not under the reserved coord prefix
		Value: []byte("x"),
	}
	cb, _ := captureResponses()
	if err := h.HandleKVOperation(context.Background(), workflowEngineIdentity, uuid.New(), nil, op, cb); err == nil {
		t.Fatal("WFE SET_NX on a non-reserved key should be denied by ACL")
	}
}

// TestKVHandler_Agent_NoCoordFastPath verifies the infra fast-path is
// WFE-only: an Agent on a reserved coord key with a denying ACL is still denied.
func TestKVHandler_Agent_NoCoordFastPath(t *testing.T) {
	aclMock := newMockACLChecker()
	aclMock.setScopeRule(string(kv.ScopeGlobal), false, 0)
	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)

	op := &pb.KVOperation{
		Op:    pb.KVOperation_SET_NX,
		Scope: pb.KVOperation_GLOBAL,
		Key:   kv.ReservedCoordKeyPrefix + "workflow/leader",
		Value: []byte("x"),
	}
	cb, _ := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb); err == nil {
		t.Fatal("Agent on reserved coord key should NOT get the infra fast-path; expected ACL denial")
	}
}

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

// TestKVHandler_SetAdd_And_SetCard verifies SET_ADD reports applied + the
// running cardinality, dedupes a repeat member, and that SET_CARD reads back
// the set cardinality without mutating it.
func TestKVHandler_SetAdd_And_SetCard(t *testing.T) {
	store := newMockKVReadWriter()
	h := newTestKVHandler(store)
	id := uuid.New()

	// First member: newly added, cardinality 1.
	add1 := &pb.KVOperation{Op: pb.KVOperation_SET_ADD, Scope: pb.KVOperation_GLOBAL, Key: "members", Value: []byte("a"), Workspace: "ws1"}
	cb1, msgs1 := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, add1, cb1); err != nil {
		t.Fatalf("SET_ADD a: %v", err)
	}
	if r := lastKV(t, *msgs1); !r.Success || !r.Applied || r.CounterValue != 1 {
		t.Fatalf("SET_ADD a success=%v applied=%v card=%d, want true/true/1", r.Success, r.Applied, r.CounterValue)
	}

	// Second distinct member: newly added, cardinality 2.
	add2 := &pb.KVOperation{Op: pb.KVOperation_SET_ADD, Scope: pb.KVOperation_GLOBAL, Key: "members", Value: []byte("b"), Workspace: "ws1"}
	cb2, msgs2 := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, add2, cb2); err != nil {
		t.Fatalf("SET_ADD b: %v", err)
	}
	if r := lastKV(t, *msgs2); !r.Success || !r.Applied || r.CounterValue != 2 {
		t.Fatalf("SET_ADD b success=%v applied=%v card=%d, want true/true/2", r.Success, r.Applied, r.CounterValue)
	}

	// Repeat member: not newly added, cardinality unchanged at 2.
	add1Again := &pb.KVOperation{Op: pb.KVOperation_SET_ADD, Scope: pb.KVOperation_GLOBAL, Key: "members", Value: []byte("a"), Workspace: "ws1"}
	cb3, msgs3 := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, add1Again, cb3); err != nil {
		t.Fatalf("SET_ADD a (repeat): %v", err)
	}
	if r := lastKV(t, *msgs3); !r.Success || r.Applied || r.CounterValue != 2 {
		t.Fatalf("SET_ADD a repeat success=%v applied=%v card=%d, want true/false/2", r.Success, r.Applied, r.CounterValue)
	}

	// SET_CARD reads the cardinality back (read-only, no Applied semantics).
	card := &pb.KVOperation{Op: pb.KVOperation_SET_CARD, Scope: pb.KVOperation_GLOBAL, Key: "members", Workspace: "ws1"}
	cb4, msgs4 := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, card, cb4); err != nil {
		t.Fatalf("SET_CARD: %v", err)
	}
	if r := lastKV(t, *msgs4); !r.Success || r.CounterValue != 2 {
		t.Fatalf("SET_CARD success=%v card=%d, want true/2", r.Success, r.CounterValue)
	}

	// SET_CARD on an absent key returns 0.
	cardAbsent := &pb.KVOperation{Op: pb.KVOperation_SET_CARD, Scope: pb.KVOperation_GLOBAL, Key: "missing", Workspace: "ws1"}
	cb5, msgs5 := captureResponses()
	if err := h.HandleKVOperation(context.Background(), agentIdentity, id, nil, cardAbsent, cb5); err != nil {
		t.Fatalf("SET_CARD absent: %v", err)
	}
	if r := lastKV(t, *msgs5); !r.Success || r.CounterValue != 0 {
		t.Fatalf("SET_CARD absent success=%v card=%d, want true/0", r.Success, r.CounterValue)
	}
}
