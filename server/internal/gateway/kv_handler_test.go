package gateway

// Tests for KVHandler.HandleKVOperation():
//   - Only agents and tasks can access KV store (permission check)
//   - Workspace scope writes allowed with nil ACL (no PostgreSQL)
//   - Workspace scope writes denied when ACL denies access
//   - Workspace scope writes allowed when ACL grants access
//   - ACL errors fail closed (operation denied with Internal error)
//   - Successful GET sends KVResponse with Success=true
//   - Failed GET returns error
//   - Successful PUT (global scope) sends KVResponse with Success=true
//   - Successful DELETE (global scope) sends KVResponse with Success=true
//   - Successful LIST sends KVResponse with keys and map
//   - Failed LIST returns error
//   - kvOperations metric records "ok" vs "error" status (behaviour tested
//     indirectly via the returned error)
//   - Unknown KV operation returns error

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestKVHandler creates a KVHandler with a mock KV store, no audit logger, and no ACL service.
func newTestKVHandler(store *mockKVReadWriter) *KVHandler {
	return NewKVHandler(store, nil, nil)
}

// newTestKVHandlerWithACL creates a KVHandler with a mock KV store and an ACL checker.
func newTestKVHandlerWithACL(store *mockKVReadWriter, aclChecker KVACLChecker) *KVHandler {
	return NewKVHandler(store, nil, aclChecker)
}

// captureResponses builds a sendResponse callback that records all sent messages.
func captureResponses() (func(*pb.DownstreamMessage), *[]*pb.DownstreamMessage) {
	var msgs []*pb.DownstreamMessage
	cb := func(msg *pb.DownstreamMessage) {
		msgs = append(msgs, msg)
	}
	return cb, &msgs
}

var agentIdentity = models.Identity{
	Type:           models.PrincipalAgent,
	Workspace:      "ws1",
	Implementation: "worker",
	Specifier:      "v1",
}

var taskIdentity = models.Identity{
	Type:           models.PrincipalTask,
	Workspace:      "ws1",
	Implementation: "batch",
	Specifier:      "job-1",
}

var userIdentity = models.Identity{
	Type:      models.PrincipalUser,
	Workspace: "ws1",
	ID:        "alice",
}

// ---------------------------------------------------------------------------
// Principal type permission check
// ---------------------------------------------------------------------------

func TestKVHandler_UserIdentity_ReturnsPermissionDenied(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_GET,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "some-key",
	}

	err := h.HandleKVOperation(context.Background(), userIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error for User identity accessing KV store, got nil")
	}
}

func TestKVHandler_OrchestratorIdentity_ReturnsPermissionDenied(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, _ := captureResponses()

	orchIdentity := models.Identity{Type: models.PrincipalOrchestrator, Implementation: "k8s", Specifier: "primary"}
	op := &pb.KVOperation{
		Op:    pb.KVOperation_GET,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "some-key",
	}

	err := h.HandleKVOperation(context.Background(), orchIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error for Orchestrator identity accessing KV store, got nil")
	}
}

func TestKVHandler_AgentIdentity_Permitted(t *testing.T) {
	store := newMockKVReadWriter()
	h := newTestKVHandler(store)
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_GET,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "test-key",
		Workspace: "ws1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error for Agent identity: %v", err)
	}
	if len(*msgs) == 0 {
		t.Error("expected a response message to be sent for successful GET")
	}
}

func TestKVHandler_TaskIdentity_Permitted(t *testing.T) {
	store := newMockKVReadWriter()
	h := newTestKVHandler(store)
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_GET,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "test-key",
		Workspace: "ws1",
	}

	err := h.HandleKVOperation(context.Background(), taskIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error for Task identity: %v", err)
	}
	if len(*msgs) == 0 {
		t.Error("expected a response message to be sent for successful GET")
	}
}

// ---------------------------------------------------------------------------
// Workspace scope: nil ACL allows all writes (no PostgreSQL)
// ---------------------------------------------------------------------------

func TestKVHandler_PutWorkspaceScope_NilACL_Succeeds(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_WORKSPACE,
		Key:   "config-key",
		Value: []byte("value"),
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected nil ACL to allow workspace PUT, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true for workspace PUT with nil ACL")
	}
}

func TestKVHandler_DeleteWorkspaceScope_NilACL_Succeeds(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_DELETE,
		Scope: pb.KVOperation_WORKSPACE,
		Key:   "config-key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected nil ACL to allow workspace DELETE, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true for workspace DELETE with nil ACL")
	}
}

func TestKVHandler_IncrementWorkspaceScope_NilACL_Succeeds(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_INCREMENT,
		Scope: pb.KVOperation_WORKSPACE,
		Key:   "counter-key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected nil ACL to allow workspace INCREMENT, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true for workspace INCREMENT with nil ACL")
	}
}

func TestKVHandler_DecrementWorkspaceScope_NilACL_Succeeds(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_DECREMENT,
		Scope: pb.KVOperation_WORKSPACE,
		Key:   "counter-key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected nil ACL to allow workspace DECREMENT, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true for workspace DECREMENT with nil ACL")
	}
}

// ---------------------------------------------------------------------------
// GET operation
// ---------------------------------------------------------------------------

func TestKVHandler_Get_SuccessSendsKVResponseWithSuccessTrue(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_GET,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "my-key",
		RequestId: "req-1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*msgs) == 0 {
		t.Fatal("expected a KV response message, got none")
	}
	kvResp := (*msgs)[0].GetKv()
	if kvResp == nil {
		t.Fatal("expected KVResponse payload, got nil")
	}
	if !kvResp.Success {
		t.Error("expected Success=true in KVResponse")
	}
	if kvResp.RequestId != "req-1" {
		t.Errorf("expected RequestId='req-1', got %q", kvResp.RequestId)
	}
}

func TestKVHandler_Get_StoreError_ReturnsError(t *testing.T) {
	store := newMockKVReadWriter()
	store.getErr = errors.New("redis unavailable")
	h := newTestKVHandler(store)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_GET,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "my-key",
		Workspace: "ws1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error when KV store GET fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// PUT operation (global scope - allowed)
// ---------------------------------------------------------------------------

func TestKVHandler_Put_GlobalScope_SuccessSendsKVResponseWithSuccessTrue(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_PUT,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "global-key",
		Value:     []byte("global-value"),
		RequestId: "req-put-1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error for global scope PUT: %v", err)
	}
	if len(*msgs) == 0 {
		t.Fatal("expected a KV response message, got none")
	}
	kvResp := (*msgs)[0].GetKv()
	if kvResp == nil || !kvResp.Success {
		t.Errorf("expected Success=true, got %v", kvResp)
	}
	if kvResp.RequestId != "req-put-1" {
		t.Errorf("expected RequestId='req-put-1', got %q", kvResp.RequestId)
	}
}

func TestKVHandler_Put_GlobalScope_StoreError_ReturnsError(t *testing.T) {
	store := newMockKVReadWriter()
	store.setErr = errors.New("write failed")
	h := newTestKVHandler(store)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "global-key",
		Value: []byte("value"),
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error when KV store SET fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// DELETE operation (global scope - allowed)
// ---------------------------------------------------------------------------

func TestKVHandler_Delete_GlobalScope_SuccessSendsKVResponse(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_DELETE,
		Scope:     pb.KVOperation_GLOBAL,
		Key:       "stale-key",
		RequestId: "req-del-1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error for global scope DELETE: %v", err)
	}
	if len(*msgs) == 0 {
		t.Fatal("expected a KV response message, got none")
	}
	kvResp := (*msgs)[0].GetKv()
	if kvResp == nil || !kvResp.Success {
		t.Errorf("expected Success=true for DELETE, got %v", kvResp)
	}
}

func TestKVHandler_Delete_GlobalScope_StoreError_ReturnsError(t *testing.T) {
	store := newMockKVReadWriter()
	store.delErr = errors.New("delete failed")
	h := newTestKVHandler(store)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_DELETE,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "stale-key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error when KV store DELETE fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// LIST operation
// ---------------------------------------------------------------------------

func TestKVHandler_List_SuccessSendsKVResponseWithItems(t *testing.T) {
	store := newMockKVReadWriter()
	store.listData = map[string]string{"k1": "v1", "k2": "v2"}
	h := newTestKVHandler(store)
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_LIST,
		Scope:     pb.KVOperation_GLOBAL,
		RequestId: "req-list-1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error for LIST: %v", err)
	}
	if len(*msgs) == 0 {
		t.Fatal("expected a KV response message, got none")
	}
	kvResp := (*msgs)[0].GetKv()
	if kvResp == nil {
		t.Fatal("expected KVResponse payload, got nil")
	}
	if !kvResp.Success {
		t.Error("expected Success=true for LIST response")
	}
	if len(kvResp.KvMap) != 2 {
		t.Errorf("expected 2 items in KvMap, got %d", len(kvResp.KvMap))
	}
	if kvResp.RequestId != "req-list-1" {
		t.Errorf("expected RequestId='req-list-1', got %q", kvResp.RequestId)
	}
}

func TestKVHandler_List_StoreError_ReturnsError(t *testing.T) {
	store := newMockKVReadWriter()
	store.listErr = errors.New("list failed")
	h := newTestKVHandler(store)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_LIST,
		Scope: pb.KVOperation_GLOBAL,
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error when KV store LIST fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// Metric status: "ok" vs "error" (via error return value)
// ---------------------------------------------------------------------------

func TestKVHandler_SuccessfulOperation_ReturnsNilError(t *testing.T) {
	// A nil error from HandleKVOperation means the "ok" metric path was taken.
	h := newTestKVHandler(newMockKVReadWriter())
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_LIST,
		Scope: pb.KVOperation_GLOBAL,
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Errorf("expected nil error for successful operation (ok metric path), got %v", err)
	}
}

func TestKVHandler_FailedOperation_ReturnsNonNilError(t *testing.T) {
	// A non-nil error from HandleKVOperation means the "error" metric path was taken.
	store := newMockKVReadWriter()
	store.listErr = errors.New("backend down")
	h := newTestKVHandler(store)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_LIST,
		Scope: pb.KVOperation_GLOBAL,
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Error("expected non-nil error for failed operation (error metric path), got nil")
	}
}

// ---------------------------------------------------------------------------
// Unknown operation
// ---------------------------------------------------------------------------

func TestKVHandler_UnknownOperation_ReturnsError(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, _ := captureResponses()

	// Op value 99 is not a defined KVOperation_OpType variant.
	op := &pb.KVOperation{
		Op:    pb.KVOperation_OpType(99),
		Scope: pb.KVOperation_GLOBAL,
		Key:   "key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error for unknown KV operation, got nil")
	}
}

// ---------------------------------------------------------------------------
// Mock ACL checker for key-level permission tests
// ---------------------------------------------------------------------------

// mockACLChecker implements KVACLChecker for testing key-level permissions.
type mockACLChecker struct {
	// decisions maps "resourceType:resourceID" to a decision to return
	decisions map[string]*acl.ACLDecision
	// defaultDecision is returned when no mapping exists
	defaultDecision *acl.ACLDecision
	err             error
}

func newMockACLChecker() *mockACLChecker {
	return &mockACLChecker{
		decisions: make(map[string]*acl.ACLDecision),
		defaultDecision: &acl.ACLDecision{
			Allowed:         true,
			FallbackApplied: true,
			Decision:        acl.DecisionAllow,
			Reason:          "fallback allow",
		},
	}
}

func (m *mockACLChecker) CheckAccess(_ context.Context, _ models.Identity, resourceType, resourceID, _, _ string, _ uuid.UUID, requiredLevel int) (*acl.ACLDecision, error) {
	if m.err != nil {
		return nil, m.err
	}
	key := resourceType + ":" + resourceID
	if d, ok := m.decisions[key]; ok {
		return d, nil
	}
	return m.defaultDecision, nil
}

func (m *mockACLChecker) CheckAccessWithAuthority(ctx context.Context, principal models.Identity, _ *acl.ResolvedAuthority, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*acl.ACLDecision, error) {
	return m.CheckAccess(ctx, principal, resourceType, resourceID, operation, workspace, sessionID, requiredLevel)
}

// setKeyRule sets an explicit (non-fallback) decision for a kv_key resource.
func (m *mockACLChecker) setKeyRule(keyName string, allowed bool, level int) {
	decision := acl.DecisionAllow
	if !allowed {
		decision = acl.DecisionDeny
	}
	m.decisions[acl.ResourceTypeKVKey+":"+keyName] = &acl.ACLDecision{
		Allowed:              allowed,
		EffectiveAccessLevel: level,
		FallbackApplied:      false,
		Decision:             decision,
		Reason:               "explicit key rule",
	}
}

// setScopeRule sets an explicit (non-fallback) decision for a kv_scope resource.
func (m *mockACLChecker) setScopeRule(scope string, allowed bool, level int) {
	decision := acl.DecisionAllow
	if !allowed {
		decision = acl.DecisionDeny
	}
	m.decisions[acl.ResourceTypeKVScope+":"+scope] = &acl.ACLDecision{
		Allowed:              allowed,
		EffectiveAccessLevel: level,
		FallbackApplied:      false,
		Decision:             decision,
		Reason:               "explicit scope rule",
	}
}

// ---------------------------------------------------------------------------
// Key-level permission tests
// ---------------------------------------------------------------------------

func TestKVHandler_KeyLevelDeny_OverridesScopeAllow_Write(t *testing.T) {
	aclMock := newMockACLChecker()
	// Key-level: deny write for "protected-key"
	aclMock.setKeyRule("protected-key", false, acl.AccessNone)
	// Scope-level: allow (default fallback)

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "protected-key",
		Value: []byte("should-fail"),
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected PermissionDenied for key-level deny, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

func TestKVHandler_KeyLevelAllow_OverridesScopeDeny_Write(t *testing.T) {
	aclMock := newMockACLChecker()
	// Key-level: allow write for "allowed-key"
	aclMock.setKeyRule("allowed-key", true, acl.AccessReadWrite)
	// Scope-level: deny
	aclMock.setScopeRule("global", false, acl.AccessNone)

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "allowed-key",
		Value: []byte("should-succeed"),
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected key-level allow to override scope deny, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true")
	}
}

func TestKVHandler_NoKeyRule_FallsThrough_ToScopeCheck(t *testing.T) {
	aclMock := newMockACLChecker()
	// No key-level rule (default fallback with FallbackApplied=true)
	// Scope-level: deny
	aclMock.setScopeRule("global", false, acl.AccessNone)

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "some-key",
		Value: []byte("value"),
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected scope-level deny after key fallthrough, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied from scope check, got %v", st.Code())
	}
}

func TestKVHandler_KeyLevelDeny_BlocksRead(t *testing.T) {
	aclMock := newMockACLChecker()
	// Key-level: deny read for "secret-key"
	aclMock.setKeyRule("secret-key", false, acl.AccessNone)

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_GET,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "secret-key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected PermissionDenied for key-level read deny, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

func TestKVHandler_KeyLevelAllow_PermitsRead(t *testing.T) {
	aclMock := newMockACLChecker()
	// Key-level: allow read for "readable-key"
	aclMock.setKeyRule("readable-key", true, acl.AccessRead)

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_GET,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "readable-key",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected key-level read allow, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true for permitted read")
	}
}

func TestKVHandler_ListOnlyChecksScopeLevel(t *testing.T) {
	aclMock := newMockACLChecker()
	// Scope-level: deny read
	aclMock.setScopeRule("global", false, acl.AccessNone)

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_LIST,
		Scope: pb.KVOperation_GLOBAL,
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected PermissionDenied for LIST with scope deny, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

func TestKVHandler_ACLError_FailsClosed(t *testing.T) {
	aclMock := newMockACLChecker()
	aclMock.err = errors.New("database unreachable")

	h := newTestKVHandlerWithACL(newMockKVReadWriter(), aclMock)
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "any-key",
		Value: []byte("value"),
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected ACL error to fail closed, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal error code, got %v", st.Code())
	}
}

// ---------------------------------------------------------------------------
// Scope mapping: scope from op overrides default
// ---------------------------------------------------------------------------

func TestKVHandler_ListWithWorkspaceScope_UsesIdentityWorkspaceWhenOpWorkspaceEmpty(t *testing.T) {
	store := newMockKVReadWriter()
	store.listData = map[string]string{"ws-key": "ws-val"}
	h := newTestKVHandler(store)
	cb, msgs := captureResponses()

	// Op.Workspace is empty → should fall back to identity.Workspace
	op := &pb.KVOperation{
		Op:        pb.KVOperation_LIST,
		Scope:     pb.KVOperation_WORKSPACE,
		Workspace: "", // explicitly empty
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*msgs) == 0 {
		t.Fatal("expected response message")
	}
	kvResp := (*msgs)[0].GetKv()
	if !kvResp.Success {
		t.Error("expected Success=true")
	}
}

// ---------------------------------------------------------------------------
// itemsToKeys helper
// ---------------------------------------------------------------------------

func TestItemsToKeys_EmptyMap_ReturnsEmptySlice(t *testing.T) {
	keys := itemsToKeys(map[string]string{})
	if len(keys) != 0 {
		t.Errorf("expected empty slice, got %v", keys)
	}
}

func TestItemsToKeys_SingleEntry_ReturnsSingleKey(t *testing.T) {
	keys := itemsToKeys(map[string]string{"alpha": "1"})
	if len(keys) != 1 || keys[0] != "alpha" {
		t.Errorf("expected [\"alpha\"], got %v", keys)
	}
}

func TestItemsToKeys_MultipleEntries_ReturnsAllKeys(t *testing.T) {
	m := map[string]string{"a": "1", "b": "2", "c": "3"}
	keys := itemsToKeys(m)
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
	}
	// Verify each key from the map appears in the result.
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	for k := range m {
		if !keySet[k] {
			t.Errorf("key %q missing from itemsToKeys result", k)
		}
	}
}

// ---------------------------------------------------------------------------
// Scope validation integration: invalid scope config returns error
// ---------------------------------------------------------------------------

func TestKVHandler_UserScope_MissingUserID_ReturnsInvalidArgument(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, _ := captureResponses()

	// USER scope requires a non-empty userID - omitting it triggers ValidateScopeConfig to fail.
	op := &pb.KVOperation{
		Op:        pb.KVOperation_GET,
		Scope:     pb.KVOperation_USER,
		Key:       "user-key",
		UserId:    "", // missing
		Workspace: "ws1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected error when userID missing for USER scope, got nil")
	}
}

// ---------------------------------------------------------------------------
// TTL parsing: positive TTL sets duration; zero/negative TTL is ignored
// ---------------------------------------------------------------------------

func TestKVHandler_Put_WithPositiveTTL_DoesNotError(t *testing.T) {
	h := newTestKVHandler(newMockKVReadWriter())
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL,
		Key:   "ttl-key",
		Value: []byte("ttl-value"),
		Ttl:   60, // 60 seconds
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("unexpected error for PUT with TTL: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true for PUT with TTL")
	}
}

// ---------------------------------------------------------------------------
// KV scope constants: ensure scope mapping in HandleKVOperation is correct
// ---------------------------------------------------------------------------

func TestKVHandler_GlobalScopeEnum_MapsToScopeGlobal(t *testing.T) {
	// Verify ScopeGlobal value matches expectations (regression guard).
	if kv.ScopeGlobal != "global" {
		t.Errorf("expected ScopeGlobal='global', got %q", kv.ScopeGlobal)
	}
}

func TestKVHandler_WorkspaceScopeEnum_MapsToScopeWorkspace(t *testing.T) {
	if kv.ScopeWorkspace != "workspace" {
		t.Errorf("expected ScopeWorkspace='workspace', got %q", kv.ScopeWorkspace)
	}
}

// ---------------------------------------------------------------------------
// Owner fast-path: exclusive scope + nil authority bypasses ACL
// ---------------------------------------------------------------------------

// denyAllACLChecker is a KVACLChecker that denies every access attempt.
// It is used to confirm that the owner fast-path is taken (i.e. the ACL DB
// is never consulted) when scope is exclusive and authority == nil.
type denyAllACLChecker struct{}

func (d *denyAllACLChecker) CheckAccess(_ context.Context, _ models.Identity, _, _, _, _ string, _ uuid.UUID, _ int) (*acl.ACLDecision, error) {
	return &acl.ACLDecision{
		Allowed:  false,
		Decision: acl.DecisionDeny,
		Reason:   "deny-all test checker",
	}, nil
}

func (d *denyAllACLChecker) CheckAccessWithAuthority(_ context.Context, _ models.Identity, _ *acl.ResolvedAuthority, _, _, _, _ string, _ uuid.UUID, _ int) (*acl.ACLDecision, error) {
	return &acl.ACLDecision{
		Allowed:  false,
		Decision: acl.DecisionDeny,
		Reason:   "deny-all test checker",
	}, nil
}

// TestKVHandler_OwnerFastPath_ExclusiveScope_NilAuthority_Succeeds verifies
// that when scope is GLOBAL_EXCLUSIVE and authority == nil the handler allows
// the write without consulting the ACL service (owner fast-path).
func TestKVHandler_OwnerFastPath_ExclusiveScope_NilAuthority_Succeeds(t *testing.T) {
	// Use a deny-all ACL checker; the fast-path must bypass it entirely.
	h := NewKVHandler(newMockKVReadWriter(), nil, &denyAllACLChecker{})
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_GLOBAL_EXCLUSIVE,
		Key:   "my-config",
		Value: []byte("value"),
	}

	// authority == nil means direct (non-OBO) authority → owner fast-path applies.
	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected owner fast-path to allow write on exclusive scope, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true via owner fast-path")
	}
}

// TestKVHandler_OwnerFastPath_WorkspaceExclusive_NilAuthority_Succeeds
// checks the same fast-path for WORKSPACE_EXCLUSIVE scope.
func TestKVHandler_OwnerFastPath_WorkspaceExclusive_NilAuthority_Succeeds(t *testing.T) {
	h := NewKVHandler(newMockKVReadWriter(), nil, &denyAllACLChecker{})
	cb, msgs := captureResponses()

	op := &pb.KVOperation{
		Op:        pb.KVOperation_PUT,
		Scope:     pb.KVOperation_WORKSPACE_EXCLUSIVE,
		Key:       "ws-config",
		Value:     []byte("value"),
		Workspace: "ws1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err != nil {
		t.Fatalf("expected owner fast-path to allow workspace-exclusive write, got error: %v", err)
	}
	if len(*msgs) == 0 || !(*msgs)[0].GetKv().Success {
		t.Error("expected Success=true via owner fast-path for workspace-exclusive scope")
	}
}

// TestKVHandler_NoFastPath_SharedScope_ACLDenies verifies that a shared
// (non-exclusive) scope does NOT get the owner fast-path: the deny-all ACL
// checker is consulted and the request is rejected.
func TestKVHandler_NoFastPath_SharedScope_ACLDenies(t *testing.T) {
	h := NewKVHandler(newMockKVReadWriter(), nil, &denyAllACLChecker{})
	cb, _ := captureResponses()

	op := &pb.KVOperation{
		Op:    pb.KVOperation_PUT,
		Scope: pb.KVOperation_USER_SHARED,
		Key:   "shared-key",
		Value: []byte("value"),
		// USER_SHARED requires a userID
		UserId:    "alice",
		Workspace: "ws1",
	}

	err := h.HandleKVOperation(context.Background(), agentIdentity, uuid.New(), nil, op, cb)
	if err == nil {
		t.Fatal("expected ACL deny for non-exclusive shared scope, got nil error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

// ---------------------------------------------------------------------------
// DECREMENT_IF end-to-end via gateway handler with real BadgerKVStore
// ---------------------------------------------------------------------------

// newTestBadgerKVHandlerForGateway creates a KVHandler backed by an in-memory
// Badger DB. Using InMemory avoids on-disk flush goroutines that can cause
// test timeouts when db.Close() is called under the test framework.
func newTestBadgerKVHandlerForGateway(t *testing.T) *KVHandler {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open (in-memory): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewKVHandler(kv.NewBadgerKVStore(db), nil, nil)
}

// TestKVHandler_DecrementIf_GatewayHandler seeds a balance of 100 via
// INCREMENT operations through the gateway handler, then fires N concurrent
// DECREMENT_IF ops with guard_value=0 (floor). Asserts that applied ops
// equal exactly balance/1 (delta is always 1 in the handler) and the final
// balance is 0.
func TestKVHandler_DecrementIf_GatewayHandler_ConcurrentOps(t *testing.T) {
	h := newTestBadgerKVHandlerForGateway(t)

	ctx := context.Background()
	const balance = 20

	// Seed balance via INCREMENT (delta=1, so 20 calls).
	for i := 0; i < balance; i++ {
		cb, _ := captureResponses()
		op := &pb.KVOperation{
			Op:    pb.KVOperation_INCREMENT,
			Scope: pb.KVOperation_GLOBAL_EXCLUSIVE,
			Key:   "balance",
		}
		if err := h.HandleKVOperation(ctx, agentIdentity, uuid.New(), nil, op, cb); err != nil {
			t.Fatalf("INCREMENT seed [%d]: %v", i, err)
		}
	}

	// Fire concurrent DECREMENT_IF ops.
	const goroutines = 40
	var appliedCount atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			cb, msgs := captureResponses()
			op := &pb.KVOperation{
				Op:         pb.KVOperation_DECREMENT_IF,
				Scope:      pb.KVOperation_GLOBAL_EXCLUSIVE,
				Key:        "balance",
				GuardValue: 0, // floor = 0
			}
			if err := h.HandleKVOperation(ctx, agentIdentity, uuid.New(), nil, op, cb); err != nil {
				t.Errorf("DECREMENT_IF: %v", err)
				return
			}
			if len(*msgs) > 0 && (*msgs)[0].GetKv().Applied {
				appliedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	// Exactly `balance` decrements should have been applied (delta=1).
	if int(appliedCount.Load()) != balance {
		t.Errorf("applied=%d, want %d", appliedCount.Load(), balance)
	}

	// Final balance must be 0.
	cb, msgs := captureResponses()
	getOp := &pb.KVOperation{
		Op:    pb.KVOperation_GET,
		Scope: pb.KVOperation_GLOBAL_EXCLUSIVE,
		Key:   "balance",
	}
	if err := h.HandleKVOperation(ctx, agentIdentity, uuid.New(), nil, getOp, cb); err != nil {
		t.Fatalf("GET final balance: %v", err)
	}
	if len(*msgs) == 0 {
		t.Fatal("expected GET response, got none")
	}
	finalVal := string((*msgs)[0].GetKv().Value)
	if finalVal != "0" {
		t.Errorf("final balance = %q, want \"0\"", finalVal)
	}
}
