package gateway

// denial_audit_test.go — focused tests verifying that terminal authorization
// denials in checkKeyPermission / checkScopeReadPermission (KV) and
// authorizeTaskOp (task) are captured in the audit log with success=false.

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	auditstore "github.com/scitrera/aether/internal/storage/audit"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// ---------------------------------------------------------------------------
// captureAuditStore — minimal auditstore.Store that records LogEvent calls.
// ---------------------------------------------------------------------------

type captureAuditStore struct {
	mu     sync.Mutex
	events []*auditstore.Event
}

func (c *captureAuditStore) LogEvent(_ context.Context, event *auditstore.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

func (c *captureAuditStore) LogEventSync(_ context.Context, event *auditstore.Event) error {
	c.LogEvent(context.Background(), event)
	return nil
}

func (c *captureAuditStore) Close() error { return nil }

func (c *captureAuditStore) QueryAuditLog(_ context.Context, _ auditstore.EventFilter) ([]*auditstore.Event, error) {
	return nil, nil
}

func (c *captureAuditStore) CleanupOldLogs(_ context.Context, _ int) (int64, error) {
	return 0, nil
}

func (c *captureAuditStore) GetConfig() *auditstore.Config { return nil }

func (c *captureAuditStore) captured() []*auditstore.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*auditstore.Event, len(c.events))
	copy(out, c.events)
	return out
}

// ---------------------------------------------------------------------------
// KV denial audit tests
// ---------------------------------------------------------------------------

// denyReasonACLChecker always returns a non-fallback denied decision with the
// supplied reason — used to exercise the key-level denial branch.
type denyReasonACLChecker struct {
	reason string
}

func (d *denyReasonACLChecker) CheckAccess(_ context.Context, _ models.Identity, _, _, _, _ string, _ uuid.UUID, _ int) (*acl.ACLDecision, error) {
	return &acl.ACLDecision{
		Allowed:         false,
		FallbackApplied: false, // explicit key-level rule
		Decision:        acl.DecisionDeny,
		Reason:          d.reason,
	}, nil
}

func (d *denyReasonACLChecker) CheckAccessWithAuthority(_ context.Context, _ models.Identity, _ *acl.ResolvedAuthority, _, _, _, _ string, _ uuid.UUID, _ int) (*acl.ACLDecision, error) {
	return &acl.ACLDecision{
		Allowed:         false,
		FallbackApplied: false,
		Decision:        acl.DecisionDeny,
		Reason:          d.reason,
	}, nil
}

// TestKVDenial_checkKeyPermission_EmitsAuditEvent verifies that a key-level
// ACL denial in checkKeyPermission produces a success=false audit event with
// the correct operation and identity.
func TestKVDenial_checkKeyPermission_EmitsAuditEvent(t *testing.T) {
	cap := &captureAuditStore{}
	h := &KVHandler{
		kvStore:     newMockKVReadWriter(),
		auditLogger: cap,
		aclService:  &denyReasonACLChecker{reason: "test-key-deny"},
	}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "worker", Specifier: "s1"}
	sid := uuid.New()

	err := h.checkKeyPermission(context.Background(), identity, nil, kv.ScopeUserShared, "some-key", audit.OpKVGet, "ws1", sid, acl.AccessRead)
	if err == nil {
		t.Fatal("expected denial error, got nil")
	}

	events := cap.captured()
	if len(events) == 0 {
		t.Fatal("expected audit event on denial, got none")
	}
	ev := events[0]
	if ev.Success {
		t.Errorf("audit event Success = true; want false")
	}
	if ev.Operation != audit.OpKVGet {
		t.Errorf("audit event Operation = %q; want %q", ev.Operation, audit.OpKVGet)
	}
	if ev.ErrorMessage != "test-key-deny" {
		t.Errorf("audit event ErrorMessage = %q; want %q", ev.ErrorMessage, "test-key-deny")
	}
}

// fallbackDenyACLChecker returns a fallback-applied deny at scope level (no
// explicit key rule), so checkKeyPermission falls through to the scope check.
type fallbackDenyACLChecker struct {
	reason string
}

func (f *fallbackDenyACLChecker) CheckAccess(_ context.Context, _ models.Identity, _, _, _, _ string, _ uuid.UUID, _ int) (*acl.ACLDecision, error) {
	return &acl.ACLDecision{
		Allowed:         false,
		FallbackApplied: true, // no key-level rule → go to scope
		Decision:        acl.DecisionDeny,
		Reason:          f.reason,
	}, nil
}

func (f *fallbackDenyACLChecker) CheckAccessWithAuthority(_ context.Context, _ models.Identity, _ *acl.ResolvedAuthority, _, _, _, _ string, _ uuid.UUID, _ int) (*acl.ACLDecision, error) {
	return &acl.ACLDecision{
		Allowed:         false,
		FallbackApplied: true,
		Decision:        acl.DecisionDeny,
		Reason:          f.reason,
	}, nil
}

// TestKVDenial_checkKeyPermission_ScopeLevel_EmitsAuditEvent covers the scope
// fall-through denial branch (no explicit key rule → scope-level deny).
func TestKVDenial_checkKeyPermission_ScopeLevel_EmitsAuditEvent(t *testing.T) {
	cap := &captureAuditStore{}
	h := &KVHandler{
		kvStore:     newMockKVReadWriter(),
		auditLogger: cap,
		aclService:  &fallbackDenyACLChecker{reason: "scope-fallback-deny"},
	}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "w", Specifier: "s"}
	sid := uuid.New()

	err := h.checkKeyPermission(context.Background(), identity, nil, kv.ScopeUserShared, "any-key", audit.OpKVPut, "ws1", sid, acl.AccessReadWrite)
	if err == nil {
		t.Fatal("expected denial error, got nil")
	}

	events := cap.captured()
	if len(events) == 0 {
		t.Fatal("expected audit event on scope-level denial, got none")
	}
	ev := events[0]
	if ev.Success {
		t.Errorf("audit event Success = true; want false")
	}
	if ev.ErrorMessage != "scope-fallback-deny" {
		t.Errorf("audit event ErrorMessage = %q; want %q", ev.ErrorMessage, "scope-fallback-deny")
	}
}

// TestKVDenial_checkScopeReadPermission_EmitsAuditEvent covers the LIST path
// (no specific key, scope-only check via checkScopeReadPermission).
func TestKVDenial_checkScopeReadPermission_EmitsAuditEvent(t *testing.T) {
	cap := &captureAuditStore{}
	h := &KVHandler{
		kvStore:     newMockKVReadWriter(),
		auditLogger: cap,
		aclService:  &denyReasonACLChecker{reason: "list-scope-deny"},
	}

	identity := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	sid := uuid.New()

	err := h.checkScopeReadPermission(context.Background(), identity, nil, kv.ScopeUserShared, audit.OpKVList, "ws1", sid)
	if err == nil {
		t.Fatal("expected denial error, got nil")
	}

	events := cap.captured()
	if len(events) == 0 {
		t.Fatal("expected audit event on scope-read denial, got none")
	}
	ev := events[0]
	if ev.Success {
		t.Errorf("audit event Success = true; want false")
	}
	if ev.Operation != audit.OpKVList {
		t.Errorf("audit event Operation = %q; want %q", ev.Operation, audit.OpKVList)
	}
	if ev.ErrorMessage != "list-scope-deny" {
		t.Errorf("audit event ErrorMessage = %q; want %q", ev.ErrorMessage, "list-scope-deny")
	}
}

// ---------------------------------------------------------------------------
// authorizeTaskOp denial tests — workspace-mismatch branch
// ---------------------------------------------------------------------------

// TestAuthorizeTaskOp_WorkspaceMismatch_EmitsAuditEvent verifies that the
// workspace-mismatch guard in authorizeTaskOp emits a task_authz_denied audit
// event with success=false.
func TestAuthorizeTaskOp_WorkspaceMismatch_EmitsAuditEvent(t *testing.T) {
	cap := &captureAuditStore{}
	gw := &GatewayServer{auditLogger: cap}

	task := &tasks.Task{
		TaskID:    "task-xyz",
		Workspace: "wsA",
	}
	caller := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "wsB", // mismatch
		Implementation: "w",
		Specifier:      "1",
	}
	client := &ClientSession{
		SessionUUID: uuid.New(),
		Identity:    caller,
	}

	result := gw.authorizeTaskOp(context.Background(), client, task)
	if result {
		t.Fatal("expected authorizeTaskOp to return false for workspace mismatch")
	}

	events := cap.captured()
	if len(events) == 0 {
		t.Fatal("expected audit event on workspace-mismatch denial, got none")
	}
	ev := events[0]
	if ev.Success {
		t.Errorf("audit event Success = true; want false")
	}
	if ev.Operation != audit.OpTaskAuthzDenied {
		t.Errorf("audit event Operation = %q; want %q", ev.Operation, audit.OpTaskAuthzDenied)
	}
	if ev.ErrorMessage != "workspace mismatch" {
		t.Errorf("audit event ErrorMessage = %q; want %q", ev.ErrorMessage, "workspace mismatch")
	}
}

// TestAuthorizeTaskOp_NoAuditLogger_NilSafe verifies that authorizeTaskOp
// does not panic when auditLogger is nil (the nil-safe auditLog helper must
// guard the call).
func TestAuthorizeTaskOp_NoAuditLogger_NilSafe(t *testing.T) {
	gw := &GatewayServer{} // auditLogger == nil

	task := &tasks.Task{TaskID: "t1", Workspace: "wsA"}
	caller := models.Identity{Type: models.PrincipalAgent, Workspace: "wsB", Implementation: "w", Specifier: "1"}
	client := &ClientSession{SessionUUID: uuid.New(), Identity: caller}

	// Must not panic.
	result := gw.authorizeTaskOp(context.Background(), client, task)
	if result {
		t.Fatal("expected false for workspace mismatch")
	}
}
