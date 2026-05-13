package gateway

// Tests for handleSubmitAuditEvent() covering:
//   - Happy path: agent submits a permitted event_type and receives success.
//   - Identity stamping: client-supplied actor metadata is preserved but the
//     persisted ActorID is the authenticated identity, not anything from the
//     request.
//   - Event-type whitelist: rejected types (auth, connection, admin, acl)
//     return ERR_AUDIT_TYPE_FORBIDDEN without enqueueing an event.
//   - Cross-workspace ACL: missing ACL service or denied permission produces
//     ERR_PERMISSION_DENIED; same-workspace submissions skip the check.
//   - Metadata sanitization: credential-shaped keys are redacted in the
//     persisted event regardless of audit verbosity.
//   - Per-principal rate limit: a constrained limiter eventually rejects with
//     ERR_AUDIT_RATE_LIMITED.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/quota"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// In-process fake database/sql driver for capturing persisted audit events.
// Mirrors the helper used in server/internal/audit/logger_test.go; duplicated
// here because the audit package's fake is unexported.
// ---------------------------------------------------------------------------

type submitAuditCapturedExec struct {
	query string
	args  []driver.Value
}

type submitAuditFakeStmt struct {
	c     *submitAuditFakeConn
	query string
}

func (s *submitAuditFakeStmt) Close() error  { return nil }
func (s *submitAuditFakeStmt) NumInput() int { return -1 }
func (s *submitAuditFakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.execs = append(s.c.execs, submitAuditCapturedExec{query: s.query, args: append([]driver.Value(nil), args...)})
	return driver.RowsAffected(1), nil
}
func (s *submitAuditFakeStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("Query not supported by submit audit fake stmt")
}

type submitAuditFakeTx struct{ c *submitAuditFakeConn }

func (t *submitAuditFakeTx) Commit() error   { return nil }
func (t *submitAuditFakeTx) Rollback() error { return nil }

type submitAuditFakeConn struct {
	mu    sync.Mutex
	execs []submitAuditCapturedExec
}

func (c *submitAuditFakeConn) Prepare(query string) (driver.Stmt, error) {
	return &submitAuditFakeStmt{c: c, query: query}, nil
}
func (c *submitAuditFakeConn) Close() error              { return nil }
func (c *submitAuditFakeConn) Begin() (driver.Tx, error) { return &submitAuditFakeTx{c: c}, nil }

type submitAuditFakeDriver struct{ conn *submitAuditFakeConn }

func (d *submitAuditFakeDriver) Open(_ string) (driver.Conn, error) { return d.conn, nil }

func newSubmitAuditFakeDB(t *testing.T) (*sql.DB, *submitAuditFakeConn) {
	t.Helper()
	conn := &submitAuditFakeConn{}
	name := fmt.Sprintf("submit-audit-fake-%s-%d", t.Name(), time.Now().UnixNano())
	sql.Register(name, &submitAuditFakeDriver{conn: conn})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db, conn
}

func (c *submitAuditFakeConn) waitForExecs(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(c.execs)
		c.mu.Unlock()
		if n >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d execs", want)
}

// newEnabledAuditLogger returns a real AuditLogger backed by the captured fake
// DB so tests can assert on what would have been persisted.
func newEnabledAuditLogger(t *testing.T) (*audit.AuditLogger, *submitAuditFakeConn) {
	db, conn := newSubmitAuditFakeDB(t)
	cfg := audit.DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 50 * time.Millisecond
	cfg.ChannelBuffer = 32
	logger := audit.NewAuditLogger(db, "test-gateway", cfg)
	t.Cleanup(func() {
		_ = logger.Close()
		_ = db.Close()
	})
	return logger, conn
}

// newAuditSubmitTestClient builds a ClientSession suitable for handler tests.
func newAuditSubmitTestClient(stream *mockStream, principalType models.PrincipalType, workspace string) *ClientSession {
	identity := models.Identity{
		Type:           principalType,
		Workspace:      workspace,
		Implementation: "audit-test",
		Specifier:      "spec-1",
	}
	return &ClientSession{
		ID:            "sess-audit-submit",
		SessionUUID:   uuid.New(),
		Identity:      identity,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}
}

// newAuditSubmitTestServer builds a GatewayServer with the supplied audit
// logger (may be nil to exercise the "audit disabled" path) and no ACL —
// suitable for handler-level tests that only need handleSubmitAuditEvent.
func newAuditSubmitTestServer(logger *audit.AuditLogger, principalRL *quota.PrincipalRateLimiter) *GatewayServer {
	s := &GatewayServer{
		gatewayID:     "test-gateway",
		auditLogger:   logger,
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
	}
	s.quotaEnforcer.foreignAuditRateLimiter = principalRL
	return s
}

// lastSubmitAuditResponse returns the most recent SubmitAuditEventResponse
// from a mockStream, or nil if none were sent.
func lastSubmitAuditResponse(stream *mockStream) *pb.SubmitAuditEventResponse {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for i := len(stream.sent) - 1; i >= 0; i-- {
		if resp := stream.sent[i].GetSubmitAuditEventResponse(); resp != nil {
			return resp
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

func TestHandleSubmitAuditEvent_HappyPath(t *testing.T) {
	logger := audit.NewAuditLogger(nil, "test-gateway", &audit.Config{Enabled: false})
	defer logger.Close()
	// We use a disabled logger so LogEvent is a no-op but the handler still
	// returns success; the response shape is what we're asserting here.

	s := newAuditSubmitTestServer(logger, nil)
	stream := &mockStream{}
	client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "foo")

	req := &pb.SubmitAuditEventRequest{
		EventType:       audit.EventTypeMessage,
		Operation:       "completed_workflow_step",
		ResourceType:    "topic",
		ResourceId:      "ag.foo.worker.v1",
		Workspace:       "", // empty = caller's workspace
		Success:         true,
		ClientRequestId: "req-1",
		Metadata: map[string]string{
			"workflow_id": "abc",
		},
	}

	s.handleSubmitAuditEvent(context.Background(), client, req)

	resp := lastSubmitAuditResponse(stream)
	if resp == nil {
		t.Fatal("expected SubmitAuditEventResponse, got none")
	}
	if !resp.Success {
		t.Errorf("expected success=true, got false (error_code=%q, error_message=%q)", resp.ErrorCode, resp.ErrorMessage)
	}
	if resp.ClientRequestId != "req-1" {
		t.Errorf("expected request id echo, got %q", resp.ClientRequestId)
	}
}

// ---------------------------------------------------------------------------
// Event-type whitelist
// ---------------------------------------------------------------------------

func TestHandleSubmitAuditEvent_EventTypeAuth_Rejected(t *testing.T) {
	logger := audit.NewAuditLogger(nil, "test-gateway", &audit.Config{Enabled: false})
	defer logger.Close()
	s := newAuditSubmitTestServer(logger, nil)

	rejected := []string{
		audit.EventTypeAuth,
		audit.EventTypeConnection,
		audit.EventTypeAdmin,
		audit.EventTypeACL,
	}

	for _, et := range rejected {
		t.Run(et, func(t *testing.T) {
			stream := &mockStream{}
			client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "foo")

			s.handleSubmitAuditEvent(context.Background(), client, &pb.SubmitAuditEventRequest{
				EventType: et,
				Operation: "x",
			})

			resp := lastSubmitAuditResponse(stream)
			if resp == nil {
				t.Fatalf("expected response for rejected event type %s", et)
			}
			if resp.Success {
				t.Errorf("expected success=false for event type %s", et)
			}
			if resp.ErrorCode != errAuditTypeForbidden {
				t.Errorf("expected %s, got %s", errAuditTypeForbidden, resp.ErrorCode)
			}
		})
	}
}

func TestHandleSubmitAuditEvent_AllowedEventTypes(t *testing.T) {
	logger := audit.NewAuditLogger(nil, "test-gateway", &audit.Config{Enabled: false})
	defer logger.Close()
	s := newAuditSubmitTestServer(logger, nil)

	allowed := []string{
		audit.EventTypeMessage,
		audit.EventTypeKV,
		audit.EventTypeTask,
		audit.EventTypeCustom,
	}

	for _, et := range allowed {
		t.Run(et, func(t *testing.T) {
			stream := &mockStream{}
			client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "foo")

			s.handleSubmitAuditEvent(context.Background(), client, &pb.SubmitAuditEventRequest{
				EventType: et,
				Operation: "test_op",
				Success:   true,
			})

			resp := lastSubmitAuditResponse(stream)
			if resp == nil {
				t.Fatalf("expected response for event type %s", et)
			}
			if !resp.Success {
				t.Errorf("expected success=true for event type %s; got error_code=%s", et, resp.ErrorCode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-workspace ACL
// ---------------------------------------------------------------------------

func TestHandleSubmitAuditEvent_CrossWorkspaceNoACL_Denied(t *testing.T) {
	logger := audit.NewAuditLogger(nil, "test-gateway", &audit.Config{Enabled: false})
	defer logger.Close()
	s := newAuditSubmitTestServer(logger, nil) // acl == nil
	stream := &mockStream{}
	client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "_apps")

	s.handleSubmitAuditEvent(context.Background(), client, &pb.SubmitAuditEventRequest{
		EventType: audit.EventTypeCustom,
		Operation: "ping",
		Workspace: "something-else",
	})

	resp := lastSubmitAuditResponse(stream)
	if resp == nil {
		t.Fatal("expected SubmitAuditEventResponse")
	}
	if resp.Success {
		t.Errorf("expected denial for cross-workspace submission without ACL service, got success")
	}
	if resp.ErrorCode != errAuditPermDenied {
		t.Errorf("expected %s, got %s", errAuditPermDenied, resp.ErrorCode)
	}
}

func TestHandleSubmitAuditEvent_SameWorkspace_NoACLRequired(t *testing.T) {
	logger := audit.NewAuditLogger(nil, "test-gateway", &audit.Config{Enabled: false})
	defer logger.Close()
	s := newAuditSubmitTestServer(logger, nil) // acl == nil
	stream := &mockStream{}
	client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "foo")

	// Empty workspace = caller's home workspace, no ACL needed.
	s.handleSubmitAuditEvent(context.Background(), client, &pb.SubmitAuditEventRequest{
		EventType: audit.EventTypeMessage,
		Operation: "ping",
		Workspace: "",
	})

	resp := lastSubmitAuditResponse(stream)
	if resp == nil || !resp.Success {
		t.Fatalf("expected success for same-workspace submission, got %v", resp)
	}

	// Same explicit workspace = also OK.
	stream2 := &mockStream{}
	client2 := newAuditSubmitTestClient(stream2, models.PrincipalAgent, "foo")
	s.handleSubmitAuditEvent(context.Background(), client2, &pb.SubmitAuditEventRequest{
		EventType: audit.EventTypeMessage,
		Operation: "ping",
		Workspace: "foo",
	})
	resp2 := lastSubmitAuditResponse(stream2)
	if resp2 == nil || !resp2.Success {
		t.Fatalf("expected success for explicit same-workspace submission, got %v", resp2)
	}
}

// ---------------------------------------------------------------------------
// Rate limit
// ---------------------------------------------------------------------------

func TestHandleSubmitAuditEvent_RateLimited(t *testing.T) {
	logger := audit.NewAuditLogger(nil, "test-gateway", &audit.Config{Enabled: false})
	defer logger.Close()

	// Tight limit: 1 event/sec, burst 1 → second event in tight loop should reject.
	rl := quota.NewPrincipalRateLimiter(1.0)
	s := newAuditSubmitTestServer(logger, rl)

	// Fire 5 events in a tight loop; at least one should be rate-limited.
	// All iterations share the same authenticated identity (the rate limiter
	// keys on identity, so reusing one stays in the same bucket).
	var rateLimited int
	for i := 0; i < 5; i++ {
		streamN := &mockStream{}
		clientN := newAuditSubmitTestClient(streamN, models.PrincipalAgent, "foo")
		s.handleSubmitAuditEvent(context.Background(), clientN, &pb.SubmitAuditEventRequest{
			EventType: audit.EventTypeMessage,
			Operation: "burst",
		})
		resp := lastSubmitAuditResponse(streamN)
		if resp == nil {
			t.Fatalf("iter %d: missing response", i)
		}
		if !resp.Success && resp.ErrorCode == errAuditRateLimited {
			rateLimited++
		}
	}

	if rateLimited == 0 {
		t.Errorf("expected at least one event to be rate-limited, got 0 rejections out of 5")
	}
}

// ---------------------------------------------------------------------------
// Audit logger nil
// ---------------------------------------------------------------------------

func TestHandleSubmitAuditEvent_AuditDisabled(t *testing.T) {
	s := newAuditSubmitTestServer(nil, nil) // auditLogger is nil
	stream := &mockStream{}
	client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "foo")

	s.handleSubmitAuditEvent(context.Background(), client, &pb.SubmitAuditEventRequest{
		EventType: audit.EventTypeMessage,
		Operation: "x",
	})

	resp := lastSubmitAuditResponse(stream)
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Success {
		t.Errorf("expected failure when auditLogger is nil")
	}
	if resp.ErrorCode != errAuditDisabled {
		t.Errorf("expected %s, got %s", errAuditDisabled, resp.ErrorCode)
	}
}

// ---------------------------------------------------------------------------
// Identity stamping + metadata sanitization (real audit logger)
// ---------------------------------------------------------------------------

// TestHandleSubmitAuditEvent_StampsIdentityAndSanitizesMetadata verifies that:
//   - The persisted ActorID is the authenticated identity (the handler ignores
//     any client-supplied "actor_id" in metadata for the trusted field).
//   - Client-supplied metadata, including a bogus "actor_id" key, is preserved
//     under the metadata map (audit consumers can still see what the client
//     claimed, but it can't override the gateway-stamped truth).
//   - Credential-shaped keys ("password" here) are redacted regardless of
//     audit VerbosityLevel.
//   - Source is recorded as "principal".
func TestHandleSubmitAuditEvent_StampsIdentityAndSanitizesMetadata(t *testing.T) {
	logger, conn := newEnabledAuditLogger(t)
	s := newAuditSubmitTestServer(logger, nil)

	stream := &mockStream{}
	client := newAuditSubmitTestClient(stream, models.PrincipalAgent, "foo")
	authenticatedID := client.Identity.String()

	s.handleSubmitAuditEvent(context.Background(), client, &pb.SubmitAuditEventRequest{
		EventType: audit.EventTypeMessage,
		Operation: "completed_step",
		Workspace: "",
		Success:   true,
		Metadata: map[string]string{
			"actor_id": "FAKE-CLIENT-SUPPLIED",
			"password": "hunter2",
			"workflow": "abc",
		},
	})

	conn.waitForExecs(t, 1, 2*time.Second)

	conn.mu.Lock()
	defer conn.mu.Unlock()
	got := conn.execs[0]

	// Schema:
	// 1=timestamp 2=event_type 3=actor_type 4=actor_id 5=subject_type 6=subject_id
	// 7=root_subject_type 8=root_subject_id 9=authority_mode ...
	// 19=success 20=error_message 21=metadata 22=source
	if len(got.args) != 22 {
		t.Fatalf("expected 22 args (incl. source), got %d", len(got.args))
	}

	actorID, _ := got.args[3].(string)
	if actorID != authenticatedID {
		t.Errorf("ActorID = %q, want authenticated identity %q (client cannot override)", actorID, authenticatedID)
	}

	source, _ := got.args[21].(string)
	if source != audit.SourcePrincipal {
		t.Errorf("source = %q, want %q", source, audit.SourcePrincipal)
	}

	metaJSON, _ := got.args[20].([]byte)
	metaStr := string(metaJSON)
	// Client-supplied actor_id is preserved in metadata for forensics, but the
	// trusted ActorID column wins.
	if !strings.Contains(metaStr, "FAKE-CLIENT-SUPPLIED") {
		t.Errorf("expected client-supplied actor_id preserved in metadata, got: %s", metaStr)
	}
	// password value MUST be redacted regardless of audit verbosity.
	if !strings.Contains(metaStr, "[REDACTED]") {
		t.Errorf("expected password value redacted, got metadata: %s", metaStr)
	}
	if strings.Contains(metaStr, "hunter2") {
		t.Errorf("password value leaked into metadata: %s", metaStr)
	}
	// Non-credential keys pass through.
	if !strings.Contains(metaStr, "workflow") {
		t.Errorf("expected workflow key preserved, got metadata: %s", metaStr)
	}
}

// ---------------------------------------------------------------------------
// Pure-function tests (no handler harness needed)
// ---------------------------------------------------------------------------

func TestIsAllowedForeignAuditEventType(t *testing.T) {
	allowed := map[string]bool{
		audit.EventTypeMessage:    true,
		audit.EventTypeKV:         true,
		audit.EventTypeTask:       true,
		audit.EventTypeCustom:     true,
		audit.EventTypeAuth:       false,
		audit.EventTypeConnection: false,
		audit.EventTypeAdmin:      false,
		audit.EventTypeACL:        false,
		"unknown":                 false,
		"":                        false,
	}
	for et, want := range allowed {
		if got := isAllowedForeignAuditEventType(et); got != want {
			t.Errorf("isAllowedForeignAuditEventType(%q) = %v, want %v", et, got, want)
		}
	}
}

func TestConvertStringMapToInterface_NilSafe(t *testing.T) {
	out := convertStringMapToInterface(nil)
	if out == nil {
		t.Fatal("expected non-nil map for nil input")
	}
	if len(out) != 0 {
		t.Errorf("expected empty map, got %v", out)
	}
}

func TestConvertStringMapToInterface_Preserves(t *testing.T) {
	in := map[string]string{"a": "1", "b": "2"}
	out := convertStringMapToInterface(in)
	if out["a"] != "1" || out["b"] != "2" {
		t.Errorf("expected preserved values, got %v", out)
	}
}
