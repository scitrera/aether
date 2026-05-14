package acl

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/pkg/models"
)

// --- buildACLMetadata ---

func TestBuildACLMetadata_ContainsRequiredFields(t *testing.T) {
	entry := &AuditLogEntry{
		Decision:        DecisionAllow,
		AccessLevel:     AccessReadWrite,
		FallbackApplied: false,
		Metadata:        map[string]interface{}{},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if m["decision"] != DecisionAllow {
		t.Errorf("metadata[decision] = %v, want %s", m["decision"], DecisionAllow)
	}
	if int(m["access_level"].(float64)) != AccessReadWrite {
		t.Errorf("metadata[access_level] = %v, want %d", m["access_level"], AccessReadWrite)
	}
	if m["fallback_applied"] != false {
		t.Errorf("metadata[fallback_applied] = %v, want false", m["fallback_applied"])
	}
}

func TestBuildACLMetadata_IncludesRuleIDWhenPresent(t *testing.T) {
	ruleID := "rule-xyz-789"
	entry := &AuditLogEntry{
		Decision:    DecisionAllow,
		AccessLevel: AccessRead,
		RuleID:      &ruleID,
		Metadata:    map[string]interface{}{},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if m["rule_id"] != ruleID {
		t.Errorf("metadata[rule_id] = %v, want %s", m["rule_id"], ruleID)
	}
}

func TestBuildACLMetadata_OmitsRuleIDWhenAbsent(t *testing.T) {
	entry := &AuditLogEntry{
		Decision:    DecisionDeny,
		AccessLevel: AccessNone,
		RuleID:      nil,
		Metadata:    map[string]interface{}{},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if _, exists := m["rule_id"]; exists {
		t.Error("rule_id should be absent when RuleID is nil")
	}
}

func TestBuildACLMetadata_PreservesExtraMetadataFields(t *testing.T) {
	entry := &AuditLogEntry{
		Decision:    DecisionAllow,
		AccessLevel: AccessRead,
		Metadata: map[string]interface{}{
			"custom_key": "custom_value",
			"count":      42,
		},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if m["custom_key"] != "custom_value" {
		t.Errorf("metadata[custom_key] = %v, want custom_value", m["custom_key"])
	}
}

// --- AuditLogger.buildEntry ---

func TestBuildEntry_PopulatesAllFields(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-001"}

	decision := &ACLDecision{
		Decision:             DecisionAllow,
		EffectiveAccessLevel: AccessReadWrite,
		FallbackApplied:      true,
	}
	principal := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	sessionID := uuid.New()
	now := time.Now()

	entry := logger.buildEntry(decision, principal, ResourceTypeWorkspace, "prod", "connect", "prod", sessionID)

	if entry.Decision != DecisionAllow {
		t.Errorf("Decision = %q, want ALLOW", entry.Decision)
	}
	if entry.AccessLevel != AccessReadWrite {
		t.Errorf("AccessLevel = %d, want %d", entry.AccessLevel, AccessReadWrite)
	}
	if entry.PrincipalType != PrincipalTypeUser {
		t.Errorf("PrincipalType = %q, want %s", entry.PrincipalType, PrincipalTypeUser)
	}
	if entry.PrincipalID != "alice" {
		t.Errorf("PrincipalID = %q, want alice", entry.PrincipalID)
	}
	if entry.SubjectType != PrincipalTypeUser {
		t.Errorf("SubjectType = %q, want %s", entry.SubjectType, PrincipalTypeUser)
	}
	if entry.SubjectID != "alice" {
		t.Errorf("SubjectID = %q, want alice", entry.SubjectID)
	}
	if entry.RootSubjectType != PrincipalTypeUser {
		t.Errorf("RootSubjectType = %q, want %s", entry.RootSubjectType, PrincipalTypeUser)
	}
	if entry.RootSubjectID != "alice" {
		t.Errorf("RootSubjectID = %q, want alice", entry.RootSubjectID)
	}
	if entry.AuthorityMode != "direct" {
		t.Errorf("AuthorityMode = %q, want direct", entry.AuthorityMode)
	}
	if entry.ResourceType != ResourceTypeWorkspace {
		t.Errorf("ResourceType = %q, want %s", entry.ResourceType, ResourceTypeWorkspace)
	}
	if entry.ResourceID != "prod" {
		t.Errorf("ResourceID = %q, want prod", entry.ResourceID)
	}
	if entry.Operation != "connect" {
		t.Errorf("Operation = %q, want connect", entry.Operation)
	}
	if entry.Workspace != "prod" {
		t.Errorf("Workspace = %q, want prod", entry.Workspace)
	}
	if entry.GatewayID != "gw-001" {
		t.Errorf("GatewayID = %q, want gw-001", entry.GatewayID)
	}
	if entry.SessionID != sessionID {
		t.Errorf("SessionID = %v, want %v", entry.SessionID, sessionID)
	}
	if !entry.FallbackApplied {
		t.Error("expected FallbackApplied=true")
	}
	if entry.Timestamp.Before(now.Add(-time.Second)) {
		t.Error("Timestamp should be approximately now")
	}
}

func TestBuildEntry_SetsRuleIDFromDecision(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	ruleID := "rule-123"
	decision := &ACLDecision{
		Decision:    DecisionAllow,
		RuleApplied: &ACLRule{RuleID: ruleID},
	}
	principal := models.Identity{Type: models.PrincipalAgent, ID: "agent-x"}

	entry := logger.buildEntry(decision, principal, ResourceTypeAgent, "agent-x", "connect", "ws", uuid.New())

	if entry.RuleID == nil {
		t.Fatal("expected RuleID to be set")
	}
	if *entry.RuleID != ruleID {
		t.Errorf("*RuleID = %q, want %s", *entry.RuleID, ruleID)
	}
}

func TestBuildEntry_RuleIDNilWhenNoRuleApplied(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	decision := &ACLDecision{Decision: DecisionDeny, RuleApplied: nil}
	principal := models.Identity{Type: models.PrincipalUser, ID: "user-z"}

	entry := logger.buildEntry(decision, principal, ResourceTypeWorkspace, "ws", "op", "ws", uuid.New())

	if entry.RuleID != nil {
		t.Error("expected RuleID to be nil when no rule was applied")
	}
}

func TestBuildEntry_MetadataMapInitialised(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	decision := &ACLDecision{Decision: DecisionDeny}
	principal := models.Identity{Type: models.PrincipalUser, ID: "u"}

	entry := logger.buildEntry(decision, principal, "workspace", "w", "op", "w", uuid.New())

	if entry.Metadata == nil {
		t.Error("expected Metadata map to be initialized, got nil")
	}
}

func TestBuildEntry_UsesCanonicalPrincipalID(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	decision := &ACLDecision{Decision: DecisionAllow}
	principal := models.Identity{
		Type:           models.PrincipalService,
		Implementation: "frontend-api",
		Specifier:      "pod-1",
	}

	entry := logger.buildEntry(decision, principal, ResourceTypeWorkspace, "ws", "read", "ws", uuid.New())

	if entry.PrincipalID != "sv::frontend-api::pod-1" {
		t.Errorf("PrincipalID = %q, want %q", entry.PrincipalID, "sv::frontend-api::pod-1")
	}
	if entry.PrincipalType != PrincipalTypeService {
		t.Errorf("PrincipalType = %q, want %s", entry.PrincipalType, PrincipalTypeService)
	}
}

// --- entryToEvent (translation to audit.AuditEvent) ---

func TestEntryToEvent_AllowMapsToSuccess(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-1"}
	ruleID := "rule-77"
	entry := &AuditLogEntry{
		Timestamp:       time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Decision:        DecisionAllow,
		AccessLevel:     AccessReadWrite,
		PrincipalType:   PrincipalTypeUser,
		PrincipalID:     "alice",
		SubjectType:     PrincipalTypeUser,
		SubjectID:       "alice",
		RootSubjectType: PrincipalTypeUser,
		RootSubjectID:   "alice",
		AuthorityMode:   audit.AuthorityModeDirect,
		ResourceType:    ResourceTypeWorkspace,
		ResourceID:      "prod",
		Operation:       "connect",
		Workspace:       "prod",
		FallbackApplied: false,
		GatewayID:       "gw-1",
		RuleID:          &ruleID,
		Metadata:        map[string]interface{}{"extra_key": "extra_val"},
	}

	ev := logger.entryToEvent(entry)

	if ev.EventType != "authorization" {
		t.Errorf("EventType = %q, want authorization", ev.EventType)
	}
	if !ev.Success {
		t.Error("ALLOW decision should map to Success=true")
	}
	if ev.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty for ALLOW", ev.ErrorMessage)
	}
	if ev.ActorType != PrincipalTypeUser || ev.ActorID != "alice" {
		t.Errorf("actor = %q/%q, want user/alice", ev.ActorType, ev.ActorID)
	}
	if ev.Source != audit.SourceGateway {
		t.Errorf("Source = %q, want %q", ev.Source, audit.SourceGateway)
	}
	if ev.Metadata["decision"] != DecisionAllow {
		t.Errorf("metadata[decision] = %v, want %s", ev.Metadata["decision"], DecisionAllow)
	}
	// access_level is preserved as float64 because the metadata round-trips
	// through JSON in entryToEvent (matches the prior batched-writer shape).
	if v, ok := ev.Metadata["access_level"].(float64); !ok || int(v) != AccessReadWrite {
		t.Errorf("metadata[access_level] = %v (%T), want %d", ev.Metadata["access_level"], ev.Metadata["access_level"], AccessReadWrite)
	}
	if ev.Metadata["rule_id"] != ruleID {
		t.Errorf("metadata[rule_id] = %v, want %s", ev.Metadata["rule_id"], ruleID)
	}
	if ev.Metadata["extra_key"] != "extra_val" {
		t.Errorf("caller-supplied metadata extras should be preserved, got %v", ev.Metadata["extra_key"])
	}
}

func TestEntryToEvent_DenyMapsToFailure(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-1"}
	entry := &AuditLogEntry{
		Decision:      DecisionDeny,
		AccessLevel:   AccessNone,
		PrincipalType: PrincipalTypeUser,
		PrincipalID:   "mallory",
		Metadata:      map[string]interface{}{},
	}

	ev := logger.entryToEvent(entry)

	if ev.Success {
		t.Error("DENY decision should map to Success=false")
	}
	if ev.ErrorMessage != "access denied" {
		t.Errorf("ErrorMessage = %q, want %q", ev.ErrorMessage, "access denied")
	}
}

// --- LogDecision → shared writer integration ---
//
// Uses a real (in-memory fake) audit.AuditLogger plus the shared
// BaseLogger queue so we exercise the full producer→consumer path the
// production code uses.

type aclFakeStmt struct {
	c     *aclFakeConn
	query string
}

func (s *aclFakeStmt) Close() error  { return nil }
func (s *aclFakeStmt) NumInput() int { return -1 }
func (s *aclFakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.execs = append(s.c.execs, append([]driver.Value(nil), args...))
	return driver.RowsAffected(1), nil
}
func (s *aclFakeStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

type aclFakeTx struct{ c *aclFakeConn }

func (t *aclFakeTx) Commit() error   { return nil }
func (t *aclFakeTx) Rollback() error { return nil }

type aclFakeConn struct {
	mu    sync.Mutex
	execs [][]driver.Value
}

func (c *aclFakeConn) Prepare(query string) (driver.Stmt, error) {
	return &aclFakeStmt{c: c, query: query}, nil
}
func (c *aclFakeConn) Close() error              { return nil }
func (c *aclFakeConn) Begin() (driver.Tx, error) { return &aclFakeTx{c: c}, nil }

type aclFakeDriver struct{ conn *aclFakeConn }

func (d *aclFakeDriver) Open(_ string) (driver.Conn, error) { return d.conn, nil }

func newACLFakeDB(t *testing.T) (*sql.DB, *aclFakeConn) {
	t.Helper()
	conn := &aclFakeConn{}
	name := fmt.Sprintf("acl-fake-%s-%d", t.Name(), time.Now().UnixNano())
	sql.Register(name, &aclFakeDriver{conn: conn})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db, conn
}

func (c *aclFakeConn) execCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.execs)
}

func TestLogDecision_RoutesToSharedWriter(t *testing.T) {
	db, conn := newACLFakeDB(t)
	defer db.Close()

	cfg := audit.DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 10 * time.Minute // size-trigger only
	shared := audit.NewAuditLogger(db, "gw-shared", cfg)
	defer shared.Close()

	logger := NewAuditLogger(shared, nil /* read db not needed */, "gw-shared")

	decision := &ACLDecision{
		Decision:             DecisionAllow,
		EffectiveAccessLevel: AccessReadWrite,
	}
	principal := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	logger.LogDecision(context.Background(), decision, principal, ResourceTypeWorkspace, "prod", "connect", "prod", uuid.New())

	// Wait up to 2s for the shared writer to flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn.execCount() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := conn.execCount(); got != 1 {
		t.Fatalf("expected 1 INSERT through shared writer, got %d", got)
	}

	conn.mu.Lock()
	args := conn.execs[0]
	conn.mu.Unlock()
	if len(args) != 22 {
		t.Fatalf("expected 22 INSERT args, got %d", len(args))
	}
	// event_type is arg index 1 in the comprehensive_audit_log INSERT.
	if et, ok := args[1].(string); !ok || et != "authorization" {
		t.Errorf("event_type arg = %v (%T), want \"authorization\"", args[1], args[1])
	}
	// success is arg index 18.
	if ok, _ := args[18].(bool); !ok {
		t.Errorf("success arg = %v, want true", args[18])
	}
	// metadata is arg index 20 (JSON bytes).
	metaJSON, ok := args[20].([]byte)
	if !ok {
		t.Fatalf("metadata arg type = %T, want []byte", args[20])
	}
	var m map[string]interface{}
	if err := json.Unmarshal(metaJSON, &m); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	if m["decision"] != DecisionAllow {
		t.Errorf("metadata[decision] = %v, want %s", m["decision"], DecisionAllow)
	}
}

func TestACLAuditLogger_NilSharedWriterIsNoop(t *testing.T) {
	logger := NewAuditLogger(nil, nil, "gw-nil")
	// Should not panic.
	logger.LogDecision(context.Background(),
		&ACLDecision{Decision: DecisionAllow},
		models.Identity{Type: models.PrincipalUser, ID: "alice"},
		ResourceTypeWorkspace, "prod", "connect", "prod", uuid.New(),
	)
	if err := logger.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

// Compile-time interface satisfaction
var _ driver.Stmt = (*aclFakeStmt)(nil)
var _ driver.Tx = (*aclFakeTx)(nil)
var _ driver.Conn = (*aclFakeConn)(nil)
var _ driver.Driver = (*aclFakeDriver)(nil)
var _ io.Closer = (*sql.DB)(nil)
