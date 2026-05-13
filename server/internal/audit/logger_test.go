package audit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// =============================================================================
// Fake storage backend: an in-process database/sql driver that captures every
// Exec call so tests can assert on what the batch writer would have persisted.
//
// We register one driver per test via t.Name() to avoid collisions across the
// suite — sql.Register panics on duplicate names.
// =============================================================================

type capturedExec struct {
	query string
	args  []driver.Value
}

type fakeStmt struct {
	c     *fakeConn
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.execs = append(s.c.execs, capturedExec{query: s.query, args: append([]driver.Value(nil), args...)})
	return driver.RowsAffected(1), nil
}
func (s *fakeStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("Query not supported by fake stmt")
}

type fakeTx struct {
	c *fakeConn
}

func (t *fakeTx) Commit() error {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.c.commits++
	return nil
}
func (t *fakeTx) Rollback() error {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.c.rollbacks++
	return nil
}

type fakeConn struct {
	mu        sync.Mutex
	execs     []capturedExec
	commits   int
	rollbacks int
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{c: c, query: query}, nil
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return &fakeTx{c: c}, nil }

// fakeDriver returns the same connection on every Open call so a single test
// can inspect aggregate captures across the *sql.DB connection pool.
type fakeDriver struct {
	conn *fakeConn
}

func (d *fakeDriver) Open(_ string) (driver.Conn, error) {
	return d.conn, nil
}

// newFakeDB registers a unique driver and returns the *sql.DB plus the
// connection backing it for assertions.
func newFakeDB(t *testing.T) (*sql.DB, *fakeConn) {
	t.Helper()
	conn := &fakeConn{}
	name := fmt.Sprintf("audit-fake-%s-%d", t.Name(), time.Now().UnixNano())
	sql.Register(name, &fakeDriver{conn: conn})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	// Single-connection pool so all Execs route through the same fakeConn.
	db.SetMaxOpenConns(1)
	return db, conn
}

func (c *fakeConn) execCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.execs)
}

// =============================================================================
// Helpers
// =============================================================================

func waitForExecs(t *testing.T, conn *fakeConn, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if conn.execCount() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d execs (got %d)", want, conn.execCount())
}

// =============================================================================
// NewAuditLogger + gating
// =============================================================================

func TestNewAuditLogger_DisabledReturnsNoOp(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	a := NewAuditLogger(nil, "gw-1", cfg)
	if a == nil {
		t.Fatal("NewAuditLogger returned nil")
	}
	// LogEvent should be a no-op (and not panic on nil base).
	a.LogEvent(context.Background(), &AuditEvent{EventType: EventTypeAuth})

	// LogEventSync should return nil (no-op) when disabled.
	if err := a.LogEventSync(context.Background(), &AuditEvent{EventType: EventTypeAuth}); err != nil {
		t.Errorf("LogEventSync on disabled logger = %v, want nil", err)
	}

	if err := a.Close(); err != nil {
		t.Errorf("Close on disabled logger = %v, want nil", err)
	}
}

func TestNewAuditLogger_NilConfigUsesDefaults(t *testing.T) {
	db, _ := newFakeDB(t)
	defer db.Close()
	a := NewAuditLogger(db, "gw-1", nil)
	if a == nil {
		t.Fatal("expected non-nil logger")
	}
	if cfg := a.GetConfig(); cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if !a.config.Enabled {
		t.Error("default config should be Enabled")
	}
	if err := a.Close(); err != nil {
		t.Errorf("Close() = %v", err)
	}
}

func TestLogEventSync_EventTypeNotEnabled(t *testing.T) {
	db, _ := newFakeDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	// Only allow connection events.
	cfg.EnabledEventTypes = []string{EventTypeConnection}

	a := NewAuditLogger(db, "gw-1", cfg)
	defer a.Close()

	err := a.LogEventSync(context.Background(), &AuditEvent{EventType: EventTypeMessage})
	if err != ErrEventNotEnabled {
		t.Errorf("LogEventSync(non-enabled type) = %v, want ErrEventNotEnabled", err)
	}
}

// =============================================================================
// Async batching: size trigger
// =============================================================================

func TestLogEvent_BatchSizeTriggersFlush(t *testing.T) {
	db, conn := newFakeDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.BatchSize = 5
	cfg.FlushPeriod = 10 * time.Minute // effectively disable time trigger
	cfg.ChannelBuffer = 50

	a := NewAuditLogger(db, "gw-async", cfg)
	defer a.Close()

	// Enqueue exactly BatchSize events of an enabled type.
	for i := 0; i < 5; i++ {
		a.LogEvent(context.Background(), &AuditEvent{
			EventType: EventTypeConnection,
			ActorType: "agent",
			ActorID:   fmt.Sprintf("ag-%d", i),
			Operation: OpConnectionEstablished,
			SessionID: uuid.New(),
		})
	}

	// All 5 events should be flushed in a single batch (so 5 Exec calls).
	waitForExecs(t, conn, 5, 2*time.Second)
}

// =============================================================================
// Async batching: time/flush-period trigger
// =============================================================================

func TestLogEvent_FlushPeriodTriggersFlush(t *testing.T) {
	db, conn := newFakeDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.BatchSize = 1000 // big enough that only time triggers
	cfg.FlushPeriod = 30 * time.Millisecond
	cfg.ChannelBuffer = 50

	a := NewAuditLogger(db, "gw-async-time", cfg)
	defer a.Close()

	// Enqueue fewer than BatchSize events.
	for i := 0; i < 3; i++ {
		a.LogEvent(context.Background(), &AuditEvent{
			EventType: EventTypeAuth,
			ActorType: "user",
			ActorID:   "alice",
			Operation: OpAuthTokenValidation,
		})
	}

	waitForExecs(t, conn, 3, 2*time.Second)
}

// =============================================================================
// Close flushes remaining events
// =============================================================================

func TestClose_FlushesRemaining(t *testing.T) {
	db, conn := newFakeDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.BatchSize = 1000
	cfg.FlushPeriod = 10 * time.Minute

	a := NewAuditLogger(db, "gw-close", cfg)

	a.LogEvent(context.Background(), &AuditEvent{
		EventType: EventTypeAdmin,
		ActorType: "user",
		ActorID:   "admin",
		Operation: OpAdminConfigChange,
	})
	a.LogEvent(context.Background(), &AuditEvent{
		EventType: EventTypeAdmin,
		ActorType: "user",
		ActorID:   "admin",
		Operation: OpAdminSessionDisconnect,
	})

	if err := a.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	if got := conn.execCount(); got != 2 {
		t.Errorf("after Close, expected 2 execs flushed, got %d", got)
	}
}

// =============================================================================
// prepareEvent / applyDirectAuthority — metadata + defaults
// =============================================================================

func TestPrepareEvent_SetsDefaults(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false // we only need the struct, not the writer
	a := NewAuditLogger(nil, "gw-default-id", cfg)

	e := &AuditEvent{EventType: EventTypeAuth}
	a.prepareEvent(e)

	if e.GatewayID != "gw-default-id" {
		t.Errorf("GatewayID = %q, want gw-default-id", e.GatewayID)
	}
	if e.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}
	if e.Metadata == nil {
		t.Error("Metadata should be initialized")
	}
}

func TestPrepareEvent_PreservesProvidedFields(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	a := NewAuditLogger(nil, "gw-default", cfg)

	provided := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	e := &AuditEvent{
		EventType: EventTypeAuth,
		GatewayID: "override-gw",
		Timestamp: provided,
		Metadata:  map[string]interface{}{"key": "value"},
	}
	a.prepareEvent(e)

	if e.GatewayID != "override-gw" {
		t.Errorf("GatewayID was overwritten: got %q", e.GatewayID)
	}
	if !e.Timestamp.Equal(provided) {
		t.Errorf("Timestamp was overwritten: got %v", e.Timestamp)
	}
	if e.Metadata["key"] != "value" {
		t.Errorf("Metadata was overwritten: got %v", e.Metadata)
	}
}

func TestApplyDirectAuthority_SetsDefaultMode(t *testing.T) {
	e := &AuditEvent{ActorType: "Agent", ActorID: "ag-1"}
	applyDirectAuthority(e)

	if e.AuthorityMode != AuthorityModeDirect {
		t.Errorf("AuthorityMode = %q, want %q", e.AuthorityMode, AuthorityModeDirect)
	}
	if e.SubjectType != "agent" || e.SubjectID != "ag-1" {
		t.Errorf("Subject not copied from actor: type=%q id=%q", e.SubjectType, e.SubjectID)
	}
	if e.RootSubjectType != "agent" || e.RootSubjectID != "ag-1" {
		t.Errorf("RootSubject not copied: type=%q id=%q", e.RootSubjectType, e.RootSubjectID)
	}
	// Actor type should be normalized to lowercase.
	if e.ActorType != "agent" {
		t.Errorf("ActorType not normalized: got %q", e.ActorType)
	}
}

func TestApplyDirectAuthority_PreservesOnBehalfOf(t *testing.T) {
	e := &AuditEvent{
		ActorType:     "service",
		ActorID:       "svc-1",
		SubjectType:   "User",
		SubjectID:     "alice",
		AuthorityMode: AuthorityModeOnBehalfOf,
	}
	applyDirectAuthority(e)

	if e.AuthorityMode != AuthorityModeOnBehalfOf {
		t.Errorf("AuthorityMode overwritten: got %q", e.AuthorityMode)
	}
	if e.SubjectType != "user" {
		t.Errorf("SubjectType not normalized: got %q", e.SubjectType)
	}
	if e.SubjectID != "alice" {
		t.Errorf("SubjectID overwritten: got %q", e.SubjectID)
	}
}

// =============================================================================
// NormalizePrincipalTypeCase: table-driven coverage
// =============================================================================

func TestNormalizePrincipalTypeCase(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"Agent", "agent"},
		{"agent", "agent"},
		{"Task", "task"},
		{"User", "user"},
		{"Service", "service"},
		{"Bridge", "bridge"},
		{"Orchestrator", "orchestrator"},
		{"WorkflowEngine", "workflow_engine"},
		{"workflowengine", "workflow_engine"},
		{"workflow_engine", "workflow_engine"},
		{"MetricsBridge", "metrics_bridge"},
		{"metricsbridge", "metrics_bridge"},
		{"metrics_bridge", "metrics_bridge"},
		// Unknown values are passed through unchanged so they remain debuggable.
		{"unexpected", "unexpected"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := NormalizePrincipalTypeCase(tc.in); got != tc.want {
			t.Errorf("NormalizePrincipalTypeCase(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// =============================================================================
// Metadata propagation through async path
// =============================================================================

func TestLogEvent_MetadataPropagation(t *testing.T) {
	db, conn := newFakeDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 10 * time.Minute

	a := NewAuditLogger(db, "gw-meta", cfg)
	defer a.Close()

	a.LogEvent(context.Background(), &AuditEvent{
		EventType: EventTypeKV,
		ActorType: "agent",
		ActorID:   "ag-1",
		Operation: OpKVPut,
		Metadata: map[string]interface{}{
			"key":  "config/setting",
			"size": 42,
		},
	})

	waitForExecs(t, conn, 1, 2*time.Second)

	conn.mu.Lock()
	got := conn.execs[0]
	conn.mu.Unlock()

	// Metadata is at position 21 in the INSERT (0-indexed = 20); source is
	// the new 22nd arg.
	if len(got.args) != 22 {
		t.Fatalf("expected 22 args, got %d", len(got.args))
	}
	metaArg, ok := got.args[20].([]byte)
	if !ok {
		t.Fatalf("metadata arg is %T, want []byte", got.args[20])
	}
	metaStr := string(metaArg)
	if !contains(metaStr, "config/setting") || !contains(metaStr, "42") {
		t.Errorf("metadata JSON did not include expected fields: %s", metaStr)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// =============================================================================
// Verbosity gating (helper-level, since LogEvent does not itself prune content)
//
// Other agents are extending sanitization; we only verify the gating helpers
// behave correctly. The actual content-stripping is their test surface.
// =============================================================================

func TestVerbosityGating_TableDriven(t *testing.T) {
	tests := []struct {
		verbosity      string
		wantContent    bool
		wantMetadata   bool
		validVerbosity bool
	}{
		{VerbosityLow, false, false, true},
		{VerbosityMedium, false, true, true},
		{VerbosityHigh, true, true, true},
		{"extreme", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.verbosity, func(t *testing.T) {
			if got := ShouldIncludeMessageContent(tt.verbosity); got != tt.wantContent {
				t.Errorf("ShouldIncludeMessageContent(%q) = %v, want %v", tt.verbosity, got, tt.wantContent)
			}
			if got := ShouldIncludeMessageMetadata(tt.verbosity); got != tt.wantMetadata {
				t.Errorf("ShouldIncludeMessageMetadata(%q) = %v, want %v", tt.verbosity, got, tt.wantMetadata)
			}
			err := ValidateVerbosityLevel(tt.verbosity)
			if tt.validVerbosity && err != nil {
				t.Errorf("ValidateVerbosityLevel(%q) = %v, want nil", tt.verbosity, err)
			}
			if !tt.validVerbosity && err == nil {
				t.Errorf("ValidateVerbosityLevel(%q) = nil, want error", tt.verbosity)
			}
		})
	}
}

// Compile-time assertion that fakeStmt satisfies the driver interface used by
// the standard sql package's connection pool.
var _ driver.Stmt = (*fakeStmt)(nil)
var _ driver.Tx = (*fakeTx)(nil)
var _ driver.Conn = (*fakeConn)(nil)
var _ driver.Driver = (*fakeDriver)(nil)

// io.Closer satisfaction sanity check for *sql.DB cleanup; not strictly
// required but documents the contract our fake satisfies.
var _ io.Closer = (*sql.DB)(nil)
