// Phase 5 Stage C: gateway-level integration tests for the full attribution path.
//
// These tests wire the complete pipeline in-process using sqlite backends:
//
//	Agent registers with ResourceSchema → PrefixIndex.Set
//	Caller calls CheckAccess for a resource_type under that prefix
//	ACL service consults PrefixIndex → populates owning_agent attribution
//	Audit writer flushes → comprehensive_audit_log row carries owning_agent in metadata
//
// Test layout:
//
//	TestRegistryACLAudit_FullAttributionPath  — happy path: PrefixIndex.Set → CheckAccess → audit row has attribution
//	TestRegistryACLAudit_NoAttributionWhenNoSchema — empty index → audit row has no owning_agent keys
//	TestPrefixConflictWire_HandleAgentOp — wire-level: ERR_PREFIX_CONFLICT: prefix returned by handleAgentOp REGISTER
package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/audit"
	legacyregistry "github.com/scitrera/aether/internal/registry"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	aclsqlite "github.com/scitrera/aether/internal/storage/acl/sqlite"
	registrysqlite "github.com/scitrera/aether/internal/storage/registry/sqlite"
	sqliteregistrymigrations "github.com/scitrera/aether/migrations/sqlite_registry"
	"github.com/scitrera/aether/pkg/models"

	_ "modernc.org/sqlite"
)

// ============================================================================
// Test harness for attribution path tests
// ============================================================================

// comprehensiveAuditLogDDL creates the comprehensive_audit_log table used as
// the audit sink. Mirrors migrations/sqlite_audit/001_audit_schema.sql; kept
// inline so the test harness is self-contained (no dependency on the audit
// migration runner in these in-package gateway tests).
const comprehensiveAuditLogDDL = `
CREATE TABLE IF NOT EXISTS comprehensive_audit_log (
    audit_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp                 TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type                TEXT NOT NULL,
    actor_type                TEXT NOT NULL,
    actor_id                  TEXT NOT NULL,
    subject_type              TEXT,
    subject_id                TEXT,
    root_subject_type         TEXT,
    root_subject_id           TEXT,
    authority_mode            TEXT NOT NULL DEFAULT 'direct',
    root_authority_grant_id   TEXT,
    authority_grant_id        TEXT,
    parent_authority_grant_id TEXT,
    resource_type             TEXT,
    resource_id               TEXT,
    operation                 TEXT NOT NULL,
    workspace                 TEXT,
    session_id                TEXT,
    gateway_id                TEXT,
    success                   INTEGER NOT NULL DEFAULT 1,
    error_message             TEXT,
    metadata                  TEXT,
    source                    TEXT NOT NULL DEFAULT 'gateway'
)`

// buildAttributionHarness constructs:
//   - A sqlite ACL state DB (acl_rules, acl_fallback_policies)
//   - The comprehensive_audit_log table in the same DB file
//   - An audit.AuditLogger with BatchSize=1 (immediate flush per event)
//   - A native sqlite ACL store wired to the shared audit writer
//   - An empty registry.PrefixIndex pre-injected into the ACL store
//
// Returns: aclStore, prefixIdx, auditDB, cleanup.
func buildAttributionHarness(t *testing.T) (aclstore.Store, *legacyregistry.PrefixIndex, *sql.DB, func()) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "acl.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)

	// Create the audit table before the ACL store opens (the ACL store's
	// migration creates its own tables; the audit table is separate).
	if _, err := db.Exec(comprehensiveAuditLogDDL); err != nil {
		_ = db.Close()
		t.Fatalf("create comprehensive_audit_log: %v", err)
	}

	// Shared audit writer: BatchSize=1 ensures a flush is triggered after each
	// enqueued event so tests can observe the row without a Close call.
	cfg := audit.DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 10 * time.Minute
	sharedAudit := audit.NewAuditLogger(db, "gw-stage-c", cfg)

	// Native sqlite ACL store (runs its own schema migrations).
	// Pass the same db handle for both state and audit reads; the audit writer
	// goes through the shared *audit.AuditLogger above.
	aclSt, err := aclsqlite.New(db, sharedAudit, db, "gw-stage-c")
	if err != nil {
		_ = sharedAudit.Close()
		_ = db.Close()
		t.Fatalf("aclsqlite.New: %v", err)
	}

	// Seed a broad fallback so CheckAccess returns ALLOW without explicit rules.
	ctx := context.Background()
	for _, cat := range []string{
		acl.RuleCategory("agent", "chat"),
		acl.RuleCategory("agent", "unregistered"),
	} {
		if err := aclSt.SetFallbackPolicy(ctx, cat, acl.AccessReadWrite, acl.SystemPrincipal); err != nil {
			_ = sharedAudit.Close()
			_ = db.Close()
			t.Fatalf("SetFallbackPolicy(%s): %v", cat, err)
		}
	}

	idx := legacyregistry.NewPrefixIndex()
	aclSt.SetPrefixIndex(idx)

	cleanup := func() {
		_ = sharedAudit.Close()
		_ = aclSt.Close()
		_ = db.Close()
	}
	return aclSt, idx, db, cleanup
}

// waitForAuditRow polls comprehensive_audit_log until a row appears with
// event_type='authorization' and the given resource_type, then returns its
// parsed metadata map. Returns nil if no row appears within timeout.
func waitForAuditRow(t *testing.T, db *sql.DB, resourceType string, timeout time.Duration) map[string]interface{} {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var metaJSON string
		err := db.QueryRowContext(context.Background(),
			`SELECT COALESCE(metadata,'{}') FROM comprehensive_audit_log
			  WHERE event_type='authorization' AND resource_type=?
			  ORDER BY audit_id DESC LIMIT 1`,
			resourceType,
		).Scan(&metaJSON)
		if err == nil {
			var m map[string]interface{}
			if jerr := json.Unmarshal([]byte(metaJSON), &m); jerr != nil {
				t.Fatalf("unmarshal audit metadata: %v", jerr)
			}
			return m
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// ============================================================================
// Attribution path tests
// ============================================================================

// TestRegistryACLAudit_FullAttributionPath is the primary Stage C integration
// test. It exercises the full chain:
//
//  1. Call PrefixIndex.Set("agent-x", [{prefix:"chat/",verbs:["read"]}])
//     (mirrors what GatewayStateProvider does after a successful Register).
//  2. Call aclStore.CheckAccess for resource_type="chat/messages/abc".
//  3. Wait for the audit writer to flush.
//  4. Assert that the audit row's metadata contains
//     owning_agent="agent-x" and owning_agent_prefix="chat/".
func TestRegistryACLAudit_FullAttributionPath(t *testing.T) {
	aclSt, idx, auditDB, cleanup := buildAttributionHarness(t)
	defer cleanup()

	ctx := context.Background()

	// Step 1: register the prefix in the index.
	idx.Set("agent-x", []legacyregistry.AgentResourceSchemaEntry{
		{ResourceTypePrefix: "chat/", PermissionVerbs: []string{"read"}},
	})

	// Step 2: CheckAccess for a resource under "chat/".
	caller := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "caller-agent",
		Specifier:      "inst-1",
	}
	if _, err := aclSt.CheckAccess(ctx, caller,
		"chat/messages/abc", "msg-1",
		"read", "ws1", uuid.New(), aclstore.AccessRead,
	); err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}

	// Step 3: wait for flush.
	meta := waitForAuditRow(t, auditDB, "chat/messages/abc", 3*time.Second)
	if meta == nil {
		t.Fatal("timed out waiting for audit row with resource_type='chat/messages/abc'")
	}

	// Step 4: assert attribution.
	if got := meta["owning_agent"]; got != "agent-x" {
		t.Errorf("metadata[owning_agent] = %v, want %q", got, "agent-x")
	}
	if got := meta["owning_agent_prefix"]; got != "chat/" {
		t.Errorf("metadata[owning_agent_prefix] = %v, want %q", got, "chat/")
	}
}

// TestRegistryACLAudit_NoAttributionWhenNoSchema verifies that an access check
// against a resource_type with no registered prefix produces an audit row that
// does NOT contain the owning_agent keys. This is the backward-compatibility
// guarantee: pre-Phase-5 agents produce clean audit rows.
func TestRegistryACLAudit_NoAttributionWhenNoSchema(t *testing.T) {
	aclSt, _, auditDB, cleanup := buildAttributionHarness(t)
	defer cleanup()

	ctx := context.Background()
	// PrefixIndex is empty — no agent declared any prefix.

	caller := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws2",
		Implementation: "legacy-agent",
		Specifier:      "inst-2",
	}
	if _, err := aclSt.CheckAccess(ctx, caller,
		"unregistered/resource", "r-1",
		"read", "ws2", uuid.New(), aclstore.AccessRead,
	); err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}

	meta := waitForAuditRow(t, auditDB, "unregistered/resource", 3*time.Second)
	if meta == nil {
		t.Fatal("timed out waiting for audit row with resource_type='unregistered/resource'")
	}

	if _, ok := meta["owning_agent"]; ok {
		t.Errorf("metadata should not contain owning_agent, got: %v", meta["owning_agent"])
	}
	if _, ok := meta["owning_agent_prefix"]; ok {
		t.Errorf("metadata should not contain owning_agent_prefix, got: %v", meta["owning_agent_prefix"])
	}
}

// ============================================================================
// Wire-level prefix conflict test
// ============================================================================

// registryOnlyProvider is a minimal admin.StateProvider backed by a real
// registrysqlite.Store. It implements only the agent-operation methods that
// handleAgentOp REGISTER/DELETE exercises. All other methods return "not
// implemented" errors so the provider stays small and focused.
type registryOnlyProvider struct {
	store *registrysqlite.Store
}

func (p *registryOnlyProvider) GetGatewayInfo(_ context.Context) (*admin.GatewayInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetHealthStatus(_ context.Context) (*admin.HealthStatus, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetConnections(_ context.Context, _ *admin.ConnectionFilter) ([]*admin.ConnectionInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetConnectionByID(_ context.Context, _ string) (*admin.ConnectionInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) DisconnectSession(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetTasks(_ context.Context, _ *admin.TaskFilter) ([]*admin.TaskInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetTaskByID(_ context.Context, _ string) (*admin.TaskInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) RetryTask(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) CancelTask(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetAgentRegistrations(ctx context.Context) ([]*admin.AgentRegistrationInfo, error) {
	regs, err := p.store.List(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]*admin.AgentRegistrationInfo, 0, len(regs))
	for _, r := range regs {
		out = append(out, registryRegToAdmin(r))
	}
	return out, nil
}
func (p *registryOnlyProvider) GetAgentByImplementation(ctx context.Context, impl string) (*admin.AgentRegistrationInfo, error) {
	r, err := p.store.Get(ctx, impl)
	if err != nil {
		return nil, err
	}
	return registryRegToAdmin(r), nil
}
func (p *registryOnlyProvider) RegisterAgent(ctx context.Context, a *admin.AgentRegistrationInfo) error {
	return p.store.Register(ctx, adminToRegistryReg(a))
}
func (p *registryOnlyProvider) UpdateAgent(ctx context.Context, _ string, a *admin.AgentRegistrationInfo) error {
	return p.store.Register(ctx, adminToRegistryReg(a))
}
func (p *registryOnlyProvider) DeleteAgent(ctx context.Context, impl string) error {
	return p.store.Delete(ctx, impl)
}
func (p *registryOnlyProvider) GetOrchestratorProfiles(_ context.Context) ([]*admin.OrchestratorProfileInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) LaunchAgent(_ context.Context, _ *admin.LaunchAgentRequest) (*admin.LaunchAgentResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetWorkspaces(_ context.Context) ([]*admin.WorkspaceInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetWorkspaceByID(_ context.Context, _ string) (*admin.WorkspaceInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) CreateWorkspace(_ context.Context, _ *admin.WorkspaceInfo) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) UpdateWorkspace(_ context.Context, _ string, _ *admin.WorkspaceInfo) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) DeleteWorkspace(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetKVKeys(_ context.Context, _, _ string) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetKVValue(_ context.Context, _, _ string) (*admin.KVEntry, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) SetKVValue(_ context.Context, _, _, _ string, _ int64) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) DeleteKVKey(_ context.Context, _, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) SubscribeEvents(_ context.Context) (<-chan *admin.Event, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) SendMessage(_ context.Context, _ *admin.SendMessageRequest) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) SubscribeToTopic(_ context.Context, _ string, _ func(*admin.MonitoredMessage)) (func(), error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) ListACLRules(_ context.Context, _ *admin.ACLRuleFilter) ([]*admin.ACLRuleInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetACLRule(_ context.Context, _, _, _, _ string) (*admin.ACLRuleInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GrantACLAccess(_ context.Context, _ *admin.GrantACLAccessRequest) (*admin.ACLRuleInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) RevokeACLAccess(_ context.Context, _, _, _, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) ListACLAuthorityGrants(_ context.Context, _ *admin.ACLAuthorityGrantFilter) ([]*admin.ACLAuthorityGrantInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetACLAuthorityGrant(_ context.Context, _ string) (*admin.ACLAuthorityGrantInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) CreateACLAuthorityGrant(_ context.Context, _ *admin.CreateACLAuthorityGrantRequest) (*admin.ACLAuthorityGrantInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) RenewACLAuthorityGrant(_ context.Context, _ *admin.RenewACLAuthorityGrantRequest) (*admin.ACLAuthorityGrantInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) RevokeACLAuthorityGrant(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) SetACLFallbackPolicy(_ context.Context, _ *admin.SetFallbackPolicyRequest) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetACLFallbackPolicy(_ context.Context, _ string) (*admin.ACLFallbackPolicyInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) QueryACLAuditLog(_ context.Context, _ *admin.ACLAuditLogFilter) ([]*admin.ACLAuditLogEntryInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) CleanupExpiredACLRules(_ context.Context) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) CleanupOldACLAuditLogs(_ context.Context, _ int) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetMessageFlow(_ context.Context, _ string) (*admin.MessageFlowInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) ListTokens(_ context.Context, _, _ int, _ bool) ([]*admin.TokenInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetToken(_ context.Context, _ string) (*admin.TokenInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) CreateToken(_ context.Context, _ *admin.CreateTokenRequest) (*admin.CreateTokenResult, error) {
	return nil, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) DeleteToken(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) RevokeToken(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) SetWorkspaceRateLimit(_ string, _ float64) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) GetWorkspaceRateLimit(_ string) (float64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) RemoveWorkspaceRateLimit(_ string) error {
	return fmt.Errorf("not implemented")
}
func (p *registryOnlyProvider) ListWorkspaceRateLimits() (map[string]float64, error) {
	return nil, fmt.Errorf("not implemented")
}

// Compile-time check.
var _ admin.StateProvider = (*registryOnlyProvider)(nil)

// adminToRegistryReg converts admin.AgentRegistrationInfo → legacyregistry.AgentRegistration
// (the concrete type consumed by registrysqlite.Store).
func adminToRegistryReg(a *admin.AgentRegistrationInfo) *legacyregistry.AgentRegistration {
	lp := make(map[string]interface{})
	for k, v := range a.LaunchParams {
		lp[k] = v
	}
	var schema []legacyregistry.AgentResourceSchemaEntry
	for _, e := range a.ResourceSchema {
		schema = append(schema, legacyregistry.AgentResourceSchemaEntry{
			ResourceTypePrefix: e.ResourceTypePrefix,
			PermissionVerbs:    e.PermissionVerbs,
			ResourceIDSchema:   e.ResourceIDSchema,
		})
	}
	return &legacyregistry.AgentRegistration{
		Implementation: a.Implementation,
		LaunchParams:   lp,
		Description:    a.Description,
		ResourceSchema: schema,
		Capabilities:   a.Capabilities,
		Extensions:     a.Extensions,
	}
}

// registryRegToAdmin converts legacyregistry.AgentRegistration → admin.AgentRegistrationInfo.
func registryRegToAdmin(r *legacyregistry.AgentRegistration) *admin.AgentRegistrationInfo {
	lp := make(map[string]interface{})
	for k, v := range r.LaunchParams {
		lp[k] = v
	}
	out := &admin.AgentRegistrationInfo{
		Implementation: r.Implementation,
		Description:    r.Description,
		RegisteredAt:   r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
		LaunchParams:   lp,
		Capabilities:   r.Capabilities,
		Extensions:     r.Extensions,
	}
	for _, e := range r.ResourceSchema {
		out.ResourceSchema = append(out.ResourceSchema, admin.AgentResourceSchemaEntry{
			ResourceTypePrefix: e.ResourceTypePrefix,
			PermissionVerbs:    e.PermissionVerbs,
			ResourceIDSchema:   e.ResourceIDSchema,
		})
	}
	return out
}

// buildConflictTestServer spins up a GatewayServer whose adminProvider is
// backed by a real sqlite registry store, so handleAgentOp REGISTER exercises
// the actual uniqueness check including the ERR_PREFIX_CONFLICT: wire encoding.
func buildConflictTestServer(t *testing.T) (*GatewayServer, func()) {
	t.Helper()
	dir := t.TempDir()

	// Registry sqlite DB.
	regDBPath := filepath.Join(dir, "registry.db")
	regDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", regDBPath)
	regDB, err := sql.Open("sqlite", regDSN)
	if err != nil {
		t.Fatalf("sql.Open registry: %v", err)
	}
	regDB.SetMaxOpenConns(1)

	// Badger for orchestrator profile state (required by registrysqlite.New).
	badgerDir := filepath.Join(dir, "badger")
	badgerDB, err := badger.Open(badger.DefaultOptions(badgerDir).WithLogger(nil))
	if err != nil {
		_ = regDB.Close()
		t.Fatalf("badger.Open: %v", err)
	}
	profileState := legacyregistry.NewBadgerProfileStateStore(badgerDB)

	regStore, err := registrysqlite.New(regDB, profileState, sqliteregistrymigrations.MigrationFS)
	if err != nil {
		_ = badgerDB.Close()
		_ = regDB.Close()
		t.Fatalf("registrysqlite.New: %v", err)
	}

	provider := &registryOnlyProvider{store: regStore}

	s := &GatewayServer{
		sessions:      newMockSessionManager(),
		router:        newInProcessRouter(),
		kv:            newMockKVReadWriter(),
		checkpoints:   newMockCheckpointManager(),
		gatewayID:     "test-conflict-gw",
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
		adminProvider: provider,
	}

	cleanup := func() {
		_ = badgerDB.Close()
		_ = regDB.Close()
	}
	return s, cleanup
}

// findAgentResponse scans a mockStream's sent buffer for the first AgentResponse.
func findAgentResponse(stream *mockStream) *pb.AgentResponse {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for _, msg := range stream.sent {
		if r := msg.GetAgent(); r != nil {
			return r
		}
	}
	return nil
}

// TestPrefixConflictWire_HandleAgentOp exercises the full handleAgentOp REGISTER
// path end-to-end with a real sqlite registry. It confirms that a second
// registration claiming an already-owned prefix returns an AgentResponse whose
// Error string starts with "ERR_PREFIX_CONFLICT:".
//
// This test validates the wire-level error-code encoding from Stage B survives
// the proto roundtrip through handleAgentOp → formatAgentError → AgentResponse.
func TestPrefixConflictWire_HandleAgentOp(t *testing.T) {
	s, cleanup := buildConflictTestServer(t)
	defer cleanup()

	ctx := context.Background()

	stream := &mockStream{}
	client, cancel := newSubscriptionTestClient(stream, callerIdentity("admin", "test-admin"))
	defer cancel()

	// Register agent-a claiming prefix "chat/".
	opA := &pb.AgentOperation{
		Op: pb.AgentOperation_REGISTER,
		Agent: &pb.AgentRegistrationInfo{
			Implementation: "conflict-agent-a",
			LaunchParams:   map[string]string{"profile": "k8s"},
			ResourceSchema: []*pb.AgentResourceSchemaEntry{
				{ResourceTypePrefix: "chat/", PermissionVerbs: []string{"read"}},
			},
		},
	}
	s.handleAgentOp(ctx, client, opA)

	respA := findAgentResponse(stream)
	if respA == nil {
		t.Fatal("expected AgentResponse after first REGISTER")
	}
	if !respA.Success {
		t.Fatalf("first REGISTER should succeed, got error: %q", respA.Error)
	}

	// Reset sent buffer to isolate the second response.
	stream.mu.Lock()
	stream.sent = nil
	stream.mu.Unlock()

	// Register agent-b claiming the same "chat/" prefix — should conflict.
	opB := &pb.AgentOperation{
		Op: pb.AgentOperation_REGISTER,
		Agent: &pb.AgentRegistrationInfo{
			Implementation: "conflict-agent-b",
			LaunchParams:   map[string]string{"profile": "k8s"},
			ResourceSchema: []*pb.AgentResourceSchemaEntry{
				{ResourceTypePrefix: "chat/", PermissionVerbs: []string{"read"}},
			},
		},
	}
	s.handleAgentOp(ctx, client, opB)

	respB := findAgentResponse(stream)
	if respB == nil {
		t.Fatal("expected AgentResponse after conflicting REGISTER")
	}
	if respB.Success {
		t.Fatal("second REGISTER should fail with ERR_PREFIX_CONFLICT:, got Success=true")
	}

	prefix := ErrCodePrefixConflict + ":"
	if len(respB.Error) < len(prefix) || respB.Error[:len(prefix)] != prefix {
		t.Errorf("AgentResponse.Error = %q; want prefix %q", respB.Error, prefix)
	}
}
