package acl

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Use dev infrastructure with centralized migrations
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, func() {}
	}

	return testDB.DB, cleanup
}

func TestACLService_GrantAndCheckAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant READ access to alice for production workspace
	_, err := service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "test grant", nil)
	if err != nil {
		t.Fatalf("Failed to grant access: %v", err)
	}

	// Check access
	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: "production",
	}

	decision, err := service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("Failed to check access: %v", err)
	}

	if !decision.Allowed {
		t.Error("Expected access to be allowed")
	}

	if decision.EffectiveAccessLevel != AccessRead {
		t.Errorf("Expected access level READ, got %d", decision.EffectiveAccessLevel)
	}
}

func TestACLService_DenyAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// No grant for bob - should be denied by fallback
	bob := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "bob",
		Workspace: "production",
	}

	// Set fallback policy to NONE (must use lowercase to match RuleCategory output)
	err := service.SetFallbackPolicy(ctx, "user_workspace", AccessNone, "_system")
	if err != nil {
		t.Fatalf("Failed to set fallback policy: %v", err)
	}

	decision, err := service.CheckAccess(ctx, bob, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("Failed to check access: %v", err)
	}

	if decision.Allowed {
		t.Error("Expected access to be denied")
	}

	if !decision.FallbackApplied {
		t.Error("Expected fallback policy to be applied")
	}
}

func TestACLService_RevokeAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant access
	_, err := service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "test grant", nil)
	if err != nil {
		t.Fatalf("Failed to grant access: %v", err)
	}

	// Revoke it
	err = service.RevokeAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production")
	if err != nil {
		t.Fatalf("Failed to revoke access: %v", err)
	}

	// Should be gone
	_, err = service.GetRule(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production")
	if err != ErrRuleNotFound {
		t.Errorf("Expected ErrRuleNotFound, got %v", err)
	}
}

func TestACLService_ExpiringRules(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant access that expires in 1 second
	expiresAt := time.Now().Add(1 * time.Second)
	_, err := service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "temporary grant", &expiresAt)
	if err != nil {
		t.Fatalf("Failed to grant access: %v", err)
	}

	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: "production",
	}

	// Should be allowed immediately
	decision, err := service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("Failed to check access: %v", err)
	}

	if !decision.Allowed {
		t.Error("Expected access to be allowed before expiration")
	}

	// Wait for expiration
	time.Sleep(1500 * time.Millisecond)

	// Set fallback to NONE so we can verify the rule is expired (must use lowercase)
	service.SetFallbackPolicy(ctx, "user_workspace", AccessNone, "_system")

	// Should be denied now (expired rule falls back to policy)
	decision, err = service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("Failed to check access: %v", err)
	}

	if decision.Allowed {
		t.Error("Expected access to be denied after expiration")
	}
}

func TestACLService_WildcardRules(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant READ access to all authenticated users for _global workspace
	_, err := service.GrantAccess(ctx, PrincipalTypeWildcard, WildcardAnyAuthenticatedUser, ResourceTypeWorkspace, GlobalWorkspace, AccessRead, "_system", "default global access", nil)
	if err != nil {
		t.Fatalf("Failed to grant wildcard access: %v", err)
	}

	// Any user should have access
	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: GlobalWorkspace,
	}

	decision, err := service.CheckAccess(ctx, alice, ResourceTypeWorkspace, GlobalWorkspace, "connect", GlobalWorkspace, uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("Failed to check access: %v", err)
	}

	if !decision.Allowed {
		t.Error("Expected wildcard rule to allow access")
	}
}

func TestACLService_InMemoryConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant access
	_, err := service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "test grant", nil)
	if err != nil {
		t.Fatalf("Failed to grant access: %v", err)
	}

	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: "production",
	}

	// Multiple checks should return consistent results (in-memory evaluation)
	for i := 0; i < 5; i++ {
		decision, err := service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
		if err != nil {
			t.Fatalf("Failed to check access on iteration %d: %v", i, err)
		}
		if !decision.Allowed {
			t.Errorf("Expected access to be allowed on iteration %d", i)
		}
		if decision.EffectiveAccessLevel != AccessRead {
			t.Errorf("Expected access level READ on iteration %d, got %d", i, decision.EffectiveAccessLevel)
		}
	}

	// Grant higher access and verify the in-memory model updates immediately
	_, err = service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessAdmin, "_system", "upgrade", nil)
	if err != nil {
		t.Fatalf("Failed to upgrade access: %v", err)
	}

	decision, err := service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessAdmin)
	if err != nil {
		t.Fatalf("Failed to check upgraded access: %v", err)
	}
	if !decision.Allowed {
		t.Error("Expected admin access to be allowed after upgrade")
	}
	if decision.EffectiveAccessLevel != AccessAdmin {
		t.Errorf("Expected access level ADMIN, got %d", decision.EffectiveAccessLevel)
	}
}

func TestACLService_AuditLog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	// Note: We close this service explicitly mid-test to flush audit logs

	ctx := context.Background()

	// Grant access
	_, err := service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "test grant", nil)
	if err != nil {
		t.Fatalf("Failed to grant access: %v", err)
	}

	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: "production",
	}

	// Check access (triggers audit log)
	_, err = service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("Failed to check access: %v", err)
	}

	// Close the service to flush the audit log buffer before querying
	service.Close()

	// Create a fresh service to query the audit log
	service = NewService(db, "test-gateway")
	defer service.Close()

	// Query audit log
	filter := AuditLogFilter{
		PrincipalID: "alice",
		Limit:       10,
	}

	entries, err := service.QueryAuditLog(ctx, filter)
	if err != nil {
		t.Fatalf("Failed to query audit log: %v", err)
	}

	if len(entries) == 0 {
		t.Error("Expected audit log entries, got none")
	}

	entry := entries[0]
	if entry.PrincipalID != "alice" {
		t.Errorf("Expected principal alice, got %s", entry.PrincipalID)
	}

	if entry.Decision != DecisionAllow {
		t.Errorf("Expected decision ALLOW, got %s", entry.Decision)
	}
}

func TestACLService_ListRules(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant multiple rules
	service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "test grant 1", nil)
	service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "staging", AccessReadWrite, "_system", "test grant 2", nil)
	service.GrantAccess(ctx, PrincipalTypeUser, "bob", ResourceTypeWorkspace, "production", AccessRead, "_system", "test grant 3", nil)

	// List all rules for alice
	filter := RuleFilter{
		PrincipalID: "alice",
	}

	rules, err := service.ListRules(ctx, filter)
	if err != nil {
		t.Fatalf("Failed to list rules: %v", err)
	}

	if len(rules) != 2 {
		t.Errorf("Expected 2 rules for alice, got %d", len(rules))
	}
}

func TestACLService_ConvenienceMethods(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Grant READWRITE access
	_, err := service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessReadWrite, "_system", "test grant", nil)
	if err != nil {
		t.Fatalf("Failed to grant access: %v", err)
	}

	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: "production",
	}

	// Test CanConnect (requires READ)
	decision, err := service.CanConnect(ctx, alice, ResourceTypeWorkspace, "production", "production", uuid.New())
	if err != nil {
		t.Fatalf("CanConnect failed: %v", err)
	}
	if !decision.Allowed {
		t.Error("Expected CanConnect to allow access")
	}

	// Test CanSendMessage (requires READWRITE)
	decision, err = service.CanSendMessage(ctx, alice, ResourceTypeWorkspace, "production", "production", uuid.New())
	if err != nil {
		t.Fatalf("CanSendMessage failed: %v", err)
	}
	if !decision.Allowed {
		t.Error("Expected CanSendMessage to allow access")
	}

	// Test CanManageWorkspace (requires MANAGE) - should be denied
	decision, err = service.CanManageWorkspace(ctx, alice, "production", uuid.New())
	if err != nil {
		t.Fatalf("CanManageWorkspace failed: %v", err)
	}
	if decision.Allowed {
		t.Error("Expected CanManageWorkspace to deny access (user only has READWRITE)")
	}
}

// Benchmark ACL evaluation with cache
func BenchmarkACLEvaluation(b *testing.B) {
	config := testutil.GetPostgresConfig()

	db, err := sql.Open("postgres", config.DSN())
	if err != nil {
		b.Skipf("Skipping benchmark, database not available: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		b.Skipf("Skipping benchmark, database not available: %v", err)
	}

	service := NewService(db, "test-gateway")
	defer service.Close()

	ctx := context.Background()

	// Setup: grant access
	service.GrantAccess(ctx, PrincipalTypeUser, "alice", ResourceTypeWorkspace, "production", AccessRead, "_system", "benchmark", nil)

	alice := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice",
		Workspace: "production",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		service.CheckAccess(ctx, alice, ResourceTypeWorkspace, "production", "connect", "production", uuid.New(), AccessRead)
	}
}
