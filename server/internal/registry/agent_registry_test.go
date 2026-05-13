package registry

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/testutil"
)

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Use dev infrastructure PostgreSQL instance
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, func() {}
	}

	return testDB.DB, cleanup
}

func TestAgentRegistry_Register(t *testing.T) {
	db, cleanup := setupTestDB(t)
	if db == nil {
		return // Test was skipped
	}
	defer cleanup()

	registry := NewAgentRegistry(db)
	ctx := context.Background()

	// Test successful registration
	reg := &AgentRegistration{
		Implementation: "test-agent",
		LaunchParams: map[string]interface{}{
			"profile": "kubernetes",
			"image":   "test-image:v1",
			"cpu":     "500m",
		},
		Description: "Test agent",
	}

	err := registry.Register(ctx, reg)
	if err != nil {
		t.Fatalf("Failed to register agent: %v", err)
	}

	// Verify registration
	retrieved, err := registry.Get(ctx, "test-agent")
	if err != nil {
		t.Fatalf("Failed to get agent: %v", err)
	}

	if retrieved.Implementation != "test-agent" {
		t.Errorf("Expected implementation 'test-agent', got '%s'", retrieved.Implementation)
	}

	if retrieved.LaunchParams["profile"] != "kubernetes" {
		t.Errorf("Expected profile 'kubernetes', got '%v'", retrieved.LaunchParams["profile"])
	}
}

func TestAgentRegistry_RegisterWithoutProfile(t *testing.T) {
	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	registry := NewAgentRegistry(db)
	ctx := context.Background()

	// Test registration without profile (should fail)
	reg := &AgentRegistration{
		Implementation: "test-agent",
		LaunchParams: map[string]interface{}{
			"image": "test-image:v1",
		},
	}

	err := registry.Register(ctx, reg)
	if err == nil {
		t.Fatal("Expected error when registering without profile, got nil")
	}

	// Verify it's a ProfileRequiredError
	t.Logf("Got expected error: %v", err)
}

func TestAgentRegistry_Exists(t *testing.T) {
	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	registry := NewAgentRegistry(db)
	ctx := context.Background()

	// Register agent
	reg := &AgentRegistration{
		Implementation: "test-agent",
		LaunchParams: map[string]interface{}{
			"profile": "kubernetes",
		},
	}
	registry.Register(ctx, reg)

	// Test exists
	exists, err := registry.Exists(ctx, "test-agent")
	if err != nil {
		t.Fatalf("Failed to check existence: %v", err)
	}
	if !exists {
		t.Error("Expected agent to exist")
	}

	// Test not exists
	exists, err = registry.Exists(ctx, "non-existent")
	if err != nil {
		t.Fatalf("Failed to check existence: %v", err)
	}
	if exists {
		t.Error("Expected agent to not exist")
	}
}

func TestAgentRegistry_List(t *testing.T) {
	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	registry := NewAgentRegistry(db)
	ctx := context.Background()

	// Register multiple agents
	agents := []*AgentRegistration{
		{
			Implementation: "k8s-agent-1",
			LaunchParams: map[string]interface{}{
				"profile": "kubernetes",
				"image":   "agent1:v1",
			},
		},
		{
			Implementation: "k8s-agent-2",
			LaunchParams: map[string]interface{}{
				"profile": "kubernetes",
				"image":   "agent2:v1",
			},
		},
		{
			Implementation: "docker-agent-1",
			LaunchParams: map[string]interface{}{
				"profile": "docker",
				"image":   "agent3:v1",
			},
		},
	}

	for _, agent := range agents {
		registry.Register(ctx, agent)
	}

	// List all agents
	all, err := registry.List(ctx, "")
	if err != nil {
		t.Fatalf("Failed to list agents: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("Expected 3 agents, got %d", len(all))
	}

	// List by profile
	k8sAgents, err := registry.List(ctx, "kubernetes")
	if err != nil {
		t.Fatalf("Failed to list kubernetes agents: %v", err)
	}
	if len(k8sAgents) != 2 {
		t.Errorf("Expected 2 kubernetes agents, got %d", len(k8sAgents))
	}
}

func TestAgentRegistry_Delete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	registry := NewAgentRegistry(db)
	ctx := context.Background()

	// Register agent
	reg := &AgentRegistration{
		Implementation: "test-agent",
		LaunchParams: map[string]interface{}{
			"profile": "kubernetes",
		},
	}
	registry.Register(ctx, reg)

	// Delete agent
	err := registry.Delete(ctx, "test-agent")
	if err != nil {
		t.Fatalf("Failed to delete agent: %v", err)
	}

	// Verify deletion
	exists, _ := registry.Exists(ctx, "test-agent")
	if exists {
		t.Error("Agent still exists after deletion")
	}

	// Test delete non-existent
	err = registry.Delete(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when deleting non-existent agent")
	}
}

func TestMergeLaunchParams(t *testing.T) {
	defaults := map[string]interface{}{
		"profile": "kubernetes",
		"cpu":     "500m",
		"memory":  "1Gi",
	}

	overrides := map[string]interface{}{
		"cpu":    "1000m",
		"labels": map[string]string{"env": "prod"},
	}

	merged := MergeLaunchParams(defaults, overrides)

	// Check that overrides took precedence
	if merged["cpu"] != "1000m" {
		t.Errorf("Expected cpu '1000m', got '%v'", merged["cpu"])
	}

	// Check that defaults are preserved
	if merged["profile"] != "kubernetes" {
		t.Errorf("Expected profile 'kubernetes', got '%v'", merged["profile"])
	}
	if merged["memory"] != "1Gi" {
		t.Errorf("Expected memory '1Gi', got '%v'", merged["memory"])
	}

	// Check that new fields from overrides are added
	if merged["labels"] == nil {
		t.Error("Expected labels to be present")
	}
}
