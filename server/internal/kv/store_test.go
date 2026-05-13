package kv

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// TestStoreIntegration tests the KV store against a real Redis instance
// Uses dev infrastructure configuration from testutil
func TestStoreIntegration(t *testing.T) {
	// Use first Redis node from dev infrastructure
	redisAddrs := testutil.GetRedisAddrs()
	if len(redisAddrs) == 0 {
		t.Skip("No Redis addresses configured")
	}
	redisAddr := redisAddrs[0]

	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	store := NewStoreFromClient(redisClient)

	// Test connectivity
	if err := store.Ping(ctx); err != nil {
		t.Skipf("Redis not available at %s (dev infrastructure may not be running): %v", redisAddr, err)
	}

	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "production",
		Implementation: "test-agent",
		Specifier:      "v1",
	}

	// Clean up before test
	t.Cleanup(func() {
		store.DeleteByPattern(ctx, agent, ScopeGlobal, "*", "", "")
		store.DeleteByPattern(ctx, agent, ScopeWorkspace, "*", "", "production")
		store.DeleteByPattern(ctx, agent, ScopeUser, "*", "alice", "")
		store.DeleteByPattern(ctx, agent, ScopeUserWorkspace, "*", "alice", "production")
	})

	t.Run("GlobalScope", func(t *testing.T) {
		// Set a global value
		err := store.Set(ctx, agent, ScopeGlobal, "api_key", "secret123", "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set global key: %v", err)
		}

		// Get the value
		val, err := store.Get(ctx, agent, ScopeGlobal, "api_key", "", "")
		if err != nil {
			t.Fatalf("Failed to get global key: %v", err)
		}
		if val != "secret123" {
			t.Errorf("Expected 'secret123', got '%s'", val)
		}

		// List all global keys
		items, err := store.List(ctx, agent, ScopeGlobal, "", "")
		if err != nil {
			t.Fatalf("Failed to list global keys: %v", err)
		}
		if items["api_key"] != "secret123" {
			t.Errorf("List() didn't return expected key")
		}
	})

	t.Run("WorkspaceScope", func(t *testing.T) {
		// Set a workspace value
		err := store.Set(ctx, agent, ScopeWorkspace, "config", "prod_config", "", "production", 0)
		if err != nil {
			t.Fatalf("Failed to set workspace key: %v", err)
		}

		// Get the value
		val, err := store.Get(ctx, agent, ScopeWorkspace, "config", "", "production")
		if err != nil {
			t.Fatalf("Failed to get workspace key: %v", err)
		}
		if val != "prod_config" {
			t.Errorf("Expected 'prod_config', got '%s'", val)
		}

		// Verify isolation - should not see this in different workspace
		_, err = store.Get(ctx, agent, ScopeWorkspace, "config", "", "staging")
		if err == nil {
			t.Error("Expected error when accessing different workspace")
		}
	})

	t.Run("UserScope", func(t *testing.T) {
		// Set a user-scoped value
		err := store.Set(ctx, agent, ScopeUser, "session", "user_session_123", "alice", "", 0)
		if err != nil {
			t.Fatalf("Failed to set user key: %v", err)
		}

		// Get the value
		val, err := store.Get(ctx, agent, ScopeUser, "session", "alice", "")
		if err != nil {
			t.Fatalf("Failed to get user key: %v", err)
		}
		if val != "user_session_123" {
			t.Errorf("Expected 'user_session_123', got '%s'", val)
		}

		// Verify isolation - different user
		_, err = store.Get(ctx, agent, ScopeUser, "session", "bob", "")
		if err == nil {
			t.Error("Expected error when accessing different user")
		}
	})

	t.Run("UserWorkspaceScope", func(t *testing.T) {
		// Set a user-workspace value
		err := store.Set(ctx, agent, ScopeUserWorkspace, "history", "conversation_history", "alice", "production", 0)
		if err != nil {
			t.Fatalf("Failed to set user-workspace key: %v", err)
		}

		// Get the value
		val, err := store.Get(ctx, agent, ScopeUserWorkspace, "history", "alice", "production")
		if err != nil {
			t.Fatalf("Failed to get user-workspace key: %v", err)
		}
		if val != "conversation_history" {
			t.Errorf("Expected 'conversation_history', got '%s'", val)
		}
	})

	t.Run("TTL", func(t *testing.T) {
		// Set a key with TTL
		err := store.Set(ctx, agent, ScopeGlobal, "temp_key", "temp_value", "", "", time.Second)
		if err != nil {
			t.Fatalf("Failed to set TTL key: %v", err)
		}

		// Check TTL is set
		ttl, err := store.GetTTL(ctx, agent, ScopeGlobal, "temp_key", "", "")
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		if ttl <= 0 || ttl > time.Second {
			t.Errorf("Unexpected TTL: %v", ttl)
		}

		// Poll until key expires or deadline
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			_, err = store.Get(ctx, agent, ScopeGlobal, "temp_key", "", "")
			if err != nil {
				break // Key expired as expected
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Key should be gone
		_, err = store.Get(ctx, agent, ScopeGlobal, "temp_key", "", "")
		if err == nil {
			t.Error("Expected key to be expired")
		}
	})

	t.Run("Increment", func(t *testing.T) {
		// Set counter
		err := store.Set(ctx, agent, ScopeGlobal, "counter", "0", "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set counter: %v", err)
		}

		// Increment
		val, err := store.Increment(ctx, agent, ScopeGlobal, "counter", "", "")
		if err != nil {
			t.Fatalf("Failed to increment: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}

		// Get value
		result, err := store.Get(ctx, agent, ScopeGlobal, "counter", "", "")
		if err != nil {
			t.Fatalf("Failed to get counter: %v", err)
		}
		if result != "1" {
			t.Errorf("Expected '1', got '%s'", result)
		}
	})

	t.Run("MultipleOperations", func(t *testing.T) {
		// Set multiple
		values := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}
		err := store.SetMultiple(ctx, agent, ScopeGlobal, values, "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set multiple: %v", err)
		}

		// Get multiple
		keys := []string{"key1", "key2", "key3"}
		result, err := store.GetMultiple(ctx, agent, ScopeGlobal, keys, "", "")
		if err != nil {
			t.Fatalf("Failed to get multiple: %v", err)
		}
		if len(result) != 3 {
			t.Errorf("Expected 3 keys, got %d", len(result))
		}

		// Delete multiple
		err = store.DeleteMultiple(ctx, agent, ScopeGlobal, keys, "", "")
		if err != nil {
			t.Fatalf("Failed to delete multiple: %v", err)
		}

		// Verify deleted
		for _, key := range keys {
			_, err := store.Get(ctx, agent, ScopeGlobal, key, "", "")
			if err == nil {
				t.Errorf("Key %s should be deleted", key)
			}
		}
	})

	t.Run("JSONOperations", func(t *testing.T) {
		type TestData struct {
			Name    string `json:"name"`
			Version int    `json:"version"`
			Active  bool   `json:"active"`
		}

		// Set JSON
		data := TestData{Name: "test-agent", Version: 1, Active: true}
		err := store.SetJSON(ctx, agent, ScopeGlobal, "config", data, "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set JSON: %v", err)
		}

		// Get JSON
		var result TestData
		err = store.GetJSON(ctx, agent, ScopeGlobal, "config", "", "", &result)
		if err != nil {
			t.Fatalf("Failed to get JSON: %v", err)
		}
		if result.Name != "test-agent" || result.Version != 1 || result.Active != true {
			t.Errorf("Unexpected JSON result: %+v", result)
		}
	})

	t.Run("DeleteByPattern", func(t *testing.T) {
		// Set some keys
		store.Set(ctx, agent, ScopeGlobal, "pattern_key_1", "value1", "", "", 0)
		store.Set(ctx, agent, ScopeGlobal, "pattern_key_2", "value2", "", "", 0)
		store.Set(ctx, agent, ScopeGlobal, "other_key", "value3", "", "", 0)

		// Delete by pattern
		count, err := store.DeleteByPattern(ctx, agent, ScopeGlobal, "pattern_key_*", "", "")
		if err != nil {
			t.Fatalf("Failed to delete by pattern: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 keys deleted, got %d", count)
		}

		// Verify pattern keys are gone
		_, err = store.Get(ctx, agent, ScopeGlobal, "pattern_key_1", "", "")
		if err == nil {
			t.Error("pattern_key_1 should be deleted")
		}

		// Verify other key remains
		_, err = store.Get(ctx, agent, ScopeGlobal, "other_key", "", "")
		if err != nil {
			t.Errorf("other_key should exist: %v", err)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		// Set a key
		store.Set(ctx, agent, ScopeGlobal, "exists_test", "value", "", "", 0)

		// Check exists
		exists, err := store.Exists(ctx, agent, ScopeGlobal, "exists_test", "", "")
		if err != nil {
			t.Fatalf("Failed to check exists: %v", err)
		}
		if !exists {
			t.Error("Key should exist")
		}

		// Check non-existent
		exists, err = store.Exists(ctx, agent, ScopeGlobal, "does_not_exist", "", "")
		if err != nil {
			t.Fatalf("Failed to check exists: %v", err)
		}
		if exists {
			t.Error("Key should not exist")
		}
	})

	t.Run("NamespaceIsolation", func(t *testing.T) {
		agent1 := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "production",
			Implementation: "python-worker",
			Specifier:      "instance-1",
		}

		agent2 := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "production",
			Implementation: "python-worker",
			Specifier:      "instance-2",
		}

		// Per-agent scopes (user, user-workspace) MUST stay isolated.
		// Each agent's per-user notes live in its own namespace.
		err := store.Set(ctx, agent1, ScopeUserWorkspace, "private_data", "agent1_secret", "alice", "production", 0)
		if err != nil {
			t.Fatalf("Failed to set per-agent key: %v", err)
		}
		_, err = store.Get(ctx, agent2, ScopeUserWorkspace, "private_data", "alice", "production")
		if err == nil {
			t.Error("Agent2 should not see Agent1's per-agent (user-workspace) data")
		}

		// Solution A: shared scopes (global, workspace) ARE cross-agent
		// by design. Two distinct agents writing the same key in the same
		// shared scope MUST overwrite each other and read the same value.
		err = store.Set(ctx, agent1, ScopeGlobal, "shared_data", "agent1_wrote_this", "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set shared global key: %v", err)
		}
		val, err := store.Get(ctx, agent2, ScopeGlobal, "shared_data", "", "")
		if err != nil {
			t.Fatalf("Agent2 must see Agent1's shared global write: %v", err)
		}
		if val != "agent1_wrote_this" {
			t.Errorf("expected shared cross-agent global value, got %q", val)
		}
	})
}

// TestStoreDefaultTTL tests that the default TTL is applied when no TTL is specified
func TestStoreDefaultTTL(t *testing.T) {
	redisAddrs := testutil.GetRedisAddrs()
	if len(redisAddrs) == 0 {
		t.Skip("No Redis addresses configured")
	}
	redisAddr := redisAddrs[0]

	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s (dev infrastructure may not be running): %v", redisAddr, err)
	}

	defaultTTL := 2 * time.Second
	store := NewStoreFromClientWithTTL(redisClient, defaultTTL)

	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test",
		Implementation: "ttl-test-agent",
		Specifier:      "v1",
	}

	t.Cleanup(func() {
		store.DeleteByPattern(ctx, agent, ScopeGlobal, "*", "", "")
	})

	t.Run("DefaultTTLAppliedWhenNoTTLSpecified", func(t *testing.T) {
		err := store.Set(ctx, agent, ScopeGlobal, "no-ttl-key", "value", "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}

		ttl, err := store.GetTTL(ctx, agent, ScopeGlobal, "no-ttl-key", "", "")
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		if ttl <= 0 || ttl > defaultTTL {
			t.Errorf("Expected TTL between 0 and %v, got %v", defaultTTL, ttl)
		}
	})

	t.Run("ExplicitTTLOverridesDefault", func(t *testing.T) {
		explicitTTL := 10 * time.Second
		err := store.Set(ctx, agent, ScopeGlobal, "explicit-ttl-key", "value", "", "", explicitTTL)
		if err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}

		ttl, err := store.GetTTL(ctx, agent, ScopeGlobal, "explicit-ttl-key", "", "")
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		if ttl <= defaultTTL || ttl > explicitTTL {
			t.Errorf("Expected TTL between %v and %v, got %v", defaultTTL, explicitTTL, ttl)
		}
	})

	t.Run("DefaultTTLAppliedInSetMultiple", func(t *testing.T) {
		values := map[string]string{
			"multi-key1": "value1",
			"multi-key2": "value2",
		}
		err := store.SetMultiple(ctx, agent, ScopeGlobal, values, "", "", 0)
		if err != nil {
			t.Fatalf("Failed to set multiple keys: %v", err)
		}

		for key := range values {
			ttl, err := store.GetTTL(ctx, agent, ScopeGlobal, key, "", "")
			if err != nil {
				t.Fatalf("Failed to get TTL for %s: %v", key, err)
			}
			if ttl <= 0 || ttl > defaultTTL {
				t.Errorf("Key %s: expected TTL between 0 and %v, got %v", key, defaultTTL, ttl)
			}
		}
	})
}

// TestStoreUnit tests without Redis (mock-based tests for namespace logic)
func TestStoreUnit(t *testing.T) {
	// These tests verify the namespace logic without requiring Redis
	t.Run("NamespaceBuildConsistency", func(t *testing.T) {
		agent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "production",
			Implementation: "python-worker",
			Specifier:      "instance-1",
		}

		// Round-trip through a per-agent scope so impl/spec survive parse.
		// (Shared scopes — global, workspace — intentionally drop the
		// agent identity from the storage key under Solution A; that's
		// covered by namespace_test.go::TestBuildNamespace_SharedScopesCrossAgent.)
		ns := BuildNamespace(agent, ScopeUserWorkspace, "alice", "production")
		if ns != "kv:agent:python-worker|instance-1:user:alice:ws:production" {
			t.Errorf("Unexpected namespace: %s", ns)
		}

		impl, spec, scope, userID, ws, err := ParseNamespace(ns)
		if err != nil {
			t.Fatalf("ParseNamespace failed: %v", err)
		}
		if impl != "python-worker" || spec != "instance-1" {
			t.Errorf("Unexpected parsed impl/spec: %s/%s", impl, spec)
		}
		if scope != ScopeUserWorkspace || ws != "production" || userID != "alice" {
			t.Errorf("Unexpected parsed scope/userID/workspace: %s/%s/%s", scope, userID, ws)
		}

		// Sanity-check the new shared shape too: round-trip MUST yield the
		// same scope and workspace, with empty agent identity (no owner).
		shared := BuildNamespace(agent, ScopeWorkspace, "", "production")
		if shared != "kv:ws:production" {
			t.Errorf("Unexpected shared workspace namespace: %s", shared)
		}
		impl2, spec2, scope2, _, ws2, err := ParseNamespace(shared)
		if err != nil {
			t.Fatalf("ParseNamespace(shared) failed: %v", err)
		}
		if impl2 != "" || spec2 != "" {
			t.Errorf("shared scope must have empty impl/spec, got %s/%s", impl2, spec2)
		}
		if scope2 != ScopeWorkspace || ws2 != "production" {
			t.Errorf("Unexpected parsed shared scope/workspace: %s/%s", scope2, ws2)
		}
	})

	t.Run("KVScopeString", func(t *testing.T) {
		tests := []struct {
			scope    KVScope
			expected string
		}{
			{ScopeGlobal, "global"},
			{ScopeWorkspace, "workspace"},
			{ScopeUser, "user"},
			{ScopeUserWorkspace, "user-workspace"},
		}

		for _, tt := range tests {
			if tt.scope.String() != tt.expected {
				t.Errorf("Scope.String() = %s, want %s", tt.scope, tt.expected)
			}
		}
	})
}
