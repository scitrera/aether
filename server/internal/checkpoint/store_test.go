package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// TestStoreIntegration tests the checkpoint store against a real Redis instance
func TestStoreIntegration(t *testing.T) {
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
	if err := store.HealthCheck(ctx); err != nil {
		t.Skipf("Redis not available at %s (dev infrastructure may not be running): %v", redisAddr, err)
	}

	// Test identity
	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test-workspace",
		Implementation: "test-agent",
		Specifier:      "inst-1",
	}

	// Clean up before and after tests
	cleanup := func() {
		store.DeleteAll(ctx, agent)
	}
	cleanup()
	t.Cleanup(cleanup)

	t.Run("SaveAndLoad", func(t *testing.T) {
		data := []byte(`{"state": "test", "counter": 42}`)

		// Save checkpoint
		err := store.Save(ctx, agent, "test-key", data, 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Load checkpoint
		cp, err := store.Load(ctx, agent, "test-key")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp == nil {
			t.Fatal("Load() returned nil checkpoint")
		}

		if string(cp.Data) != string(data) {
			t.Errorf("Load() data = %q, want %q", string(cp.Data), string(data))
		}
		if cp.Key != "test-key" {
			t.Errorf("Load() key = %q, want %q", cp.Key, "test-key")
		}
		if cp.Identity != agent.String() {
			t.Errorf("Load() identity = %q, want %q", cp.Identity, agent.String())
		}
		if cp.SavedAt.IsZero() {
			t.Error("Load() savedAt should not be zero")
		}
	})

	t.Run("SaveAndLoadDefaultKey", func(t *testing.T) {
		data := []byte("default checkpoint data")

		// Save with empty key (should use DefaultKey)
		err := store.Save(ctx, agent, "", data, 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Load with empty key
		cp, err := store.Load(ctx, agent, "")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp == nil {
			t.Fatal("Load() returned nil checkpoint")
		}

		if string(cp.Data) != string(data) {
			t.Errorf("Load() data = %q, want %q", string(cp.Data), string(data))
		}
		if cp.Key != DefaultKey {
			t.Errorf("Load() key = %q, want %q", cp.Key, DefaultKey)
		}
	})

	t.Run("LoadNonExistent", func(t *testing.T) {
		cp, err := store.Load(ctx, agent, "nonexistent-key")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp != nil {
			t.Errorf("Load() = %v, want nil for nonexistent checkpoint", cp)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		data := []byte("to be deleted")

		// Save checkpoint
		err := store.Save(ctx, agent, "delete-test", data, 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Verify it exists
		exists, err := store.Exists(ctx, agent, "delete-test")
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Fatal("Checkpoint should exist before delete")
		}

		// Delete checkpoint
		err = store.Delete(ctx, agent, "delete-test")
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}

		// Verify it's gone
		exists, err = store.Exists(ctx, agent, "delete-test")
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists {
			t.Error("Checkpoint should not exist after delete")
		}
	})

	t.Run("List", func(t *testing.T) {
		// Save multiple checkpoints
		keys := []string{"list-1", "list-2", "list-3"}
		for _, key := range keys {
			err := store.Save(ctx, agent, key, []byte("data-"+key), 0)
			if err != nil {
				t.Fatalf("Save() error = %v", err)
			}
		}

		// List checkpoints
		gotKeys, err := store.List(ctx, agent)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}

		// Check that all saved keys are in the list
		for _, wantKey := range keys {
			found := false
			for _, gotKey := range gotKeys {
				if gotKey == wantKey {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("List() missing key %q", wantKey)
			}
		}
	})

	t.Run("Exists", func(t *testing.T) {
		// Save checkpoint
		err := store.Save(ctx, agent, "exists-test", []byte("exists"), 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Check exists
		exists, err := store.Exists(ctx, agent, "exists-test")
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Error("Exists() = false, want true")
		}

		// Check non-existent
		exists, err = store.Exists(ctx, agent, "does-not-exist")
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists {
			t.Error("Exists() = true, want false for nonexistent key")
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		// Create a separate identity for this test
		taskIdentity := models.Identity{
			Type:           models.PrincipalTask,
			Workspace:      "test-workspace",
			Implementation: "test-task",
			Specifier:      "delete-all-test",
		}

		// Save multiple checkpoints
		for i := 0; i < 5; i++ {
			key := "batch-" + string(rune('a'+i))
			err := store.Save(ctx, taskIdentity, key, []byte("batch data"), 0)
			if err != nil {
				t.Fatalf("Save() error = %v", err)
			}
		}

		// Verify they exist
		keys, err := store.List(ctx, taskIdentity)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if len(keys) < 5 {
			t.Fatalf("Expected at least 5 keys, got %d", len(keys))
		}

		// Delete all
		count, err := store.DeleteAll(ctx, taskIdentity)
		if err != nil {
			t.Fatalf("DeleteAll() error = %v", err)
		}
		if count < 5 {
			t.Errorf("DeleteAll() count = %d, want >= 5", count)
		}

		// Verify all are gone
		keys, err = store.List(ctx, taskIdentity)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("List() returned %d keys after DeleteAll, want 0", len(keys))
		}
	})

	t.Run("TTL", func(t *testing.T) {
		data := []byte("expiring data")

		// Save with 1 second TTL
		err := store.Save(ctx, agent, "ttl-test", data, time.Second)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Verify it exists immediately
		cp, err := store.Load(ctx, agent, "ttl-test")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp == nil {
			t.Fatal("Checkpoint should exist before TTL expiry")
		}
		if cp.ExpiresAt.IsZero() {
			t.Error("ExpiresAt should be set when TTL is specified")
		}

		// Poll until checkpoint expires or deadline
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			cp, err = store.Load(ctx, agent, "ttl-test")
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cp == nil {
				break // Expired as expected
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Should be gone
		cp, err = store.Load(ctx, agent, "ttl-test")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp != nil {
			t.Error("Checkpoint should be expired after TTL")
		}
	})

	t.Run("IdentityIsolation", func(t *testing.T) {
		agent1 := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "workspace-1",
			Implementation: "worker",
			Specifier:      "inst-1",
		}
		agent2 := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "workspace-2",
			Implementation: "worker",
			Specifier:      "inst-2",
		}

		// Clean up
		t.Cleanup(func() {
			store.DeleteAll(ctx, agent1)
			store.DeleteAll(ctx, agent2)
		})

		// Save checkpoint for agent1
		err := store.Save(ctx, agent1, "private", []byte("agent1 secret"), 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Agent2 should not see agent1's checkpoint
		cp, err := store.Load(ctx, agent2, "private")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp != nil {
			t.Error("Agent2 should not see Agent1's checkpoint")
		}

		// Agent1 should see its own checkpoint
		cp, err = store.Load(ctx, agent1, "private")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp == nil {
			t.Error("Agent1 should see its own checkpoint")
		}
	})

	t.Run("OverwriteCheckpoint", func(t *testing.T) {
		// Save initial data
		err := store.Save(ctx, agent, "overwrite-test", []byte("initial"), 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Save again with different data
		err = store.Save(ctx, agent, "overwrite-test", []byte("updated"), 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Load and verify updated data
		cp, err := store.Load(ctx, agent, "overwrite-test")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp == nil {
			t.Fatal("Load() returned nil")
		}
		if string(cp.Data) != "updated" {
			t.Errorf("Load() data = %q, want %q", string(cp.Data), "updated")
		}
	})

	t.Run("LargeData", func(t *testing.T) {
		// Create 1MB of data
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		// Save large checkpoint
		err := store.Save(ctx, agent, "large-data", largeData, 0)
		if err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Load and verify
		cp, err := store.Load(ctx, agent, "large-data")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp == nil {
			t.Fatal("Load() returned nil")
		}
		if len(cp.Data) != len(largeData) {
			t.Errorf("Load() data length = %d, want %d", len(cp.Data), len(largeData))
		}

		// Verify data integrity (spot check)
		for i := 0; i < len(largeData); i += 1000 {
			if cp.Data[i] != largeData[i] {
				t.Errorf("Data mismatch at position %d: got %d, want %d", i, cp.Data[i], largeData[i])
				break
			}
		}
	})
}

// TestStoreUnit tests without Redis
func TestStoreUnit(t *testing.T) {
	t.Run("BuildKey", func(t *testing.T) {
		agent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "prod",
			Implementation: "worker",
			Specifier:      "inst-1",
		}

		key := buildKey(agent, "mykey")
		expected := "checkpoint:ag::prod::worker::inst-1:mykey"
		if key != expected {
			t.Errorf("buildKey() = %q, want %q", key, expected)
		}
	})

	t.Run("BuildKeyDefaultKey", func(t *testing.T) {
		agent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "prod",
			Implementation: "worker",
			Specifier:      "inst-1",
		}

		key := buildKey(agent, "")
		expected := "checkpoint:ag::prod::worker::inst-1:default"
		if key != expected {
			t.Errorf("buildKey() with empty key = %q, want %q", key, expected)
		}
	})

	t.Run("BuildPatternKey", func(t *testing.T) {
		task := models.Identity{
			Type:           models.PrincipalTask,
			Workspace:      "dev",
			Implementation: "batch",
			Specifier:      "job-1",
		}

		pattern := buildPatternKey(task)
		expected := "checkpoint:tu::dev::batch::job-1:*"
		if pattern != expected {
			t.Errorf("buildPatternKey() = %q, want %q", pattern, expected)
		}
	})

	t.Run("Constants", func(t *testing.T) {
		if DefaultKey != "default" {
			t.Errorf("DefaultKey = %q, want %q", DefaultKey, "default")
		}
		if KeyPrefix != "checkpoint" {
			t.Errorf("KeyPrefix = %q, want %q", KeyPrefix, "checkpoint")
		}
	})
}
