package state

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// TestSessionRegistryIntegration tests the session registry against a real Redis instance
func TestSessionRegistryIntegration(t *testing.T) {
	redisAddrs := testutil.GetRedisAddrs()
	if len(redisAddrs) == 0 {
		t.Skip("No Redis addresses configured")
	}
	redisAddr := redisAddrs[0]

	ctx := context.Background()
	registry := NewSessionRegistryFromClient(redis.NewClient(&redis.Options{Addr: redisAddr}))

	// Test connectivity
	if err := registry.redis.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s (dev infrastructure may not be running): %v", redisAddr, err)
	}

	// Test identity
	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test-workspace",
		Implementation: "test-agent",
		Specifier:      "session-test",
	}

	t.Run("AcquireLock", func(t *testing.T) {
		// Clean up any existing lock
		registry.ReleaseLock(ctx, agent, "any")
		registry.redis.Del(ctx, "lock:"+agent.String())

		sessionID := "session-1"

		// Acquire lock
		acquired, err := registry.AcquireLock(ctx, agent, sessionID)
		if err != nil {
			t.Fatalf("AcquireLock() error = %v", err)
		}
		if !acquired {
			t.Error("AcquireLock() = false, want true for first acquire")
		}

		// Trying to acquire again with different session should fail
		acquired2, err := registry.AcquireLock(ctx, agent, "session-2")
		if err != nil {
			t.Fatalf("AcquireLock() second error = %v", err)
		}
		if acquired2 {
			t.Error("AcquireLock() = true, want false when lock already held")
		}

		// Clean up
		registry.ReleaseLock(ctx, agent, sessionID)
	})

	t.Run("AcquireOrResumeLock_NewLock", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "resume-test-new",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		sessionID := "new-session"

		// Acquire new lock without resume
		acquired, resumed, forced, err := acquireLegacy(registry, ctx, testAgent, sessionID, "", LockRefreshInterval.Milliseconds())
		if err != nil {
			t.Fatalf("AcquireOrResumeLock() error = %v", err)
		}
		if !acquired {
			t.Error("acquired = false, want true")
		}
		if resumed {
			t.Error("resumed = true, want false for new lock")
		}
		if forced {
			t.Error("forced = true, want false for new lock")
		}

		// Clean up
		registry.ReleaseLock(ctx, testAgent, sessionID)
	})

	t.Run("AcquireOrResumeLock_Resume", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "resume-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		originalSessionID := "original-session"
		newSessionID := "new-session"

		// First acquire
		acquired, err := registry.AcquireLock(ctx, testAgent, originalSessionID)
		if err != nil {
			t.Fatalf("AcquireLock() error = %v", err)
		}
		if !acquired {
			t.Fatal("Initial AcquireLock() failed")
		}

		// Resume with matching session ID
		acquired, resumed, forced, err := acquireLegacy(registry, ctx, testAgent, newSessionID, originalSessionID, LockRefreshInterval.Milliseconds())
		if err != nil {
			t.Fatalf("AcquireOrResumeLock() error = %v", err)
		}
		if !acquired {
			t.Error("acquired = false, want true for resume")
		}
		if !resumed {
			t.Error("resumed = false, want true for resume")
		}
		if forced {
			t.Error("forced = true, want false for resume")
		}

		// Clean up
		registry.ReleaseLock(ctx, testAgent, newSessionID)
	})

	t.Run("AcquireOrResumeLock_Conflict", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "conflict-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		originalSessionID := "session-holder"
		wrongResumeID := "wrong-session"
		newSessionID := "new-session"

		// First acquire
		acquired, err := registry.AcquireLock(ctx, testAgent, originalSessionID)
		if err != nil {
			t.Fatalf("AcquireLock() error = %v", err)
		}
		if !acquired {
			t.Fatal("Initial AcquireLock() failed")
		}

		// Try to resume with wrong session ID
		acquired, resumed, forced, err := acquireLegacy(registry, ctx, testAgent, newSessionID, wrongResumeID, LockRefreshInterval.Milliseconds())
		if err != nil {
			t.Fatalf("AcquireOrResumeLock() error = %v", err)
		}
		if acquired {
			t.Error("acquired = true, want false for wrong resume ID")
		}
		if resumed {
			t.Error("resumed = true, want false for wrong resume ID")
		}
		if forced {
			t.Error("forced = true, want false for wrong resume ID")
		}

		// Clean up
		registry.ReleaseLock(ctx, testAgent, originalSessionID)
	})

	t.Run("RefreshLock", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "refresh-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		sessionID := "refresh-session"

		// Acquire lock
		acquired, err := registry.AcquireLock(ctx, testAgent, sessionID)
		if err != nil || !acquired {
			t.Fatalf("AcquireLock() failed: acquired=%v, err=%v", acquired, err)
		}

		// Get initial TTL
		key := "lock:" + testAgent.String()
		initialTTL, err := registry.redis.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("TTL() error = %v", err)
		}

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Refresh lock
		refreshed, err := registry.RefreshLock(ctx, testAgent, sessionID)
		if err != nil {
			t.Fatalf("RefreshLock() error = %v", err)
		}
		if !refreshed {
			t.Error("RefreshLock() = false, want true")
		}

		// TTL should be reset
		newTTL, err := registry.redis.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("TTL() error = %v", err)
		}
		// New TTL should be close to LockTTL (allowing for some milliseconds difference)
		if newTTL < LockTTL-time.Second {
			t.Errorf("TTL after refresh = %v, want close to %v", newTTL, LockTTL)
		}
		t.Logf("Initial TTL: %v, New TTL after refresh: %v", initialTTL, newTTL)

		// Clean up
		registry.ReleaseLock(ctx, testAgent, sessionID)
	})

	t.Run("RefreshLock_WrongSession", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "refresh-wrong-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		sessionID := "correct-session"
		wrongSessionID := "wrong-session"

		// Acquire lock
		acquired, err := registry.AcquireLock(ctx, testAgent, sessionID)
		if err != nil || !acquired {
			t.Fatalf("AcquireLock() failed: acquired=%v, err=%v", acquired, err)
		}

		// Try to refresh with wrong session ID
		refreshed, err := registry.RefreshLock(ctx, testAgent, wrongSessionID)
		if err != nil {
			t.Fatalf("RefreshLock() error = %v", err)
		}
		if refreshed {
			t.Error("RefreshLock() = true, want false for wrong session")
		}

		// Clean up
		registry.ReleaseLock(ctx, testAgent, sessionID)
	})

	t.Run("ReleaseLock", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "release-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		sessionID := "release-session"

		// Acquire lock
		acquired, err := registry.AcquireLock(ctx, testAgent, sessionID)
		if err != nil || !acquired {
			t.Fatalf("AcquireLock() failed")
		}

		// Verify lock exists
		active, err := registry.IsActive(ctx, testAgent.String())
		if err != nil {
			t.Fatalf("IsActive() error = %v", err)
		}
		if !active {
			t.Error("IsActive() = false, want true after acquire")
		}

		// Release lock
		err = registry.ReleaseLock(ctx, testAgent, sessionID)
		if err != nil {
			t.Fatalf("ReleaseLock() error = %v", err)
		}

		// Verify lock is gone
		active, err = registry.IsActive(ctx, testAgent.String())
		if err != nil {
			t.Fatalf("IsActive() error = %v", err)
		}
		if active {
			t.Error("IsActive() = true, want false after release")
		}
	})

	t.Run("ReleaseLock_WrongSession", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "release-wrong-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		sessionID := "correct-session"
		wrongSessionID := "wrong-session"

		// Acquire lock
		acquired, err := registry.AcquireLock(ctx, testAgent, sessionID)
		if err != nil || !acquired {
			t.Fatalf("AcquireLock() failed")
		}

		// Try to release with wrong session ID (should be no-op)
		err = registry.ReleaseLock(ctx, testAgent, wrongSessionID)
		if err != nil {
			t.Fatalf("ReleaseLock() error = %v", err)
		}

		// Lock should still exist
		active, err := registry.IsActive(ctx, testAgent.String())
		if err != nil {
			t.Fatalf("IsActive() error = %v", err)
		}
		if !active {
			t.Error("Lock was released with wrong session ID")
		}

		// Clean up with correct session
		registry.ReleaseLock(ctx, testAgent, sessionID)
	})

	t.Run("IsActive", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "isactive-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		// Should be inactive initially
		active, err := registry.IsActive(ctx, testAgent.String())
		if err != nil {
			t.Fatalf("IsActive() error = %v", err)
		}
		if active {
			t.Error("IsActive() = true, want false when no lock")
		}

		// Acquire lock
		acquired, _ := registry.AcquireLock(ctx, testAgent, "session")
		if !acquired {
			t.Fatal("Failed to acquire lock")
		}

		// Should be active
		active, err = registry.IsActive(ctx, testAgent.String())
		if err != nil {
			t.Fatalf("IsActive() error = %v", err)
		}
		if !active {
			t.Error("IsActive() = false, want true when lock held")
		}

		// Clean up
		registry.ReleaseLock(ctx, testAgent, "session")
	})

	t.Run("IsOnline", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "isonline-test",
		}
		// Ensure clean state
		registry.redis.Del(ctx, "lock:"+testAgent.String())

		// Should be offline initially
		if registry.IsOnline(testAgent) {
			t.Error("IsOnline() = true, want false when no lock")
		}

		// Acquire lock
		acquired, _ := registry.AcquireLock(ctx, testAgent, "session")
		if !acquired {
			t.Fatal("Failed to acquire lock")
		}

		// Should be online
		if !registry.IsOnline(testAgent) {
			t.Error("IsOnline() = false, want true when lock held")
		}

		// Clean up
		registry.ReleaseLock(ctx, testAgent, "session")
	})

	t.Run("RegisterAndUnregisterSession", func(t *testing.T) {
		testAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "test-workspace",
			Implementation: "test-agent",
			Specifier:      "session-reg-test",
		}
		sessionID := "registered-session"

		// Clean up any existing session
		registry.UnregisterSession(ctx, sessionID)

		// Register session
		err := registry.RegisterSession(ctx, testAgent, sessionID, "test-gateway-1")
		if err != nil {
			t.Fatalf("RegisterSession() error = %v", err)
		}

		// Verify session exists
		key := "session:" + sessionID
		exists, err := registry.redis.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists == 0 {
			t.Error("Session should exist after registration")
		}

		// Check session data
		identity, err := registry.redis.HGet(ctx, key, "identity").Result()
		if err != nil {
			t.Fatalf("HGet() error = %v", err)
		}
		if identity != testAgent.String() {
			t.Errorf("identity = %q, want %q", identity, testAgent.String())
		}

		// Check gateway_id was stored
		gw, err := registry.redis.HGet(ctx, key, "gateway_id").Result()
		if err != nil {
			t.Fatalf("HGet(gateway_id) error = %v", err)
		}
		if gw != "test-gateway-1" {
			t.Errorf("gateway_id = %q, want %q", gw, "test-gateway-1")
		}

		// Unregister session
		err = registry.UnregisterSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("UnregisterSession() error = %v", err)
		}

		// Verify session is gone
		exists, err = registry.redis.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists != 0 {
			t.Error("Session should not exist after unregistration")
		}
	})

	t.Run("GetSessionIdentity", func(t *testing.T) {
		testUser := models.Identity{
			Type:      models.PrincipalUser,
			ID:        "alice",
			Specifier: "window-1",
		}
		sessionID := "lookup-session"

		registry.UnregisterSession(ctx, sessionID)
		if err := registry.RegisterSession(ctx, testUser, sessionID, "test-gateway-1"); err != nil {
			t.Fatalf("RegisterSession() error = %v", err)
		}

		got, err := registry.GetSessionIdentity(ctx, sessionID)
		if err != nil {
			t.Fatalf("GetSessionIdentity() error = %v", err)
		}
		if got.String() != testUser.String() {
			t.Fatalf("GetSessionIdentity() = %q, want %q", got.String(), testUser.String())
		}

		_ = registry.UnregisterSession(ctx, sessionID)
	})

	t.Run("CleanupStaleLocks", func(t *testing.T) {
		// Create some keys without TTL (simulating stale locks)
		staleLockKey := "lock:ag.stale-workspace.stale-impl.stale-spec"
		registry.redis.Set(ctx, staleLockKey, "stale-session", 0) // No TTL

		// Create a lock with TTL
		freshAgent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "fresh-workspace",
			Implementation: "fresh-impl",
			Specifier:      "fresh-spec",
		}
		registry.AcquireLock(ctx, freshAgent, "fresh-session")

		// Run cleanup
		removed, err := registry.CleanupStaleLocks(ctx)
		if err != nil {
			t.Fatalf("CleanupStaleLocks() error = %v", err)
		}

		t.Logf("Removed %d stale locks", removed)

		// Stale lock should be gone
		exists, err := registry.redis.Exists(ctx, staleLockKey).Result()
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists != 0 {
			t.Error("Stale lock should be removed")
		}

		// Fresh lock should still exist
		if !registry.IsOnline(freshAgent) {
			t.Error("Fresh lock should still exist after cleanup")
		}

		// Clean up
		registry.ReleaseLock(ctx, freshAgent, "fresh-session")
	})

	t.Run("GetRedisClient", func(t *testing.T) {
		client := registry.GetRedisClient()
		if client == nil {
			t.Error("GetRedisClient() returned nil")
		}

		// Verify it works
		err := client.Ping(ctx).Err()
		if err != nil {
			t.Errorf("GetRedisClient().Ping() error = %v", err)
		}
	})
}

// TestSessionRegistryUnit tests without Redis
func TestSessionRegistryUnit(t *testing.T) {
	t.Run("Constants", func(t *testing.T) {
		if LockTTL != 30*time.Second {
			t.Errorf("LockTTL = %v, want 30s", LockTTL)
		}
		if LockRefreshInterval != 10*time.Second {
			t.Errorf("LockRefreshInterval = %v, want 10s", LockRefreshInterval)
		}
		if LockRefreshInterval >= LockTTL {
			t.Error("LockRefreshInterval should be less than LockTTL")
		}
	})
}
