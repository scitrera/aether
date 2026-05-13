package quota

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/testutil"
	pkgerrors "github.com/scitrera/aether/pkg/errors"
)

// testRedisClient returns a Redis client for testing, or skips the test if Redis is unavailable.
// Uses testutil.GetRedisAddrs() for consistent dev infrastructure configuration.
func testRedisClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	addrs := testutil.GetRedisAddrs()
	client := redis.NewClient(&redis.Options{
		Addr:        addrs[0],
		DB:          15, // Use DB 15 for tests to avoid collisions
		DialTimeout: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("Redis not available at %s: %v", addrs[0], err)
	}
	t.Cleanup(func() {
		client.FlushDB(context.Background())
		client.Close()
	})
	return client
}

func testDefaults() DefaultQuotas {
	return DefaultQuotas{
		MaxConnectionsPerWorkspace: 5,
		MaxMessageRatePerIdentity:  10,
		MaxKVKeysPerNamespace:      100,
		MaxKVValueSize:             1024,
	}
}

func TestCheckConnectionQuota(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-conn"

	// Should allow connections under limit
	if err := qm.checkConnectionQuota(ctx, workspace); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Fill to limit
	for i := 0; i < 5; i++ {
		if err := qm.incrementConnections(ctx, workspace); err != nil {
			t.Fatalf("increment failed: %v", err)
		}
	}

	// Should reject at limit
	err := qm.checkConnectionQuota(ctx, workspace)
	if err == nil {
		t.Fatal("expected quota exceeded error, got nil")
	}
	qErr, ok := err.(*pkgerrors.QuotaExceededError)
	if !ok {
		t.Fatalf("expected *pkgerrors.QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "connections" {
		t.Errorf("expected resource 'connections', got '%s'", qErr.Resource)
	}
	if qErr.Current != 5 {
		t.Errorf("expected current 5, got %d", qErr.Current)
	}
	if qErr.Limit != 5 {
		t.Errorf("expected limit 5, got %d", qErr.Limit)
	}
}

func TestIncrementDecrementConnections(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-incrdecr"

	// Increment twice
	if err := qm.incrementConnections(ctx, workspace); err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if err := qm.incrementConnections(ctx, workspace); err != nil {
		t.Fatalf("increment failed: %v", err)
	}

	// Verify count is 2
	count, _ := client.Get(ctx, keyPrefixConn+workspace).Int()
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}

	// Decrement
	if err := qm.DecrementConnections(ctx, workspace); err != nil {
		t.Fatalf("decrement failed: %v", err)
	}

	count, _ = client.Get(ctx, keyPrefixConn+workspace).Int()
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	// Decrement below zero clamps to 0
	if err := qm.DecrementConnections(ctx, workspace); err != nil {
		t.Fatalf("decrement failed: %v", err)
	}
	if err := qm.DecrementConnections(ctx, workspace); err != nil {
		t.Fatalf("decrement failed: %v", err)
	}
	count, _ = client.Get(ctx, keyPrefixConn+workspace).Int()
	if count != 0 {
		t.Fatalf("expected count 0 after clamping, got %d", count)
	}
}

func TestCheckAndIncrementConnections(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-atomic"

	// Fill to limit atomically
	for i := 0; i < 5; i++ {
		if err := qm.CheckAndIncrementConnections(ctx, workspace); err != nil {
			t.Fatalf("expected no error on connection %d, got: %v", i+1, err)
		}
	}

	// 6th connection should fail atomically
	err := qm.CheckAndIncrementConnections(ctx, workspace)
	if err == nil {
		t.Fatal("expected quota exceeded error, got nil")
	}
	qErr, ok := err.(*pkgerrors.QuotaExceededError)
	if !ok {
		t.Fatalf("expected *pkgerrors.QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "connections" {
		t.Errorf("expected resource 'connections', got '%s'", qErr.Resource)
	}

	// Verify count did not increment past the limit
	count, _ := client.Get(ctx, keyPrefixConn+workspace).Int()
	if count != 5 {
		t.Errorf("expected count 5 (not incremented past limit), got %d", count)
	}
}

func TestCheckMessageQuota(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-msgrate"
	identity := "ag::test-ws-msgrate::impl::spec"

	// Should allow messages under limit (limit is 10)
	for i := 0; i < 10; i++ {
		if err := qm.CheckMessageQuota(ctx, workspace, identity); err != nil {
			t.Fatalf("expected no error on message %d, got: %v", i+1, err)
		}
	}

	// 11th message should fail
	err := qm.CheckMessageQuota(ctx, workspace, identity)
	if err == nil {
		t.Fatal("expected quota exceeded error, got nil")
	}
	qErr, ok := err.(*pkgerrors.QuotaExceededError)
	if !ok {
		t.Fatalf("expected *pkgerrors.QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "message_rate" {
		t.Errorf("expected resource 'message_rate', got '%s'", qErr.Resource)
	}
}

func TestCheckKVQuota(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-kv"

	// Under limit
	if err := qm.CheckKVQuota(ctx, workspace, "ns1", 50); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// At limit
	err := qm.CheckKVQuota(ctx, workspace, "ns1", 100)
	if err == nil {
		t.Fatal("expected quota exceeded error")
	}
	qErr, ok := err.(*pkgerrors.QuotaExceededError)
	if !ok {
		t.Fatalf("expected *pkgerrors.QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "kv_keys" {
		t.Errorf("expected resource 'kv_keys', got '%s'", qErr.Resource)
	}
}

func TestCheckKVValueSize(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-kvsize"

	// Under limit (1024 bytes)
	if err := qm.CheckKVValueSize(ctx, workspace, 512); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Over limit
	err := qm.CheckKVValueSize(ctx, workspace, 2048)
	if err == nil {
		t.Fatal("expected quota exceeded error")
	}
	qErr, ok := err.(*pkgerrors.QuotaExceededError)
	if !ok {
		t.Fatalf("expected *pkgerrors.QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "kv_value_size" {
		t.Errorf("expected resource 'kv_value_size', got '%s'", qErr.Resource)
	}
}

func TestWorkspaceOverrideTakesPrecedence(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	qm := NewQuotaManager(client, testDefaults())

	workspace := "test-ws-override"

	// Set override with higher connection limit
	err := qm.SetWorkspaceQuota(ctx, workspace, WorkspaceQuota{
		MaxConnections:            10,
		MaxMessageRatePerIdentity: 50,
		MaxKVKeys:                 200,
		MaxKVValueSize:            4096,
	})
	if err != nil {
		t.Fatalf("failed to set workspace quota: %v", err)
	}

	// Fill default limit (5) and verify still allowed (override is 10)
	for i := 0; i < 5; i++ {
		if err := qm.incrementConnections(ctx, workspace); err != nil {
			t.Fatalf("increment failed: %v", err)
		}
	}
	if err := qm.checkConnectionQuota(ctx, workspace); err != nil {
		t.Fatalf("expected no error with override limit 10, got: %v", err)
	}

	// Verify override is returned by GetWorkspaceQuota
	q, err := qm.GetWorkspaceQuota(ctx, workspace)
	if err != nil {
		t.Fatalf("failed to get workspace quota: %v", err)
	}
	if q.MaxConnections != 10 {
		t.Errorf("expected MaxConnections 10, got %d", q.MaxConnections)
	}
	if q.MaxMessageRatePerIdentity != 50 {
		t.Errorf("expected MaxMessageRatePerIdentity 50, got %f", q.MaxMessageRatePerIdentity)
	}
}

func TestGetWorkspaceQuotaDefaultsFallback(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	defaults := testDefaults()
	qm := NewQuotaManager(client, defaults)

	// No override set — should return defaults
	q, err := qm.GetWorkspaceQuota(ctx, "nonexistent-ws")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.MaxConnections != defaults.MaxConnectionsPerWorkspace {
		t.Errorf("expected MaxConnections %d, got %d", defaults.MaxConnectionsPerWorkspace, q.MaxConnections)
	}
	if q.MaxMessageRatePerIdentity != defaults.MaxMessageRatePerIdentity {
		t.Errorf("expected MaxMessageRatePerIdentity %f, got %f", defaults.MaxMessageRatePerIdentity, q.MaxMessageRatePerIdentity)
	}
	if q.MaxKVKeys != defaults.MaxKVKeysPerNamespace {
		t.Errorf("expected MaxKVKeys %d, got %d", defaults.MaxKVKeysPerNamespace, q.MaxKVKeys)
	}
}

func TestQuotaExceededErrorMessage(t *testing.T) {
	err := &pkgerrors.QuotaExceededError{
		Resource:  "connections",
		Workspace: "ws1",
		Current:   10,
		Limit:     5,
	}
	expected := "quota exceeded for connections in workspace 'ws1': current 10, limit 5"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}

	// With identity
	err2 := &pkgerrors.QuotaExceededError{
		Resource:  "message_rate",
		Workspace: "ws1",
		Identity:  "ag::ws1::impl::spec",
		Current:   101,
		Limit:     100,
	}
	if err2.Identity == "" {
		t.Error("expected identity to be set")
	}
	msg := err2.Error()
	if msg == "" {
		t.Error("expected non-empty error message")
	}
}

func TestUnlimitedQuotas(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()

	// Zero defaults = unlimited
	qm := NewQuotaManager(client, DefaultQuotas{})

	if err := qm.checkConnectionQuota(ctx, "ws"); err != nil {
		t.Fatalf("expected no error with unlimited quota, got: %v", err)
	}
	if err := qm.CheckMessageQuota(ctx, "ws", "id"); err != nil {
		t.Fatalf("expected no error with unlimited quota, got: %v", err)
	}
	if err := qm.CheckKVQuota(ctx, "ws", "ns", 999999); err != nil {
		t.Fatalf("expected no error with unlimited quota, got: %v", err)
	}
	if err := qm.CheckKVValueSize(ctx, "ws", 999999999); err != nil {
		t.Fatalf("expected no error with unlimited quota, got: %v", err)
	}
}
