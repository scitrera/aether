package redisutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client, mr
}

func TestScanKeys_Empty(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	keys, err := ScanKeys(ctx, client, "nonexistent:*")
	if err != nil {
		t.Fatalf("ScanKeys() unexpected error: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("ScanKeys() = %v, want empty slice", keys)
	}
}

func TestScanKeys_ReturnsAllMatches(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	// Insert test keys
	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("test:key:%d", i), "val", 0).Err(); err != nil {
			t.Fatalf("Set() error: %v", err)
		}
	}
	// Insert a key that should NOT match
	if err := client.Set(ctx, "other:key", "val", 0).Err(); err != nil {
		t.Fatalf("Set() error: %v", err)
	}

	keys, err := ScanKeys(ctx, client, "test:key:*")
	if err != nil {
		t.Fatalf("ScanKeys() unexpected error: %v", err)
	}
	if len(keys) != 5 {
		t.Errorf("ScanKeys() returned %d keys, want 5", len(keys))
	}
	for _, k := range keys {
		if len(k) < len("test:key:") || k[:len("test:key:")] != "test:key:" {
			t.Errorf("ScanKeys() returned unexpected key %q", k)
		}
	}
}

func TestScanKeys_PatternFiltering(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	prefixes := []string{"alpha:", "beta:", "gamma:"}
	for _, p := range prefixes {
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("%s%d", p, i)
			if err := client.Set(ctx, key, "v", 0).Err(); err != nil {
				t.Fatalf("Set(%s) error: %v", key, err)
			}
		}
	}

	keys, err := ScanKeys(ctx, client, "beta:*")
	if err != nil {
		t.Fatalf("ScanKeys() unexpected error: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("ScanKeys(beta:*) returned %d keys, want 3", len(keys))
	}
}

func TestScanKeysLimit_CappsResults(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := client.Set(ctx, fmt.Sprintf("limit:key:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set() error: %v", err)
		}
	}

	keys, err := ScanKeysLimit(ctx, client, "limit:key:*", 3)
	if err != nil {
		t.Fatalf("ScanKeysLimit() unexpected error: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("ScanKeysLimit(maxKeys=3) returned %d keys, want exactly 3", len(keys))
	}
}

func TestScanKeysLimit_ZeroMeansAll(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		if err := client.Set(ctx, fmt.Sprintf("unlimited:key:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set() error: %v", err)
		}
	}

	keys, err := ScanKeysLimit(ctx, client, "unlimited:key:*", 0)
	if err != nil {
		t.Fatalf("ScanKeysLimit(maxKeys=0) unexpected error: %v", err)
	}
	if len(keys) != 6 {
		t.Errorf("ScanKeysLimit(maxKeys=0) returned %d keys, want 6", len(keys))
	}
}

func TestScanKeys_ErrorPropagated(t *testing.T) {
	client, mr := newTestRedisClient(t)
	ctx := context.Background()

	// Close the miniredis server to force a connection error
	mr.Close()

	_, err := ScanKeys(ctx, client, "*")
	if err == nil {
		t.Error("ScanKeys() expected error when Redis is unavailable, got nil")
	}
}
