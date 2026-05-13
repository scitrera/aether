package quota

import (
	"context"
	"testing"
)

func TestMemoryQuota_Connections(t *testing.T) {
	ctx := context.Background()
	mgr := NewMemoryQuotaManager(DefaultQuotas{MaxConnectionsPerWorkspace: 2})

	// 1st connection — should be allowed.
	if err := mgr.CheckAndIncrementConnections(ctx, "ws1"); err != nil {
		t.Fatalf("1st connection: unexpected error: %v", err)
	}

	// 2nd connection — should be allowed.
	if err := mgr.CheckAndIncrementConnections(ctx, "ws1"); err != nil {
		t.Fatalf("2nd connection: unexpected error: %v", err)
	}

	// 3rd connection — at limit, should be rejected.
	if err := mgr.CheckAndIncrementConnections(ctx, "ws1"); err == nil {
		t.Error("3rd connection: expected error at limit, got nil")
	}

	// Decrement one — should succeed.
	if err := mgr.DecrementConnections(ctx, "ws1"); err != nil {
		t.Fatalf("DecrementConnections: %v", err)
	}

	// Now a new connection — back under limit.
	if err := mgr.CheckAndIncrementConnections(ctx, "ws1"); err != nil {
		t.Fatalf("connection after decrement: unexpected error: %v", err)
	}
}

func TestMemoryQuota_MessageRate(t *testing.T) {
	ctx := context.Background()
	mgr := NewMemoryQuotaManager(DefaultQuotas{MaxMessageRatePerIdentity: 2})

	// 1st message — allowed.
	if err := mgr.CheckMessageQuota(ctx, "ws1", "id1"); err != nil {
		t.Fatalf("1st message: unexpected error: %v", err)
	}

	// 2nd message — allowed.
	if err := mgr.CheckMessageQuota(ctx, "ws1", "id1"); err != nil {
		t.Fatalf("2nd message: unexpected error: %v", err)
	}

	// 3rd message — rate exceeded.
	if err := mgr.CheckMessageQuota(ctx, "ws1", "id1"); err == nil {
		t.Error("3rd message: expected rate exceeded error, got nil")
	}

	// Different identity — separate counter, should be allowed.
	if err := mgr.CheckMessageQuota(ctx, "ws1", "id2"); err != nil {
		t.Fatalf("different identity: unexpected error: %v", err)
	}
}

func TestMemoryQuota_KVValueSize(t *testing.T) {
	ctx := context.Background()
	mgr := NewMemoryQuotaManager(DefaultQuotas{MaxKVValueSize: 100})

	// Size 50 — allowed.
	if err := mgr.CheckKVValueSize(ctx, "ws1", 50); err != nil {
		t.Fatalf("size 50: unexpected error: %v", err)
	}

	// Size 100 — at limit, allowed.
	if err := mgr.CheckKVValueSize(ctx, "ws1", 100); err != nil {
		t.Fatalf("size 100 (at limit): unexpected error: %v", err)
	}

	// Size 101 — over limit, rejected.
	if err := mgr.CheckKVValueSize(ctx, "ws1", 101); err == nil {
		t.Error("size 101: expected error over limit, got nil")
	}
}
