package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/scitrera/aether/internal/coordkv"
	"github.com/scitrera/aether/internal/kv"
)

func newTestRedisClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return client, mr
}

// newTestLocker returns a coordkv backend over a miniredis-backed KV store,
// exercising the real Redis SetNX/CompareAndSet/CompareAndDelete primitives.
func newTestLocker(t *testing.T) *coordkv.Backend {
	t.Helper()
	client, _ := newTestRedisClient(t)
	return coordkv.NewGlobal(kv.NewStoreFromClient(client))
}

func TestCoordkvBackend_SetNX_MutualExclusion(t *testing.T) {
	b := newTestLocker(t)
	ctx := context.Background()

	ok, err := b.TryAcquire(ctx, "workflow:leader", "owner-a", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("first TryAcquire ok=%v err=%v", ok, err)
	}
	ok, err = b.TryAcquire(ctx, "workflow:leader", "owner-b", 30*time.Second)
	if err != nil {
		t.Fatalf("second TryAcquire err=%v", err)
	}
	if ok {
		t.Fatal("second TryAcquire should fail while held")
	}
	holder, err := b.Peek(ctx, "workflow:leader")
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if holder != "owner-a" {
		t.Errorf("holder = %q, want owner-a", holder)
	}
}

func TestCoordkvBackend_RefreshAndRelease(t *testing.T) {
	b := newTestLocker(t)
	ctx := context.Background()

	if ok, _ := b.TryAcquire(ctx, "k", "owner-a", 30*time.Second); !ok {
		t.Fatal("acquire failed")
	}
	// Owner can refresh.
	if ok, err := b.Refresh(ctx, "k", "owner-a", 30*time.Second); err != nil || !ok {
		t.Fatalf("Refresh ok=%v err=%v", ok, err)
	}
	// A non-owner cannot refresh.
	if ok, _ := b.Refresh(ctx, "k", "owner-b", 30*time.Second); ok {
		t.Fatal("non-owner Refresh should fail")
	}
	// A non-owner cannot release.
	if ok, _ := b.Release(ctx, "k", "owner-b"); ok {
		t.Fatal("non-owner Release should fail")
	}
	// Owner releases; key becomes free.
	if ok, err := b.Release(ctx, "k", "owner-a"); err != nil || !ok {
		t.Fatalf("Release ok=%v err=%v", ok, err)
	}
	if ok, _ := b.TryAcquire(ctx, "k", "owner-b", 30*time.Second); !ok {
		t.Fatal("re-acquire after release should succeed")
	}
}

func TestCoordkvBackend_LeaseExpiryReacquire(t *testing.T) {
	client, mr := newTestRedisClient(t)
	b := coordkv.NewGlobal(kv.NewStoreFromClient(client))
	ctx := context.Background()

	if ok, _ := b.TryAcquire(ctx, "k", "owner-a", 30*time.Second); !ok {
		t.Fatal("acquire failed")
	}
	// Simulate lease TTL expiry.
	mr.FastForward(31 * time.Second)
	if ok, err := b.TryAcquire(ctx, "k", "owner-b", 30*time.Second); err != nil || !ok {
		t.Fatalf("re-acquire after lease expiry ok=%v err=%v", ok, err)
	}
}

func TestCoordLeaderElector_SingleLeaderAndFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing-based leader failover test in -short mode")
	}
	// Shared backend across two electors (same miniredis).
	client, _ := newTestRedisClient(t)
	mk := func(id string) LeaderElector {
		locker := coordkv.NewGlobal(kv.NewStoreFromClient(client))
		// Short lease for a fast test.
		return NewCoordLeaderElector(locker, "workflow:leader", id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := mk("instance-a")
	a.Start(ctx)

	// a should win leadership.
	if !waitFor(2*time.Second, a.IsLeader) {
		t.Fatal("instance-a did not become leader")
	}

	b := mk("instance-b")
	b.Start(ctx)
	// b must NOT become leader while a holds it.
	time.Sleep(300 * time.Millisecond)
	if b.IsLeader() {
		t.Fatal("instance-b became leader while instance-a holds leadership")
	}

	// a steps down; b should take over (eager release on shutdown).
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatalf("a.Shutdown: %v", err)
	}
	if !waitFor(workflowLeaderTTL+5*time.Second, b.IsLeader) {
		t.Fatal("instance-b did not take over after instance-a shutdown")
	}
	_ = b.Shutdown(context.Background())
}

func TestSingleNodeLeaderElector_AlwaysLeader(t *testing.T) {
	le := NewSingleNodeLeaderElector()
	le.Start(context.Background())
	if !le.IsLeader() {
		t.Error("single-node elector should always be leader")
	}
	if err := le.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
