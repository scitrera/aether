package workflow

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return client, mr
}

func TestLeaderElector_IsLeader_falseBeforeAcquire(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")

	if le.IsLeader() {
		t.Error("IsLeader() = true before TryAcquire, want false")
	}
}

func TestLeaderElector_TryAcquire_succeedsWhenLockFree(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	ctx := context.Background()

	got := le.TryAcquire(ctx)
	if !got {
		t.Error("TryAcquire() = false on free lock, want true")
	}
	if !le.IsLeader() {
		t.Error("IsLeader() = false after successful TryAcquire, want true")
	}
}

func TestLeaderElector_TryAcquire_secondInstanceFailsWhenLockHeld(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le1 := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	le2 := NewRedisLeaderElector(client, "wf:leader:test", "instance-2")
	ctx := context.Background()

	if !le1.TryAcquire(ctx) {
		t.Fatal("le1.TryAcquire() = false, want true")
	}

	got := le2.TryAcquire(ctx)
	if got {
		t.Error("le2.TryAcquire() = true when lock held by le1, want false")
	}
	if le2.IsLeader() {
		t.Error("le2.IsLeader() = true after failed acquire, want false")
	}
}

func TestLeaderElector_TryAcquire_refreshesExistingLock(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	ctx := context.Background()

	if !le.TryAcquire(ctx) {
		t.Fatal("first TryAcquire() = false, want true")
	}
	// Second call should refresh (not drop) the lock
	if !le.TryAcquire(ctx) {
		t.Error("second TryAcquire() = false, want true (refresh)")
	}
	if !le.IsLeader() {
		t.Error("IsLeader() = false after refresh, want true")
	}
}

func TestLeaderElector_Release_clearsLeaderState(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	ctx := context.Background()

	if !le.TryAcquire(ctx) {
		t.Fatal("TryAcquire() = false, want true")
	}
	le.Release(ctx)

	if le.IsLeader() {
		t.Error("IsLeader() = true after Release, want false")
	}
}

func TestLeaderElector_Release_allowsOtherInstanceToAcquire(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le1 := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	le2 := NewRedisLeaderElector(client, "wf:leader:test", "instance-2")
	ctx := context.Background()

	if !le1.TryAcquire(ctx) {
		t.Fatal("le1.TryAcquire() = false")
	}
	le1.Release(ctx)

	if !le2.TryAcquire(ctx) {
		t.Error("le2.TryAcquire() = false after le1 released, want true")
	}
}

func TestLeaderElector_Release_isNoOpWhenNotLeader(t *testing.T) {
	client, _ := newTestRedisClient(t)
	le := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	ctx := context.Background()

	// Should not panic or error when not leader
	le.Release(ctx)
	if le.IsLeader() {
		t.Error("IsLeader() = true after Release on non-leader, want false")
	}
}

func TestLeaderElector_TryAcquire_reacquiresAfterLockExpiry(t *testing.T) {
	client, mr := newTestRedisClient(t)
	le1 := NewRedisLeaderElector(client, "wf:leader:test", "instance-1")
	le2 := NewRedisLeaderElector(client, "wf:leader:test", "instance-2")
	ctx := context.Background()

	if !le1.TryAcquire(ctx) {
		t.Fatal("le1.TryAcquire() = false")
	}

	// Simulate lock TTL expiry via miniredis fast-forward
	mr.FastForward(31 * 1e9) // 31 seconds in nanoseconds

	if !le2.TryAcquire(ctx) {
		t.Error("le2.TryAcquire() = false after le1 lock expired, want true")
	}
}
