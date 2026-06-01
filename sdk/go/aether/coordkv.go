package aether

import (
	"context"
	"errors"
	"time"

	"github.com/scitrera/aether/sdk/go/coord"
)

// CoordScope binds the coordination primitives to a KV scope and (where the
// scope requires them) a user/workspace, plus an optional per-operation gRPC
// timeout. The zero value targets the global scope with the default KV timeout.
//
// Coordination keys are ordinary KV keys: pick a scope that all participating
// replicas share. Cluster-wide locks typically use KVScopeWorkspace with a
// fixed workspace (or KVScopeGlobal for tenant-wide coordination).
type CoordScope struct {
	Scope     KVScope
	UserID    string
	Workspace string
	// OpTimeout bounds each underlying synchronous KV round-trip. Zero uses
	// DefaultKVTimeout.
	OpTimeout time.Duration
}

func (cs CoordScope) timeout() time.Duration {
	if cs.OpTimeout <= 0 {
		return DefaultKVTimeout
	}
	return cs.OpTimeout
}

// kvCoordBackend adapts *KV to coord.Locker and coord.Counter, binding a fixed
// scope/user/workspace. Each method maps 1:1 to a synchronous KV op.
type kvCoordBackend struct {
	kv    *KV
	scope CoordScope
}

var _ coord.Locker = (*kvCoordBackend)(nil)
var _ coord.Counter = (*kvCoordBackend)(nil)

func (b *kvCoordBackend) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return b.kv.SetNXSync(ctx, key, []byte(owner), b.scope.Scope, b.scope.UserID, b.scope.Workspace, ttl, b.scope.timeout())
}

func (b *kvCoordBackend) Refresh(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	// Re-assert ownership and extend the lease: compare-and-set owner→owner.
	return b.kv.CompareAndSetSync(ctx, key, []byte(owner), []byte(owner), b.scope.Scope, b.scope.UserID, b.scope.Workspace, ttl, b.scope.timeout())
}

func (b *kvCoordBackend) Release(ctx context.Context, key, owner string) (bool, error) {
	return b.kv.CompareAndDeleteSync(ctx, key, []byte(owner), b.scope.Scope, b.scope.UserID, b.scope.Workspace, b.scope.timeout())
}

func (b *kvCoordBackend) Peek(ctx context.Context, key string) (string, error) {
	resp, err := b.kv.GetSync(ctx, KVGetOptions{
		Key:       key,
		Scope:     b.scope.Scope,
		UserID:    b.scope.UserID,
		Workspace: b.scope.Workspace,
		Timeout:   b.scope.timeout(),
	})
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Success {
		return "", nil
	}
	return string(resp.Value), nil
}

func (b *kvCoordBackend) IncrementIf(ctx context.Context, key string, delta, ceiling int64) (int64, bool, error) {
	return b.kv.IncrementIfSync(ctx, key, b.scope.Scope, b.scope.UserID, b.scope.Workspace, delta, ceiling, b.scope.timeout())
}

func (b *kvCoordBackend) DecrementIf(ctx context.Context, key string, delta, floor int64) (int64, bool, error) {
	return b.kv.DecrementIfSync(ctx, key, b.scope.Scope, b.scope.UserID, b.scope.Workspace, delta, floor, b.scope.timeout())
}

// Locker returns a coord.Locker bound to scope, for use with coord.NewMutex /
// coord.NewLeaderElection / coord.NewOnce when finer control than the
// convenience constructors below is needed.
func (kv *KV) Locker(scope CoordScope) coord.Locker {
	return &kvCoordBackend{kv: kv, scope: scope}
}

// Counter returns a coord.Counter bound to scope, for use with
// coord.NewSemaphore.
func (kv *KV) Counter(scope CoordScope) coord.Counter {
	return &kvCoordBackend{kv: kv, scope: scope}
}

// NewMutex returns a distributed Mutex over key in the given scope.
func (kv *KV) NewMutex(key string, scope CoordScope, opts ...coord.MutexOption) *coord.Mutex {
	return coord.NewMutex(kv.Locker(scope), key, opts...)
}

// NewLeaderElection returns a LeaderElection over key in the given scope. Wire
// OnAcquire/OnLose then call Start.
func (kv *KV) NewLeaderElection(key string, scope CoordScope, opts ...coord.LeaderOption) *coord.LeaderElection {
	return coord.NewLeaderElection(kv.Locker(scope), key, opts...)
}

// NewSemaphore returns an N-permit Semaphore over key in the given scope.
func (kv *KV) NewSemaphore(key string, limit int64, scope CoordScope) (*coord.Semaphore, error) {
	if kv == nil {
		return nil, errors.New("aether: nil KV")
	}
	return coord.NewSemaphore(kv.Counter(scope), key, limit)
}

// NewOnce returns a cluster-wide run-once guard over key in the given scope.
func (kv *KV) NewOnce(key string, scope CoordScope, opts ...coord.OnceOption) *coord.Once {
	return coord.NewOnce(kv.Locker(scope), key, opts...)
}
