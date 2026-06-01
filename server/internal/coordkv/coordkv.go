// Package coordkv adapts an in-process kv.KVReadWriter to the coord.Locker and
// coord.Counter interfaces, so server-side components can use the shared
// distributed-coordination primitives (Mutex, LeaderElection, Semaphore, Once)
// over whichever KV backend the deployment uses (Redis / Badger / JetStream).
//
// It is the in-process counterpart to the SDK's gRPC coord adapter: the same
// coord algorithms run here against the store directly (no gateway round-trip,
// no ACL — appropriate for trusted infrastructure coordination such as
// workflow leader election).
package coordkv

import (
	"context"
	"errors"
	"time"

	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/sdk/go/coord"
)

// Store is the subset of the KV store API the coordination adapter needs.
// kv.Store (Redis), kv.BadgerKVStore, and kv.JetStreamKVStore all satisfy it
// structurally. Declared locally (rather than importing gateway.KVReadWriter)
// to keep this leaf package free of the gateway dependency.
type Store interface {
	SetNX(ctx context.Context, agent models.Identity, scope kv.KVScope, key, value, userID, workspace string, ttl time.Duration) (bool, error)
	CompareAndSet(ctx context.Context, agent models.Identity, scope kv.KVScope, key, expected, value, userID, workspace string, ttl time.Duration) (bool, error)
	CompareAndDelete(ctx context.Context, agent models.Identity, scope kv.KVScope, key, expected, userID, workspace string) (bool, error)
	Get(ctx context.Context, agent models.Identity, scope kv.KVScope, key, userID, workspace string) (string, error)
	IncrementIf(ctx context.Context, agent models.Identity, scope kv.KVScope, key, userID, workspace string, delta, ceiling int64) (int64, bool, error)
	DecrementIf(ctx context.Context, agent models.Identity, scope kv.KVScope, key, userID, workspace string, delta, floor int64) (int64, bool, error)
}

// Backend binds a KV store to a fixed scope/identity/user/workspace and
// implements coord.Locker and coord.Counter. For infrastructure coordination
// use kv.ScopeGlobal with a zero Identity so all replicas sharing the backend
// rendezvous on the same "kv:global:{key}" namespace.
type Backend struct {
	store     Store
	identity  models.Identity
	scope     kv.KVScope
	userID    string
	workspace string
}

var (
	_ coord.Locker  = (*Backend)(nil)
	_ coord.Counter = (*Backend)(nil)
)

// New returns a coordkv Backend over store bound to the given scope/identity.
func New(store Store, scope kv.KVScope, identity models.Identity, userID, workspace string) *Backend {
	return &Backend{store: store, scope: scope, identity: identity, userID: userID, workspace: workspace}
}

// NewGlobal returns a Backend on the tenant-wide shared global scope (zero
// identity), the typical choice for infrastructure locks.
func NewGlobal(store Store) *Backend {
	return New(store, kv.ScopeGlobal, models.Identity{}, "", "")
}

func (b *Backend) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return b.store.SetNX(ctx, b.identity, b.scope, key, owner, b.userID, b.workspace, ttl)
}

func (b *Backend) Refresh(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return b.store.CompareAndSet(ctx, b.identity, b.scope, key, owner, owner, b.userID, b.workspace, ttl)
}

func (b *Backend) Release(ctx context.Context, key, owner string) (bool, error) {
	return b.store.CompareAndDelete(ctx, b.identity, b.scope, key, owner, b.userID, b.workspace)
}

func (b *Backend) Peek(ctx context.Context, key string) (string, error) {
	val, err := b.store.Get(ctx, b.identity, b.scope, key, b.userID, b.workspace)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

func (b *Backend) IncrementIf(ctx context.Context, key string, delta, ceiling int64) (int64, bool, error) {
	return b.store.IncrementIf(ctx, b.identity, b.scope, key, b.userID, b.workspace, delta, ceiling)
}

func (b *Backend) DecrementIf(ctx context.Context, key string, delta, floor int64) (int64, bool, error) {
	return b.store.DecrementIf(ctx, b.identity, b.scope, key, b.userID, b.workspace, delta, floor)
}
