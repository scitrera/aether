// Package coord provides backend-agnostic distributed coordination primitives
// — Mutex, LeaderElection, Semaphore, and Once — built on a small set of
// atomic conditional-write operations.
//
// The same algorithms run server-side (in-process, over an Aether KV store)
// and client-side (over the gRPC KV protocol) by depending only on the Locker
// and Counter interfaces below. Adapters in the server and SDK bind these
// interfaces to a concrete backend (Redis / Badger / NATS-JetStream) and a KV
// scope; this package contains no backend, network, or scope knowledge.
//
// Lock model: a lock is a KV key holding an opaque owner token with a lease
// TTL. acquire = TryAcquire (set-if-absent), refresh = Refresh
// (compare-and-set owner→owner, extending the TTL), release = Release
// (compare-and-delete). If the holder dies without releasing, the TTL expires
// and the lock becomes acquirable again. Fencing tokens are per-instance
// random IDs (see NewOwnerID).
package coord

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"time"
)

// Locker is the minimal atomic-conditional-write surface a backend must
// provide for Mutex / LeaderElection / Once. Each method maps 1:1 to an Aether
// KV primitive; all composition (reentrancy, renewal, blocking) lives in this
// package, keeping adapters trivial.
type Locker interface {
	// TryAcquire sets key=owner with lease ttl only if key is currently
	// absent. Returns true iff the caller now holds the lock. (Maps to SetNX.)
	TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	// Refresh extends the lease (and re-asserts ownership) iff the current
	// holder is owner. Returns false when the lock has been lost. (Maps to
	// CompareAndSet owner→owner.)
	Refresh(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	// Release frees key iff the current holder is owner. Returns true iff the
	// delete was applied. (Maps to CompareAndDelete.)
	Release(ctx context.Context, key, owner string) (bool, error)
	// Peek returns the current owner of key, or "" when the key is unheld.
	Peek(ctx context.Context, key string) (string, error)
}

// Counter is the guarded-counter surface backing Semaphore. Maps 1:1 to the
// KV IncrementIf / DecrementIf operations.
type Counter interface {
	// IncrementIf adds delta iff the result would not exceed ceiling. Returns
	// the (possibly unchanged) value and whether the mutation applied.
	IncrementIf(ctx context.Context, key string, delta, ceiling int64) (int64, bool, error)
	// DecrementIf subtracts delta iff the result would not drop below floor.
	DecrementIf(ctx context.Context, key string, delta, floor int64) (int64, bool, error)
}

const (
	// DefaultLeaseTTL is the default lock/lease lifetime when none is supplied.
	DefaultLeaseTTL = 30 * time.Second
	// DefaultRenewInterval is the default lease-renewal cadence. It must be
	// comfortably below the lease TTL so a transient backend blip does not lose
	// an otherwise-healthy lock.
	DefaultRenewInterval = 10 * time.Second
)

// NewOwnerID returns a process-unique fencing token of the form
// "<hostname>:<16 hex chars>", stable for the lifetime of the returned string
// and distinct across restarts. Use one owner ID per logical lock holder.
func NewOwnerID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hostname + ":" + hex.EncodeToString(b[:])
}

// sleepCtx sleeps for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
