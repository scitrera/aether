package coord

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrNotHeld is returned by Unlock when the caller does not (or no longer)
// holds the lock.
var ErrNotHeld = errors.New("coord: lock not held")

// Mutex is a distributed mutual-exclusion lock over a single KV key. A held
// Mutex auto-renews its lease in the background until Unlock or until renewal
// fails (e.g. the backend lost the key); a renewal failure is surfaced via the
// channel returned by Lost.
//
// A Mutex is single-holder and not safe for concurrent Lock/Unlock from
// multiple goroutines on the same instance; create one Mutex per goroutine
// that needs the lock (they coordinate through the shared backend key).
type Mutex struct {
	locker        Locker
	key           string
	owner         string
	ttl           time.Duration
	renewInterval time.Duration

	mu         sync.Mutex
	held       bool
	cancelHold context.CancelFunc
	lost       chan struct{}
}

// MutexOption customizes a Mutex.
type MutexOption func(*Mutex)

// WithLeaseTTL sets the lock lease TTL and (proportionally) the renew interval
// when the latter is left at its default.
func WithLeaseTTL(ttl time.Duration) MutexOption {
	return func(m *Mutex) { m.ttl = ttl }
}

// WithRenewInterval overrides the lease-renewal cadence. Must be < lease TTL.
func WithRenewInterval(d time.Duration) MutexOption {
	return func(m *Mutex) { m.renewInterval = d }
}

// WithOwnerID sets an explicit fencing token instead of a generated one.
func WithOwnerID(owner string) MutexOption {
	return func(m *Mutex) { m.owner = owner }
}

// NewMutex creates a Mutex for key backed by locker.
func NewMutex(locker Locker, key string, opts ...MutexOption) *Mutex {
	m := &Mutex{
		locker:        locker,
		key:           key,
		owner:         NewOwnerID(),
		ttl:           DefaultLeaseTTL,
		renewInterval: 0,
	}
	for _, o := range opts {
		o(m)
	}
	if m.renewInterval <= 0 {
		m.renewInterval = m.ttl / 3
		if m.renewInterval <= 0 {
			m.renewInterval = DefaultRenewInterval
		}
	}
	return m
}

// OwnerID returns this Mutex's fencing token.
func (m *Mutex) OwnerID() string { return m.owner }

// TryLock attempts to acquire the lock once without blocking. Returns true iff
// acquired. On success a background renewer keeps the lease alive until Unlock.
func (m *Mutex) TryLock(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held {
		return true, nil
	}
	ok, err := m.locker.TryAcquire(ctx, m.key, m.owner, m.ttl)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	m.startHoldLocked()
	return true, nil
}

// Lock blocks until the lock is acquired or ctx is cancelled. Acquisition
// attempts are spaced by renewInterval with ±25% jitter to avoid lock-step
// thundering herds across contenders.
func (m *Mutex) Lock(ctx context.Context) error {
	for {
		ok, err := m.TryLock(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if !sleepCtx(ctx, jitter(m.renewInterval)) {
			return ctx.Err()
		}
	}
}

// Unlock releases the lock and stops the background renewer. Returns ErrNotHeld
// if the lock was not held (or was already lost).
func (m *Mutex) Unlock(ctx context.Context) error {
	m.mu.Lock()
	if !m.held {
		m.mu.Unlock()
		return ErrNotHeld
	}
	m.stopHoldLocked()
	m.mu.Unlock()

	_, err := m.locker.Release(ctx, m.key, m.owner)
	return err
}

// Lost returns a channel closed when the background renewer determines the
// lock has been lost (renewal failed or the key was taken over). The channel
// is recreated on each successful acquire; capture it after Lock/TryLock.
func (m *Mutex) Lost() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lost
}

// startHoldLocked launches the renewer. Caller must hold m.mu.
func (m *Mutex) startHoldLocked() {
	m.held = true
	m.lost = make(chan struct{})
	holdCtx, cancel := context.WithCancel(context.Background())
	m.cancelHold = cancel
	lost := m.lost
	go m.renewLoop(holdCtx, lost)
}

// stopHoldLocked tears down the renewer. Caller must hold m.mu.
func (m *Mutex) stopHoldLocked() {
	m.held = false
	if m.cancelHold != nil {
		m.cancelHold()
		m.cancelHold = nil
	}
}

// renewLoop refreshes the lease until the hold context is cancelled or a
// refresh fails; on failure it marks the lock lost.
func (m *Mutex) renewLoop(ctx context.Context, lost chan struct{}) {
	ticker := time.NewTicker(m.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := m.locker.Refresh(ctx, m.key, m.owner, m.ttl)
			if ctx.Err() != nil {
				return
			}
			if err != nil || !ok {
				m.mu.Lock()
				if m.held {
					m.held = false
					close(lost)
				}
				m.cancelHold = nil
				m.mu.Unlock()
				return
			}
		}
	}
}

// jitter returns d scaled by a random factor in [0.75, 1.25).
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(float64(d) * (0.75 + rand.Float64()*0.5))
}
