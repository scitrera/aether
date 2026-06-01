package coord

import (
	"context"
	"sync"
	"time"
)

// LeaderElection races for and holds leadership on a single KV key. Exactly
// one holder leads at a time: leadership is the lock, and the lease TTL ensures
// a crashed leader is replaced after at most ttl. Unlike best-effort
// read-back schemes, acquisition uses an atomic set-if-absent (TryAcquire), so
// two replicas cannot both believe they are leader.
//
// API mirrors the common OnAcquire/OnLose shape so existing callers can adopt
// it without restructuring:
//
//	le := coord.NewLeaderElection(locker, "service::leader")
//	le.OnAcquire(func(ctx context.Context) { startLeaderWork(ctx) })
//	le.OnLose(func() { stopLeaderWork() })
//	le.Start(ctx)
//	defer le.Shutdown(context.Background())
type LeaderElection struct {
	locker        Locker
	key           string
	owner         string
	ttl           time.Duration
	renewInterval time.Duration

	mu        sync.Mutex
	isLeader  bool
	onAcquire []func(ctx context.Context)
	onLose    []func()

	loopCancel context.CancelFunc
	done       chan struct{}
}

// LeaderOption customizes a LeaderElection.
type LeaderOption func(*LeaderElection)

// WithLeaderLeaseTTL sets the leadership lease TTL.
func WithLeaderLeaseTTL(ttl time.Duration) LeaderOption {
	return func(l *LeaderElection) { l.ttl = ttl }
}

// WithLeaderRenewInterval sets the renewal cadence (must be < lease TTL).
func WithLeaderRenewInterval(d time.Duration) LeaderOption {
	return func(l *LeaderElection) { l.renewInterval = d }
}

// WithLeaderOwnerID sets an explicit fencing token (defaults to NewOwnerID).
func WithLeaderOwnerID(owner string) LeaderOption {
	return func(l *LeaderElection) { l.owner = owner }
}

// NewLeaderElection creates a LeaderElection for key backed by locker.
func NewLeaderElection(locker Locker, key string, opts ...LeaderOption) *LeaderElection {
	l := &LeaderElection{
		locker: locker,
		key:    key,
		owner:  NewOwnerID(),
		ttl:    DefaultLeaseTTL,
	}
	for _, o := range opts {
		o(l)
	}
	if l.renewInterval <= 0 {
		l.renewInterval = l.ttl / 3
		if l.renewInterval <= 0 {
			l.renewInterval = DefaultRenewInterval
		}
	}
	return l
}

// OwnerID returns this replica's fencing token.
func (l *LeaderElection) OwnerID() string { return l.owner }

// OnAcquire registers a callback invoked (in its own goroutine) when this
// replica becomes leader. The callback receives a context cancelled when
// leadership is lost or the loop shuts down. Must be called before Start.
func (l *LeaderElection) OnAcquire(fn func(ctx context.Context)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onAcquire = append(l.onAcquire, fn)
}

// OnLose registers a callback invoked synchronously when leadership is lost.
// Must be called before Start.
func (l *LeaderElection) OnLose(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onLose = append(l.onLose, fn)
}

// IsLeader reports the current leadership status. Thread-safe.
func (l *LeaderElection) IsLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isLeader
}

// Start launches the election loop in a background goroutine. Idempotent.
func (l *LeaderElection) Start(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done != nil {
		return
	}
	lctx, cancel := context.WithCancel(ctx)
	l.loopCancel = cancel
	l.done = make(chan struct{})
	go l.electionLoop(lctx)
}

// Shutdown cancels the election loop, fires OnLose if currently leader, and
// best-effort releases the key so the next leader need not wait for TTL
// expiry. Blocks until the loop exits or ctx is cancelled.
func (l *LeaderElection) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	cancel := l.loopCancel
	done := l.done
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Best-effort release so a successor can acquire immediately.
	relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer relCancel()
	_, _ = l.locker.Release(relCtx, l.key, l.owner)
	return nil
}

func (l *LeaderElection) electionLoop(ctx context.Context) {
	defer func() {
		l.mu.Lock()
		wasLeader := l.isLeader
		l.isLeader = false
		onLose := l.onLose
		l.mu.Unlock()
		if wasLeader {
			fireOnLose(onLose)
		}
		close(l.done)
	}()

	for ctx.Err() == nil {
		acquired, err := l.locker.TryAcquire(ctx, l.key, l.owner, l.ttl)
		if err != nil || !acquired {
			// Not leader: wait a renew interval (jittered) and retry. Jitter
			// spreads contender retries so they don't poll in lock-step.
			if !sleepCtx(ctx, jitter(l.renewInterval)) {
				return
			}
			continue
		}

		leaderCtx, leaderCancel := context.WithCancel(ctx)
		l.mu.Lock()
		l.isLeader = true
		onAcquire := l.onAcquire
		l.mu.Unlock()
		fireOnAcquire(leaderCtx, onAcquire)

		// Hold leadership until the lease is lost or ctx is cancelled.
		l.holdLoop(ctx)

		l.mu.Lock()
		l.isLeader = false
		onLose := l.onLose
		l.mu.Unlock()
		leaderCancel()
		fireOnLose(onLose)

		if ctx.Err() != nil {
			return
		}
	}
}

// holdLoop renews leadership every renewInterval until renewal fails or ctx is
// cancelled.
func (l *LeaderElection) holdLoop(ctx context.Context) {
	ticker := time.NewTicker(l.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := l.locker.Refresh(ctx, l.key, l.owner, l.ttl)
			if ctx.Err() != nil {
				return
			}
			if err != nil || !ok {
				return // leadership lost
			}
		}
	}
}

func fireOnAcquire(ctx context.Context, callbacks []func(ctx context.Context)) {
	for _, fn := range callbacks {
		go fn(ctx)
	}
}

func fireOnLose(callbacks []func()) {
	for _, fn := range callbacks {
		fn()
	}
}
