package coord

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMutex_MutualExclusion(t *testing.T) {
	lk := NewMemoryLocker()
	ctx := context.Background()

	const goroutines = 20
	const incrsPer = 50
	var counter int
	var mu sync.Mutex // guards counter only to detect races via -race; the
	// distributed Mutex is what actually serializes critical sections.

	var wg sync.WaitGroup
	var maxConcurrent int64
	var inCrit int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := NewMutex(lk, "crit", WithLeaseTTL(2*time.Second))
			for j := 0; j < incrsPer; j++ {
				if err := m.Lock(ctx); err != nil {
					t.Errorf("Lock: %v", err)
					return
				}
				n := atomic.AddInt64(&inCrit, 1)
				if n > atomic.LoadInt64(&maxConcurrent) {
					atomic.StoreInt64(&maxConcurrent, n)
				}
				mu.Lock()
				counter++
				mu.Unlock()
				atomic.AddInt64(&inCrit, -1)
				if err := m.Unlock(ctx); err != nil {
					t.Errorf("Unlock: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if counter != goroutines*incrsPer {
		t.Errorf("counter = %d, want %d", counter, goroutines*incrsPer)
	}
	if maxConcurrent > 1 {
		t.Errorf("observed %d concurrent critical-section holders, want 1", maxConcurrent)
	}
}

func TestMutex_TryLockContended(t *testing.T) {
	lk := NewMemoryLocker()
	ctx := context.Background()

	a := NewMutex(lk, "k", WithLeaseTTL(time.Minute))
	b := NewMutex(lk, "k", WithLeaseTTL(time.Minute))

	ok, err := a.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("a.TryLock ok=%v err=%v", ok, err)
	}
	ok, err = b.TryLock(ctx)
	if err != nil {
		t.Fatalf("b.TryLock err=%v", err)
	}
	if ok {
		t.Fatal("b should not acquire while a holds")
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a.Unlock: %v", err)
	}
	ok, err = b.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("b.TryLock after release ok=%v err=%v", ok, err)
	}
}

func TestLeaderElection_ExactlyOneLeader(t *testing.T) {
	lk := NewMemoryLocker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 5
	var acquireCount int64
	var current int64 // number currently believing they're leader
	var maxLeaders int64

	elections := make([]*LeaderElection, n)
	for i := 0; i < n; i++ {
		le := NewLeaderElection(lk, "leader",
			WithLeaderLeaseTTL(300*time.Millisecond),
			WithLeaderRenewInterval(60*time.Millisecond))
		le.OnAcquire(func(_ context.Context) {
			atomic.AddInt64(&acquireCount, 1)
			n := atomic.AddInt64(&current, 1)
			for {
				m := atomic.LoadInt64(&maxLeaders)
				if n <= m || atomic.CompareAndSwapInt64(&maxLeaders, m, n) {
					break
				}
			}
		})
		le.OnLose(func() { atomic.AddInt64(&current, -1) })
		elections[i] = le
		le.Start(ctx)
	}

	// Let the cluster settle on a leader.
	time.Sleep(400 * time.Millisecond)

	leaders := 0
	for _, le := range elections {
		if le.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("IsLeader() true for %d replicas, want 1", leaders)
	}
	if atomic.LoadInt64(&maxLeaders) > 1 {
		t.Fatalf("max concurrent OnAcquire-active leaders = %d, want 1", maxLeaders)
	}

	for _, le := range elections {
		_ = le.Shutdown(context.Background())
	}
}

func TestLeaderElection_FailoverOnShutdown(t *testing.T) {
	lk := NewMemoryLocker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mk := func() *LeaderElection {
		return NewLeaderElection(lk, "leader",
			WithLeaderLeaseTTL(300*time.Millisecond),
			WithLeaderRenewInterval(50*time.Millisecond))
	}
	a, b := mk(), mk()
	a.Start(ctx)
	time.Sleep(150 * time.Millisecond)
	b.Start(ctx)
	time.Sleep(150 * time.Millisecond)

	// Identify the current leader and shut it down; the other must take over.
	var leader, follower *LeaderElection
	switch {
	case a.IsLeader() && !b.IsLeader():
		leader, follower = a, b
	case b.IsLeader() && !a.IsLeader():
		leader, follower = b, a
	default:
		t.Fatalf("expected exactly one leader before failover (a=%v b=%v)", a.IsLeader(), b.IsLeader())
	}

	if err := leader.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}
	// Follower should acquire within a couple renew intervals (shutdown
	// releases the key eagerly).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !follower.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !follower.IsLeader() {
		t.Fatal("follower did not take over leadership after leader shutdown")
	}
	_ = follower.Shutdown(context.Background())
}

func TestSemaphore_Limit(t *testing.T) {
	lk := NewMemoryLocker()
	ctx := context.Background()
	sem, err := NewSemaphore(lk, "sem", 3)
	if err != nil {
		t.Fatalf("NewSemaphore: %v", err)
	}

	// Acquire up to the limit.
	for i := 0; i < 3; i++ {
		ok, err := sem.TryAcquire(ctx)
		if err != nil || !ok {
			t.Fatalf("acquire %d ok=%v err=%v", i, ok, err)
		}
	}
	// 4th must be refused.
	ok, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("4th acquire err=%v", err)
	}
	if ok {
		t.Fatal("4th acquire should be refused at limit 3")
	}
	// Release one, then a new acquire should succeed.
	if released, err := sem.Release(ctx); err != nil || !released {
		t.Fatalf("release released=%v err=%v", released, err)
	}
	ok, err = sem.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("acquire after release ok=%v err=%v", ok, err)
	}
	// Release floor: releasing below 0 must not apply.
	for i := 0; i < 3; i++ {
		_, _ = sem.Release(ctx)
	}
	released, err := sem.Release(ctx)
	if err != nil {
		t.Fatalf("over-release err=%v", err)
	}
	if released {
		t.Fatal("release below floor 0 should not apply")
	}
}

func TestOnce_SingleWinner(t *testing.T) {
	lk := NewMemoryLocker()
	ctx := context.Background()

	const n = 30
	var runs int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := NewOnce(lk, "init")
			<-start
			ran, err := o.Do(ctx, func(_ context.Context) error {
				atomic.AddInt64(&runs, 1)
				return nil
			})
			if err != nil {
				t.Errorf("Do: %v", err)
			}
			_ = ran
		}()
	}
	close(start)
	wg.Wait()

	if runs != 1 {
		t.Fatalf("fn ran %d times, want exactly 1", runs)
	}
}

func TestOnce_RetryAfterFailure(t *testing.T) {
	lk := NewMemoryLocker()
	ctx := context.Background()

	o := NewOnce(lk, "init")
	ran, err := o.Do(ctx, func(_ context.Context) error {
		return context.Canceled // simulate failure
	})
	if !ran || err == nil {
		t.Fatalf("first Do ran=%v err=%v, want ran=true err!=nil", ran, err)
	}

	// Marker should have been released, so a second attempt can run.
	o2 := NewOnce(lk, "init")
	var ran2 bool
	ran2, err = o2.Do(ctx, func(_ context.Context) error { return nil })
	if !ran2 || err != nil {
		t.Fatalf("retry Do ran=%v err=%v, want ran=true err=nil", ran2, err)
	}
}
