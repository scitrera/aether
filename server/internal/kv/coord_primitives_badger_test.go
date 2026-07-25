package kv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBadgerSetNX_AbsentThenPresent verifies SetNX writes once and refuses a
// second writer.
func TestBadgerSetNX_AbsentThenPresent(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	ok, err := s.SetNX(ctx, agent, ScopeGlobal, "lock", "owner-a", "", "", 0)
	if err != nil {
		t.Fatalf("SetNX: %v", err)
	}
	if !ok {
		t.Fatal("first SetNX should acquire")
	}

	ok, err = s.SetNX(ctx, agent, ScopeGlobal, "lock", "owner-b", "", "", 0)
	if err != nil {
		t.Fatalf("SetNX 2: %v", err)
	}
	if ok {
		t.Fatal("second SetNX should NOT acquire while key present")
	}

	// Holder unchanged.
	val, err := s.Get(ctx, agent, ScopeGlobal, "lock", "", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "owner-a" {
		t.Errorf("holder = %q, want owner-a", val)
	}
}

// TestBadgerSetNX_TTLExpiryReacquire verifies a SetNX lock with TTL becomes
// re-acquirable after the TTL elapses (Badger honors TTL natively).
//
// NOTE: Badger stores entry expiry as Unix *seconds* (WithTTL truncates to
// second granularity), so this test uses second-scale durations. Sub-second
// lock TTLs are unreliable on the Badger backend; real coordination TTLs are
// always several seconds (workflow/sandbox use 30s), where ±1s rounding is
// harmless.
func TestBadgerSetNX_TTLExpiryReacquire(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing-based TTL test in -short mode")
	}
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	ok, err := s.SetNX(ctx, agent, ScopeGlobal, "lock", "owner-a", "", "", 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("first SetNX ok=%v err=%v", ok, err)
	}
	// Still held immediately.
	ok, _ = s.SetNX(ctx, agent, ScopeGlobal, "lock", "owner-b", "", "", 2*time.Second)
	if ok {
		t.Fatal("should not acquire before TTL expiry")
	}

	// Sleep past the second-truncated expiry boundary.
	time.Sleep(3100 * time.Millisecond)

	ok, err = s.SetNX(ctx, agent, ScopeGlobal, "lock", "owner-b", "", "", 5*time.Second)
	if err != nil {
		t.Fatalf("SetNX after expiry: %v", err)
	}
	if !ok {
		t.Fatal("should re-acquire after TTL expiry")
	}
}

// TestBadgerCompareAndSet covers match, mismatch, and missing-key cases plus
// the refresh idiom (expected == new).
func TestBadgerCompareAndSet(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	// Missing key never matches a non-empty expected.
	ok, err := s.CompareAndSet(ctx, agent, ScopeGlobal, "k", "owner-a", "owner-a", "", "", time.Second)
	if err != nil {
		t.Fatalf("CAS missing: %v", err)
	}
	if ok {
		t.Fatal("CAS on missing key should not apply")
	}

	if _, err := s.SetNX(ctx, agent, ScopeGlobal, "k", "owner-a", "", "", 0); err != nil {
		t.Fatalf("seed SetNX: %v", err)
	}

	// Mismatch.
	ok, _ = s.CompareAndSet(ctx, agent, ScopeGlobal, "k", "owner-x", "owner-b", "", "", time.Second)
	if ok {
		t.Fatal("CAS mismatch should not apply")
	}

	// Match (refresh idiom: expected == new value).
	ok, err = s.CompareAndSet(ctx, agent, ScopeGlobal, "k", "owner-a", "owner-a", "", "", time.Second)
	if err != nil {
		t.Fatalf("CAS match: %v", err)
	}
	if !ok {
		t.Fatal("CAS match should apply")
	}

	// Match with new owner (handoff).
	ok, _ = s.CompareAndSet(ctx, agent, ScopeGlobal, "k", "owner-a", "owner-b", "", "", time.Second)
	if !ok {
		t.Fatal("CAS handoff should apply")
	}
	val, _ := s.Get(ctx, agent, ScopeGlobal, "k", "", "")
	if val != "owner-b" {
		t.Errorf("value = %q, want owner-b", val)
	}
}

// TestBadgerCompareAndDelete covers match, mismatch, and missing cases.
func TestBadgerCompareAndDelete(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	// Missing.
	ok, err := s.CompareAndDelete(ctx, agent, ScopeGlobal, "k", "owner-a", "", "")
	if err != nil {
		t.Fatalf("CAD missing: %v", err)
	}
	if ok {
		t.Fatal("CAD on missing key should not apply")
	}

	if _, err := s.SetNX(ctx, agent, ScopeGlobal, "k", "owner-a", "", "", 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Mismatch leaves the key intact.
	ok, _ = s.CompareAndDelete(ctx, agent, ScopeGlobal, "k", "owner-x", "", "")
	if ok {
		t.Fatal("CAD mismatch should not apply")
	}
	if _, err := s.Get(ctx, agent, ScopeGlobal, "k", "", ""); err != nil {
		t.Fatalf("key should still exist after mismatched CAD: %v", err)
	}

	// Match deletes.
	ok, err = s.CompareAndDelete(ctx, agent, ScopeGlobal, "k", "owner-a", "", "")
	if err != nil {
		t.Fatalf("CAD match: %v", err)
	}
	if !ok {
		t.Fatal("CAD match should apply")
	}
	if _, err := s.Get(ctx, agent, ScopeGlobal, "k", "", ""); err == nil {
		t.Fatal("key should be gone after matched CAD")
	}
}

// TestBadgerSetNX_ConcurrentSingleWinner asserts that under N concurrent
// SetNX attempts exactly one acquires the lock — the core mutual-exclusion
// guarantee.
func TestBadgerSetNX_ConcurrentSingleWinner(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	const n = 50
	var winners int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ok, err := s.SetNX(ctx, agent, ScopeGlobal, "race", fmt.Sprintf("owner-%d", i), "", "", time.Minute)
			if err != nil {
				t.Errorf("SetNX: %v", err)
				return
			}
			if ok {
				atomic.AddInt64(&winners, 1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Fatalf("expected exactly 1 SetNX winner, got %d", winners)
	}
}

// TestBadgerIncrement_ConcurrentSameKey verifies the counter increment retries
// on optimistic-concurrency conflict: many concurrent increments of the same hot
// key (e.g. a per-user ratelimit counter under a connection storm) must all
// succeed with no lost updates and no "failed to modify counter: Transaction
// Conflict" errors. Without the retry, concurrent increments fail on ErrConflict.
func TestBadgerIncrement_ConcurrentSameKey(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	const n = 100
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := s.Increment(ctx, agent, ScopeGlobal, "hot", "", ""); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Increment failed (retry-on-conflict missing?): %v", err)
	}

	// Final value must equal the number of successful increments — no lost
	// updates. One extra increment reads back the committed total.
	got, err := s.Increment(ctx, agent, ScopeGlobal, "hot", "", "")
	if err != nil {
		t.Fatalf("final Increment: %v", err)
	}
	if got != n+1 {
		t.Fatalf("expected counter == %d after %d concurrent + 1 increments, got %d", n+1, n, got)
	}
}
