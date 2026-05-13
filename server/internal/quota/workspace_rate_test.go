package quota

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Under-limit pass / at-limit reject
// =============================================================================

func TestWorkspaceRateLimiter_UnderLimit(t *testing.T) {
	t.Parallel()
	// 100 req/sec with burst of 100 — first 100 calls should all pass.
	rl := NewWorkspaceRateLimiter(100)

	for i := 0; i < 100; i++ {
		if !rl.Allow("ws-a") {
			t.Fatalf("request %d should be allowed under burst", i+1)
		}
	}
}

func TestWorkspaceRateLimiter_AtLimit_Rejects(t *testing.T) {
	t.Parallel()
	// Effective rate 1/sec, burst 1. After consuming the single token, the
	// next call should be rejected because tokens refill at one per second.
	rl := NewWorkspaceRateLimiter(1)

	if !rl.Allow("ws-strict") {
		t.Fatal("first request should be allowed (burst token)")
	}
	if rl.Allow("ws-strict") {
		t.Fatal("second immediate request should be rejected at the limit")
	}
}

func TestWorkspaceRateLimiter_ZeroRate_Unlimited(t *testing.T) {
	t.Parallel()
	// A defaultRate of 0 means unlimited — Allow always returns true.
	rl := NewWorkspaceRateLimiter(0)
	for i := 0; i < 1000; i++ {
		if !rl.Allow("ws-unlimited") {
			t.Fatalf("unlimited workspace rejected on call %d", i+1)
		}
	}
}

// =============================================================================
// Sliding-window / token refill (real time-based check)
// =============================================================================

func TestWorkspaceRateLimiter_SlidingWindowRefill(t *testing.T) {
	t.Parallel()
	// 20 tokens per second — drain the burst, then wait long enough for at
	// least one token to refill (~50 ms after exhaustion).
	rl := NewWorkspaceRateLimiter(20)

	// Drain the burst (burst == int(rate) == 20).
	for i := 0; i < 20; i++ {
		if !rl.Allow("ws-window") {
			t.Fatalf("burst request %d should be allowed", i+1)
		}
	}
	// The bucket is now empty; verify the 21st request is rejected.
	if rl.Allow("ws-window") {
		t.Fatal("21st immediate request should be rejected")
	}

	// Wait for refill. At 20 tokens/sec, one token regenerates every 50 ms.
	// Add slack for scheduler jitter.
	time.Sleep(150 * time.Millisecond)

	if !rl.Allow("ws-window") {
		t.Fatal("after refill window, request should be allowed")
	}
}

// =============================================================================
// Per-workspace overrides
// =============================================================================

func TestWorkspaceRateLimiter_SetGetRate(t *testing.T) {
	t.Parallel()
	rl := NewWorkspaceRateLimiter(10)

	// Default rate applies when nothing set.
	if got := rl.GetWorkspaceRate("ws-default"); got != 10 {
		t.Errorf("default rate = %v, want 10", got)
	}

	rl.SetWorkspaceRate("ws-fast", 100)
	if got := rl.GetWorkspaceRate("ws-fast"); got != 100 {
		t.Errorf("set rate = %v, want 100", got)
	}

	// Removing the override reverts to the default.
	rl.RemoveWorkspaceRate("ws-fast")
	if got := rl.GetWorkspaceRate("ws-fast"); got != 10 {
		t.Errorf("after remove, rate = %v, want default 10", got)
	}
}

func TestWorkspaceRateLimiter_SetZeroRevertsToDefault(t *testing.T) {
	t.Parallel()
	rl := NewWorkspaceRateLimiter(50)

	rl.SetWorkspaceRate("ws", 5)
	rl.SetWorkspaceRate("ws", 0) // zero => revert to default
	if got := rl.GetWorkspaceRate("ws"); got != 50 {
		t.Errorf("rate after zero set = %v, want default 50", got)
	}
}

func TestWorkspaceRateLimiter_OverrideAppliesImmediately(t *testing.T) {
	t.Parallel()
	rl := NewWorkspaceRateLimiter(100)

	// Burn through a few default-rate calls.
	for i := 0; i < 5; i++ {
		if !rl.Allow("ws") {
			t.Fatalf("default-rate request %d should pass", i+1)
		}
	}

	// Override to a much tighter rate — limiter should be replaced and a
	// burst of 1 enforced.
	rl.SetWorkspaceRate("ws", 1)
	if !rl.Allow("ws") {
		t.Fatal("first request after override should consume burst token")
	}
	if rl.Allow("ws") {
		t.Fatal("second immediate request after override should be rejected")
	}
}

func TestWorkspaceRateLimiter_ListAndDefaultRate(t *testing.T) {
	t.Parallel()
	rl := NewWorkspaceRateLimiter(7)

	rl.SetWorkspaceRate("ws-a", 10)
	rl.SetWorkspaceRate("ws-b", 20)

	all := rl.ListWorkspaceRates()
	if len(all) != 2 {
		t.Errorf("ListWorkspaceRates length = %d, want 2", len(all))
	}
	if all["ws-a"] != 10 || all["ws-b"] != 20 {
		t.Errorf("ListWorkspaceRates = %v, want {ws-a:10, ws-b:20}", all)
	}

	if got := rl.DefaultRate(); got != 7 {
		t.Errorf("DefaultRate = %v, want 7", got)
	}
}

// =============================================================================
// Separate workspaces — independent buckets
// =============================================================================

func TestWorkspaceRateLimiter_SeparateWorkspaces(t *testing.T) {
	t.Parallel()
	rl := NewWorkspaceRateLimiter(1)

	if !rl.Allow("ws-a") {
		t.Fatal("ws-a first request should pass")
	}
	// ws-b has its own bucket; should also pass.
	if !rl.Allow("ws-b") {
		t.Fatal("ws-b first request should pass (independent bucket)")
	}

	// Both buckets now exhausted.
	if rl.Allow("ws-a") {
		t.Error("ws-a second request should be rejected")
	}
	if rl.Allow("ws-b") {
		t.Error("ws-b second request should be rejected")
	}
}

// =============================================================================
// Concurrent access — race detector should be clean.
// =============================================================================

func TestWorkspaceRateLimiter_ConcurrentAllow(t *testing.T) {
	t.Parallel()

	// Generous rate so most calls pass; the test is about race-cleanliness
	// rather than counting tokens precisely.
	rl := NewWorkspaceRateLimiter(10000)

	const goroutines = 16
	const callsEach = 200

	var wg sync.WaitGroup
	var allowed atomic.Int64
	var rejected atomic.Int64
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < callsEach; i++ {
				if rl.Allow("ws-concurrent") {
					allowed.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	total := allowed.Load() + rejected.Load()
	if total != int64(goroutines*callsEach) {
		t.Errorf("total calls = %d, want %d", total, goroutines*callsEach)
	}
	if allowed.Load() == 0 {
		t.Error("expected at least some calls to be allowed")
	}
}

func TestWorkspaceRateLimiter_ConcurrentSetAndAllow(t *testing.T) {
	t.Parallel()
	// Exercise the write-lock path concurrently with reads — race detector
	// should remain clean.
	rl := NewWorkspaceRateLimiter(100)

	const writers = 4
	const readers = 8
	const iters = 200

	var wg sync.WaitGroup

	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				rate := float64((id*iters + i) % 50)
				rl.SetWorkspaceRate("ws-race", rate)
			}
		}(w)
	}

	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = rl.Allow("ws-race")
				_ = rl.GetWorkspaceRate("ws-race")
				_ = rl.ListWorkspaceRates()
			}
		}()
	}

	wg.Wait()
}
