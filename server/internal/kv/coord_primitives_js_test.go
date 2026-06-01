package kv_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/kv"
)

// TestJSSetNX_AbsentThenPresent verifies SetNX acquires once and refuses a
// second writer while the (non-expired) entry is present.
func TestJSSetNX_AbsentThenPresent(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	ok, err := s.SetNX(ctx, agent, kv.ScopeGlobal, "lock", "owner-a", "", "", 0)
	if err != nil || !ok {
		t.Fatalf("first SetNX ok=%v err=%v", ok, err)
	}
	ok, err = s.SetNX(ctx, agent, kv.ScopeGlobal, "lock", "owner-b", "", "", 0)
	if err != nil {
		t.Fatalf("second SetNX: %v", err)
	}
	if ok {
		t.Fatal("second SetNX should not acquire while present")
	}
	val, err := s.Get(ctx, agent, kv.ScopeGlobal, "lock", "", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "owner-a" {
		t.Errorf("holder = %q, want owner-a", val)
	}
}

// TestJSSetNX_SoftTTLExpiredReacquire is the critical JetStream-specific path:
// a soft-TTL-expired entry still physically exists, so SetNX must detect the
// logical expiry and Update-over the stale revision rather than failing on
// kv.Create's ErrKeyExists.
func TestJSSetNX_SoftTTLExpiredReacquire(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	ok, err := s.SetNX(ctx, agent, kv.ScopeGlobal, "lock", "owner-a", "", "", 250*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first SetNX ok=%v err=%v", ok, err)
	}
	// Held before expiry.
	ok, _ = s.SetNX(ctx, agent, kv.ScopeGlobal, "lock", "owner-b", "", "", time.Second)
	if ok {
		t.Fatal("should not acquire before soft-TTL expiry")
	}

	time.Sleep(400 * time.Millisecond)

	// After soft-TTL expiry the physical entry lingers, but SetNX must treat
	// it as free and re-acquire.
	ok, err = s.SetNX(ctx, agent, kv.ScopeGlobal, "lock", "owner-b", "", "", time.Second)
	if err != nil {
		t.Fatalf("SetNX after soft-TTL expiry: %v", err)
	}
	if !ok {
		t.Fatal("should re-acquire after soft-TTL expiry")
	}
	val, err := s.Get(ctx, agent, kv.ScopeGlobal, "lock", "", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "owner-b" {
		t.Errorf("holder = %q, want owner-b", val)
	}
}

// TestJSCompareAndSet covers match/mismatch/missing + the refresh idiom.
func TestJSCompareAndSet(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	ok, _ := s.CompareAndSet(ctx, agent, kv.ScopeGlobal, "k", "owner-a", "owner-a", "", "", time.Second)
	if ok {
		t.Fatal("CAS on missing key should not apply")
	}

	if _, err := s.SetNX(ctx, agent, kv.ScopeGlobal, "k", "owner-a", "", "", 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if ok, _ := s.CompareAndSet(ctx, agent, kv.ScopeGlobal, "k", "owner-x", "owner-b", "", "", time.Second); ok {
		t.Fatal("CAS mismatch should not apply")
	}
	if ok, err := s.CompareAndSet(ctx, agent, kv.ScopeGlobal, "k", "owner-a", "owner-b", "", "", time.Second); err != nil || !ok {
		t.Fatalf("CAS match ok=%v err=%v", ok, err)
	}
	val, _ := s.Get(ctx, agent, kv.ScopeGlobal, "k", "", "")
	if val != "owner-b" {
		t.Errorf("value = %q, want owner-b", val)
	}
}

// TestJSCompareAndDelete covers match/mismatch/missing.
func TestJSCompareAndDelete(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	if ok, _ := s.CompareAndDelete(ctx, agent, kv.ScopeGlobal, "k", "owner-a", "", ""); ok {
		t.Fatal("CAD on missing should not apply")
	}
	if _, err := s.SetNX(ctx, agent, kv.ScopeGlobal, "k", "owner-a", "", "", 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if ok, _ := s.CompareAndDelete(ctx, agent, kv.ScopeGlobal, "k", "owner-x", "", ""); ok {
		t.Fatal("CAD mismatch should not apply")
	}
	if ok, err := s.CompareAndDelete(ctx, agent, kv.ScopeGlobal, "k", "owner-a", "", ""); err != nil || !ok {
		t.Fatalf("CAD match ok=%v err=%v", ok, err)
	}
	if _, err := s.Get(ctx, agent, kv.ScopeGlobal, "k", "", ""); err == nil {
		t.Fatal("key should be gone after matched CAD")
	}
}

// TestJSSetNX_ConcurrentSingleWinner asserts exactly one winner under
// contention (revision-CAS mutual exclusion).
func TestJSSetNX_ConcurrentSingleWinner(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	const n = 25
	var winners int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ok, err := s.SetNX(ctx, agent, kv.ScopeGlobal, "race", fmt.Sprintf("owner-%d", i), "", "", time.Minute)
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
