package kv

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/pkg/models"
)

func newTestBadgerStore(t *testing.T) *BadgerKVStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "kv-if-")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	opts := badger.DefaultOptions(filepath.Join(dir, "db"))
	opts.Logger = nil
	opts.SyncWrites = false
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewBadgerKVStore(db)
}

func testAgent() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "production",
		Implementation: "python-worker",
		Specifier:      "instance-1",
	}
}

func TestIncrementIf_BelowCeiling(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()
	val, applied, err := s.IncrementIf(ctx, agent, ScopeGlobal, "counter", "", "", 5, 10)
	if err != nil {
		t.Fatalf("IncrementIf: %v", err)
	}
	if !applied || val != 5 {
		t.Errorf("got applied=%v val=%d, want applied=true val=5", applied, val)
	}
}

func TestIncrementIf_AtCeiling(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	// Seed to 9 with a permitted increment.
	if _, _, err := s.IncrementIf(ctx, agent, ScopeGlobal, "counter", "", "", 9, 10); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Next +2 would push to 11 > ceiling 10 → reject.
	val, applied, err := s.IncrementIf(ctx, agent, ScopeGlobal, "counter", "", "", 2, 10)
	if err != nil {
		t.Fatalf("IncrementIf rejected: %v", err)
	}
	if applied || val != 9 {
		t.Errorf("got applied=%v val=%d, want applied=false val=9 (unchanged)", applied, val)
	}
	// +1 to exactly the ceiling → permitted.
	val, applied, err = s.IncrementIf(ctx, agent, ScopeGlobal, "counter", "", "", 1, 10)
	if err != nil {
		t.Fatalf("IncrementIf at ceiling: %v", err)
	}
	if !applied || val != 10 {
		t.Errorf("got applied=%v val=%d, want applied=true val=10", applied, val)
	}
}

func TestDecrementIf_AboveFloor(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()
	if _, _, err := s.IncrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 100, 1000); err != nil {
		t.Fatalf("seed: %v", err)
	}
	val, applied, err := s.DecrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 25, 0)
	if err != nil {
		t.Fatalf("DecrementIf: %v", err)
	}
	if !applied || val != 75 {
		t.Errorf("got applied=%v val=%d, want applied=true val=75", applied, val)
	}
}

func TestDecrementIf_AtFloor(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()
	if _, _, err := s.IncrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 5, 1000); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Try to over-decrement: -10 would go below floor 0.
	val, applied, err := s.DecrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 10, 0)
	if err != nil {
		t.Fatalf("DecrementIf rejected: %v", err)
	}
	if applied || val != 5 {
		t.Errorf("got applied=%v val=%d, want applied=false val=5 (unchanged)", applied, val)
	}
	// Decrement to exactly the floor → permitted.
	val, applied, err = s.DecrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 5, 0)
	if err != nil {
		t.Fatalf("DecrementIf at floor: %v", err)
	}
	if !applied || val != 0 {
		t.Errorf("got applied=%v val=%d, want applied=true val=0", applied, val)
	}
}

// TestDecrementIf_ConcurrentRace fires N concurrent guarded decrements
// against a balance of 100 and asserts the total number of "applied"
// responses matches exactly the number that fit above floor 0. This
// catches races: a non-atomic implementation would over-debit.
func TestDecrementIf_ConcurrentRace(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()
	if _, _, err := s.IncrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 100, 10000); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const goroutines = 50
	const debit = 3
	// 50 * 3 = 150 desired debits. Floor 0 means at most floor(100/3)=33 applies.
	var applied atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := s.DecrementIf(ctx, agent, ScopeGlobal, "balance", "", "", debit, 0)
			if err != nil {
				t.Errorf("DecrementIf: %v", err)
				return
			}
			if ok {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()

	final, _, err := s.DecrementIf(ctx, agent, ScopeGlobal, "balance", "", "", 0, 0)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	expectedApplied := int32(33)
	if applied.Load() != expectedApplied {
		t.Errorf("applied=%d, want %d", applied.Load(), expectedApplied)
	}
	expectedFinal := int64(100 - int64(expectedApplied)*debit) // 100 - 99 = 1
	if final != expectedFinal {
		t.Errorf("final balance = %d, want %d", final, expectedFinal)
	}
}

// TestIncrementIf_RejectsNegativeDelta confirms callers can't slip a
// negative delta past IncrementIf to bypass the guarded-decrement path.
func TestIncrementIf_RejectsNegativeDelta(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()
	if _, _, err := s.IncrementIf(ctx, agent, ScopeGlobal, "k", "", "", -1, 100); err == nil {
		t.Error("expected error for negative delta on IncrementIf")
	}
	if _, _, err := s.DecrementIf(ctx, agent, ScopeGlobal, "k", "", "", -1, 0); err == nil {
		t.Error("expected error for negative delta on DecrementIf")
	}
}
