package kv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// These tests run against the Badger backend (no external infrastructure). The
// SetAdd/SetCard contract is backend-agnostic; jetstream-backed equivalents live
// in set_ops_js_test.go.

func TestSetAdd_NewAndDuplicate(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	added, card, err := s.SetAdd(ctx, agent, ScopeGlobal, "set", "m1", "", "", 0)
	if err != nil {
		t.Fatalf("SetAdd m1: %v", err)
	}
	if !added || card != 1 {
		t.Errorf("first add: got added=%v card=%d, want true,1", added, card)
	}

	// Duplicate member: not added, cardinality unchanged.
	added, card, err = s.SetAdd(ctx, agent, ScopeGlobal, "set", "m1", "", "", 0)
	if err != nil {
		t.Fatalf("SetAdd dup: %v", err)
	}
	if added || card != 1 {
		t.Errorf("dup add: got added=%v card=%d, want false,1", added, card)
	}

	// Second distinct member.
	added, card, err = s.SetAdd(ctx, agent, ScopeGlobal, "set", "m2", "", "", 0)
	if err != nil {
		t.Fatalf("SetAdd m2: %v", err)
	}
	if !added || card != 2 {
		t.Errorf("second add: got added=%v card=%d, want true,2", added, card)
	}
}

func TestSetCard_AbsentAndPresent(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	card, err := s.SetCard(ctx, agent, ScopeGlobal, "missing", "", "")
	if err != nil {
		t.Fatalf("SetCard absent: %v", err)
	}
	if card != 0 {
		t.Errorf("absent card=%d, want 0", card)
	}

	for i := 0; i < 3; i++ {
		if _, _, err := s.SetAdd(ctx, agent, ScopeGlobal, "set", fmt.Sprintf("m%d", i), "", "", 0); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	card, err = s.SetCard(ctx, agent, ScopeGlobal, "set", "", "")
	if err != nil {
		t.Fatalf("SetCard present: %v", err)
	}
	if card != 3 {
		t.Errorf("card=%d, want 3", card)
	}
}

// TestSetAdd_ExactlyOneCompleter is the fan-in correctness property: with N
// distinct members added concurrently, every member is added exactly once and
// exactly ONE caller observes the add that brought cardinality to N. That unique
// caller is the join's single firer.
func TestSetAdd_ExactlyOneCompleter(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	const n = 50
	var addedCount, completers atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			added, card, err := s.SetAdd(ctx, agent, ScopeGlobal, "barrier", fmt.Sprintf("m%d", i), "", "", 0)
			if err != nil {
				t.Errorf("SetAdd: %v", err)
				return
			}
			if added {
				addedCount.Add(1)
			}
			if added && card == int64(n) {
				completers.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if addedCount.Load() != int32(n) {
		t.Errorf("addedCount=%d, want %d", addedCount.Load(), n)
	}
	if completers.Load() != 1 {
		t.Errorf("completers=%d, want exactly 1", completers.Load())
	}
	card, err := s.SetCard(ctx, agent, ScopeGlobal, "barrier", "", "")
	if err != nil {
		t.Fatalf("SetCard: %v", err)
	}
	if card != int64(n) {
		t.Errorf("final card=%d, want %d", card, n)
	}
}

// TestSetAdd_DuplicateRace: many concurrent adds of the SAME member yield
// exactly one added==true and a final cardinality of 1 (at-most-once dedup
// ledger semantics).
func TestSetAdd_DuplicateRace(t *testing.T) {
	s := newTestBadgerStore(t)
	ctx := context.Background()
	agent := testAgent()

	const g = 50
	var added atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < g; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := s.SetAdd(ctx, agent, ScopeGlobal, "dedup", "same", "", "", 0)
			if err != nil {
				t.Errorf("SetAdd: %v", err)
				return
			}
			if ok {
				added.Add(1)
			}
		}()
	}
	wg.Wait()

	if added.Load() != 1 {
		t.Errorf("added=%d, want exactly 1", added.Load())
	}
	card, err := s.SetCard(ctx, agent, ScopeGlobal, "dedup", "", "")
	if err != nil {
		t.Fatalf("SetCard: %v", err)
	}
	if card != 1 {
		t.Errorf("card=%d, want 1", card)
	}
}
