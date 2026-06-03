package kv_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scitrera/aether/internal/kv"
)

// JetStream-backed equivalents of the SetAdd/SetCard tests, exercising the
// revision-guarded CAS path against an embedded NATS server.

func TestSetAddJS_NewDuplicateAndCard(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	added, card, err := s.SetAdd(ctx, agent, kv.ScopeGlobal, "set", "m1", "", "", 0)
	if err != nil {
		t.Fatalf("SetAdd m1: %v", err)
	}
	if !added || card != 1 {
		t.Errorf("first add: got added=%v card=%d, want true,1", added, card)
	}

	added, card, err = s.SetAdd(ctx, agent, kv.ScopeGlobal, "set", "m1", "", "", 0)
	if err != nil {
		t.Fatalf("SetAdd dup: %v", err)
	}
	if added || card != 1 {
		t.Errorf("dup add: got added=%v card=%d, want false,1", added, card)
	}

	added, card, err = s.SetAdd(ctx, agent, kv.ScopeGlobal, "set", "m2", "", "", 0)
	if err != nil {
		t.Fatalf("SetAdd m2: %v", err)
	}
	if !added || card != 2 {
		t.Errorf("second add: got added=%v card=%d, want true,2", added, card)
	}

	card, err = s.SetCard(ctx, agent, kv.ScopeGlobal, "set", "", "")
	if err != nil {
		t.Fatalf("SetCard: %v", err)
	}
	if card != 2 {
		t.Errorf("card=%d, want 2", card)
	}

	// Absent set → 0.
	card, err = s.SetCard(ctx, agent, kv.ScopeGlobal, "missing", "", "")
	if err != nil {
		t.Fatalf("SetCard absent: %v", err)
	}
	if card != 0 {
		t.Errorf("absent card=%d, want 0", card)
	}
}

// TestSetAddJS_ConcurrentCompleter verifies the exactly-one-completer property
// holds through the JetStream CAS loop. N is kept modest to stay within the CAS
// retry budget under embedded-server contention.
func TestSetAddJS_ConcurrentCompleter(t *testing.T) {
	s := newTestJSStore(t)
	ctx := context.Background()
	agent := jsAgent()

	const n = 8
	var addedCount, completers atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			added, card, err := s.SetAdd(ctx, agent, kv.ScopeGlobal, "barrier", fmt.Sprintf("m%d", i), "", "", 0)
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
	card, err := s.SetCard(ctx, agent, kv.ScopeGlobal, "barrier", "", "")
	if err != nil {
		t.Fatalf("SetCard: %v", err)
	}
	if card != int64(n) {
		t.Errorf("final card=%d, want %d", card, n)
	}
}
