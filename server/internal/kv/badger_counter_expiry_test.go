package kv

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// All counter writes must retain the original expiry, including guarded writes.
// Dropping it turns a per-minute quota into a permanent cumulative limit.
func TestBadgerCountersPreserveExpiry(t *testing.T) {
	for _, operation := range []string{"increment", "decrement", "increment-if", "decrement-if"} {
		t.Run(operation, func(t *testing.T) {
			s := newTestBadgerStore(t)
			ctx, agent := context.Background(), testAgent()
			key := "expiry-regression"
			if err := s.Set(ctx, agent, ScopeGlobal, key, "5", "", "", time.Minute); err != nil {
				t.Fatal(err)
			}
			expiry := func() uint64 {
				var result uint64
				if err := s.db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(s.badgerKey(agent, ScopeGlobal, key, "", ""))
					if err == nil {
						result = item.ExpiresAt()
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return result
			}
			before := expiry()
			if before == 0 {
				t.Fatal("fixture must expire")
			}
			var err error
			switch operation {
			case "increment":
				_, err = s.Increment(ctx, agent, ScopeGlobal, key, "", "")
			case "decrement":
				_, err = s.Decrement(ctx, agent, ScopeGlobal, key, "", "")
			case "increment-if":
				_, _, err = s.IncrementIf(ctx, agent, ScopeGlobal, key, "", "", 1, 10)
			case "decrement-if":
				_, _, err = s.DecrementIf(ctx, agent, ScopeGlobal, key, "", "", 1, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
			if after := expiry(); after != before {
				t.Fatalf("expiry changed from %d to %d", before, after)
			}
		})
	}
}

func TestBadgerIncrementStartsNewWindowAfterExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for Badger second-granularity expiry")
	}
	s := newTestBadgerStore(t)
	ctx, agent := context.Background(), testAgent()
	if err := s.Set(ctx, agent, ScopeGlobal, "window", "0", "", "", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	for want := int64(1); want <= 3; want++ {
		got, err := s.Increment(ctx, agent, ScopeGlobal, "window", "", "")
		if err != nil || got != want {
			t.Fatalf("counter=%d, want=%d, error=%v", got, want, err)
		}
	}
	time.Sleep(2100 * time.Millisecond)
	got, err := s.Increment(ctx, agent, ScopeGlobal, "window", "", "")
	if err != nil || got != 1 {
		t.Fatalf("new window counter=%d, want=1, error=%v", got, err)
	}
}
