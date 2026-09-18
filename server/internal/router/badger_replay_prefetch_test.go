package router

import (
	"bytes"
	"context"
	"expvar"
	"testing"

	"github.com/dgraph-io/badger/v4"
	badgery "github.com/dgraph-io/badger/v4/y"
)

func TestBadgerRouterReplayDoesNotPrefetchAdjacentTopics(t *testing.T) {
	// Badger's value-log counters are process-wide. Keep this test sequential.
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueThreshold(1024)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	router := NewBadgerRouter(db)
	t.Cleanup(func() { router.Close() })
	if err := router.Publish(context.Background(), "a", []byte("target-message")); err != nil {
		t.Fatal(err)
	}
	// These adjacent values live in the value log. The requested topic's small
	// inline value does not, so replay must perform no value-log reads at all.
	unrelated := bytes.Repeat([]byte("x"), 64*1024)
	for i := 0; i < 64; i++ {
		if err := router.Publish(context.Background(), "z", unrelated); err != nil {
			t.Fatal(err)
		}
	}
	reads, ok := expvar.Get(badgery.BADGER_METRIC_PREFIX + "read_num_vlog").(*expvar.Int)
	if !ok {
		t.Fatal("Badger value-log read metric unavailable")
	}
	for _, tc := range []struct {
		topic string
		want  int
		last  uint64
	}{
		{"a", 1, 1}, {"b", 0, 0},
	} {
		t.Run(tc.topic, func(t *testing.T) {
			before := reads.Value()
			var last uint64
			delivered := 0
			err := router.replay(tc.topic, "", 0, func(value []byte) {
				delivered++
				if string(value) != "target-message" {
					t.Errorf("unexpected replay payload")
				}
			}, &last)
			if err != nil {
				t.Fatal(err)
			}
			if delivered != tc.want || last != tc.last {
				t.Fatalf("delivery changed: count=%d sequence=%d", delivered, last)
			}
			if delta := reads.Value() - before; delta != 0 {
				t.Fatalf("replay prefetched %d unrelated value-log entries", delta)
			}
		})
	}
}
