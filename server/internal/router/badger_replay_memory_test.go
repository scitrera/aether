package router

import (
	"bytes"
	"expvar"
	"runtime"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	badgery "github.com/dgraph-io/badger/v4/y"
)

func TestBadgerReplayDoesNotReadAheadOfHandler(t *testing.T) {
	// Value-log counters are process-wide; do not run in parallel.
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueThreshold(1024))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	router := NewBadgerRouter(db)
	payload := bytes.Repeat([]byte("x"), 64*1024)
	for i := 0; i < 128; i++ {
		if _, err := router.appendMessage("large", payload); err != nil {
			t.Fatal(err)
		}
	}
	reads := expvar.Get(badgery.BADGER_METRIC_PREFIX + "read_num_vlog").(*expvar.Int)
	before := reads.Value()
	var last uint64
	delivered := 0
	err = router.replay("large", "", 0, func(value []byte) {
		delivered++
		if !bytes.Equal(value, payload) {
			t.Error("incorrect replay payload")
		}
		if delivered == 1 {
			// Model a slow consumer; let any speculative reads finish.
			time.Sleep(100 * time.Millisecond)
			if delta := reads.Value() - before; delta != 1 {
				t.Errorf("blocked handler read %d values; want exactly 1", delta)
			}
		}
	}, &last)
	if err != nil || delivered != 128 || last != 128 {
		t.Fatalf("replay: err=%v delivered=%d last=%d", err, delivered, last)
	}
}

// Run with -bench=BenchmarkBadgerReplayLargePayloads -benchmem -benchtime=3x.
// Synthetic on-disk payloads exercise the real value-log allocation path.
func BenchmarkBadgerReplayLargePayloads(b *testing.B) {
	db, err := badger.Open(badger.DefaultOptions(b.TempDir()).WithLogger(nil).WithValueThreshold(1024))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	router := NewBadgerRouter(db)
	const messages = 128
	payload := bytes.Repeat([]byte("x"), 1024*1024)
	for i := 0; i < messages; i++ {
		if _, err := router.appendMessage("large", payload); err != nil {
			b.Fatal(err)
		}
	}
	runtime.GC()
	b.ReportAllocs()
	b.SetBytes(messages * int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var last uint64
		count := 0
		if err := router.replay("large", "", 0, func(value []byte) {
			count++
			if len(value) != len(payload) || value[0] != 'x' {
				b.Fatal("incorrect payload")
			}
		}, &last); err != nil || count != messages || last != messages {
			b.Fatalf("replay: err=%v count=%d last=%d", err, count, last)
		}
	}
}
