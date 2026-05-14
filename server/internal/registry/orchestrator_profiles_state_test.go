package registry

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func newTestBadger(t *testing.T) *badger.DB {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "badger")
	opts := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestBadgerProfileStateStore_IncrSequential(t *testing.T) {
	store := NewBadgerProfileStateStore(newTestBadger(t))
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		got, err := store.Incr(ctx, "rr:foo")
		if err != nil {
			t.Fatalf("Incr: %v", err)
		}
		if got != i {
			t.Fatalf("Incr #%d: want %d, got %d", i, i, got)
		}
	}
}

func TestBadgerProfileStateStore_IncrIndependentKeys(t *testing.T) {
	store := NewBadgerProfileStateStore(newTestBadger(t))
	ctx := context.Background()
	if v, err := store.Incr(ctx, "a"); err != nil || v != 1 {
		t.Fatalf("a -> 1: got %d err %v", v, err)
	}
	if v, err := store.Incr(ctx, "b"); err != nil || v != 1 {
		t.Fatalf("b -> 1: got %d err %v", v, err)
	}
	if v, err := store.Incr(ctx, "a"); err != nil || v != 2 {
		t.Fatalf("a -> 2: got %d err %v", v, err)
	}
}

// TestBadgerProfileStateStore_IncrConcurrent verifies the retry-on-conflict
// loop produces correct totals under heavy contention on a single key.
func TestBadgerProfileStateStore_IncrConcurrent(t *testing.T) {
	store := NewBadgerProfileStateStore(newTestBadger(t))
	ctx := context.Background()
	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := store.Incr(ctx, "rr:hot"); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent Incr error: %v", err)
	}
	final, err := store.Incr(ctx, "rr:hot")
	if err != nil {
		t.Fatalf("final Incr: %v", err)
	}
	if final != goroutines+1 {
		t.Fatalf("final counter: want %d, got %d", goroutines+1, final)
	}
}
