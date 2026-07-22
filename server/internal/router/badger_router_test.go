package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// openTestDB opens a Badger database in a temporary directory for testing.
func openTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir())
	opts.Logger = nil // suppress badger log output in tests
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestBadgerRouter_PublishSubscribe(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()

	var mu sync.Mutex
	var received []string

	unsub, err := r.Subscribe("topic1", func(payload []byte) {
		mu.Lock()
		received = append(received, string(payload))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	if err := r.Publish(context.Background(), "topic1", []byte("hello")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := len(received)
	var first string
	if got > 0 {
		first = received[0]
	}
	mu.Unlock()

	if got != 1 {
		t.Fatalf("expected 1 message, got %d", got)
	}
	if first != "hello" {
		t.Fatalf("expected %q, got %q", "hello", first)
	}

	unsub()
}

func TestBadgerRouter_Replay(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()

	ctx := context.Background()

	// Publish 3 messages before any subscriber exists.
	msgs := []string{"alpha", "beta", "gamma"}
	for _, m := range msgs {
		if err := r.Publish(ctx, "topic1", []byte(m)); err != nil {
			t.Fatalf("Publish(%q) error = %v", m, err)
		}
	}

	var mu sync.Mutex
	var received []string

	// Subscribe with replay — should receive all 3 historical messages.
	unsub, err := r.SubscribeExclusive("topic1", "consumer1", func(payload []byte) {
		mu.Lock()
		received = append(received, string(payload))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeExclusive() error = %v", err)
	}
	defer unsub()

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := make([]string, len(received))
	copy(got, received)
	mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("expected 3 replayed messages, got %d: %v", len(got), got)
	}
	for i, want := range msgs {
		if got[i] != want {
			t.Errorf("message[%d]: want %q, got %q", i, want, got[i])
		}
	}
}

func TestBadgerRouter_ExclusiveReject(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()

	// First exclusive subscriber — must succeed.
	unsub1, err := r.SubscribeExclusive("topic1", "consumer1", func([]byte) {})
	if err != nil {
		t.Fatalf("first SubscribeExclusive() error = %v", err)
	}

	// Second exclusive subscriber with the same name — must fail.
	_, err = r.SubscribeExclusive("topic1", "consumer1", func([]byte) {})
	if err == nil {
		t.Fatal("second SubscribeExclusive() expected error, got nil")
	}

	// Unsubscribe the first one.
	unsub1()

	// Now the same consumer name should be acquirable again.
	unsub2, err := r.SubscribeExclusive("topic1", "consumer1", func([]byte) {})
	if err != nil {
		t.Fatalf("SubscribeExclusive() after unsubscribe error = %v", err)
	}
	unsub2()
}

func TestBadgerRouter_SubscribeExclusiveFromTimestamp_BehavesLikeFullReplay(t *testing.T) {
	// BadgerRouter does not index by timestamp, so SubscribeExclusiveFromTimestamp
	// must behave identically to SubscribeExclusive (full replay from persisted offset
	// or log start). The startTimestampMs argument is ignored.
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()

	ctx := context.Background()

	msgs := []string{"msg1", "msg2", "msg3"}
	for _, m := range msgs {
		if err := r.Publish(ctx, "ts-topic", []byte(m)); err != nil {
			t.Fatalf("Publish(%q) error = %v", m, err)
		}
	}

	var mu sync.Mutex
	var received []string

	// Pass a non-zero timestamp — it should be ignored; full replay must occur.
	unsub, err := r.SubscribeExclusiveFromTimestamp("ts-topic", "ts-consumer", 1_700_000_000_000, func(payload []byte) {
		mu.Lock()
		received = append(received, string(payload))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromTimestamp() error = %v", err)
	}
	defer unsub()

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := make([]string, len(received))
	copy(got, received)
	mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("expected 3 replayed messages (full replay), got %d: %v", len(got), got)
	}
	for i, want := range msgs {
		if got[i] != want {
			t.Errorf("message[%d]: want %q, got %q", i, want, got[i])
		}
	}
}

func TestBadgerRouter_SubscribeFromNow(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()

	ctx := context.Background()

	// Publish 2 messages before the subscriber is created.
	if err := r.Publish(ctx, "topic1", []byte("before1")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := r.Publish(ctx, "topic1", []byte("before2")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	var mu sync.Mutex
	var received []string

	// Subscribe from now — historical messages must be skipped.
	unsub, err := r.SubscribeExclusiveFromNow("topic1", "consumer1", func(payload []byte) {
		mu.Lock()
		received = append(received, string(payload))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromNow() error = %v", err)
	}
	defer unsub()

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	countBeforePublish := len(received)
	mu.Unlock()

	if countBeforePublish != 0 {
		t.Fatalf("expected 0 messages before new publish, got %d", countBeforePublish)
	}

	// Publish 1 more message — subscriber must receive exactly this one.
	if err := r.Publish(ctx, "topic1", []byte("after1")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := make([]string, len(received))
	copy(got, received)
	mu.Unlock()

	if len(got) != 1 {
		t.Fatalf("expected exactly 1 message after publish, got %d: %v", len(got), got)
	}
	if got[0] != "after1" {
		t.Errorf("expected %q, got %q", "after1", got[0])
	}
}

// TestBadgerRouter_ReplayPersistsOffset verifies that catching up via the replay
// path commits the consumer offset — so a named consumer that reconnects does not
// re-replay (and, in production, re-shed) the same historical backlog forever.
// This is the specific failure mode for messages that only ever reach a consumer
// via replay: Publish drops from the live fan-out channel when full but still
// persists to badger, so a slow consumer's messages are served via replay, and
// without an offset save they re-deliver on every reconnect.
func TestBadgerRouter_ReplayPersistsOffset(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()
	ctx := context.Background()

	// Publish 3 messages before any subscriber exists — they land in badger only
	// (no live consumer), so the first subscribe serves them via replay, not drain.
	for _, m := range []string{"a", "b", "c"} {
		if err := r.Publish(ctx, "t", []byte(m)); err != nil {
			t.Fatalf("Publish(%q) error = %v", m, err)
		}
	}

	var mu sync.Mutex
	var first []string
	unsub1, err := r.SubscribeExclusive("t", "c1", func(p []byte) {
		mu.Lock()
		first = append(first, string(p))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("first SubscribeExclusive() error = %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	n1 := len(first)
	mu.Unlock()
	if n1 != 3 {
		t.Fatalf("first subscribe: expected 3 replayed, got %d", n1)
	}
	unsub1()

	// Second subscribe with the SAME consumer name + db: replay must have persisted
	// the offset, so there is nothing left to re-replay.
	var second []string
	unsub2, err := r.SubscribeExclusive("t", "c1", func(p []byte) {
		mu.Lock()
		second = append(second, string(p))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("second SubscribeExclusive() error = %v", err)
	}
	defer unsub2()
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	n2 := len(second)
	got2 := append([]string(nil), second...)
	mu.Unlock()
	if n2 != 0 {
		t.Fatalf("second subscribe: expected 0 re-replayed (offset persisted by replay), got %d: %v", n2, got2)
	}
}

// TestBadgerRouter_ResumeOrTail verifies the resume-or-tail start policy: a
// cold consumer (no committed offset) starts at the tail and does NOT replay the
// retained backlog, while a reconnecting consumer resumes from its committed
// offset and replays only the gap — the fix for shared/broadcast lanes that a
// client re-subscribes on every connect (they previously re-dumped full history).
func TestBadgerRouter_ResumeOrTail(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()
	ctx := context.Background()

	// Retained backlog published before anyone subscribes.
	for _, m := range []string{"old1", "old2", "old3"} {
		if err := r.Publish(ctx, "t", []byte(m)); err != nil {
			t.Fatalf("Publish(%q): %v", m, err)
		}
	}

	var mu sync.Mutex
	var got1 []string
	unsub1, err := r.SubscribeExclusiveResumeOrTail("t", "c1", func(p []byte) {
		mu.Lock()
		got1 = append(got1, string(p))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("cold SubscribeExclusiveResumeOrTail: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	coldReplayed := len(got1)
	mu.Unlock()
	if coldReplayed != 0 {
		t.Fatalf("cold resume-or-tail should start at tail (0 backlog replayed), got %d: %v", coldReplayed, got1)
	}

	// Live messages after subscribe are delivered and advance the offset via drain.
	for _, m := range []string{"live1", "live2"} {
		if err := r.Publish(ctx, "t", []byte(m)); err != nil {
			t.Fatalf("Publish(%q): %v", m, err)
		}
	}
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	got1Copy := append([]string(nil), got1...)
	mu.Unlock()
	if len(got1Copy) != 2 || got1Copy[0] != "live1" || got1Copy[1] != "live2" {
		t.Fatalf("expected live delivery [live1 live2], got %v", got1Copy)
	}
	unsub1()

	// A message published while the consumer is away — the gap to catch up on.
	if err := r.Publish(ctx, "t", []byte("gap1")); err != nil {
		t.Fatalf("Publish(gap1): %v", err)
	}

	// Reconnect: committed offset exists → replay only the gap, not the old
	// backlog and not the already-seen live messages.
	var got2 []string
	unsub2, err := r.SubscribeExclusiveResumeOrTail("t", "c1", func(p []byte) {
		mu.Lock()
		got2 = append(got2, string(p))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("warm SubscribeExclusiveResumeOrTail: %v", err)
	}
	defer unsub2()
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	got2Copy := append([]string(nil), got2...)
	mu.Unlock()
	if len(got2Copy) != 1 || got2Copy[0] != "gap1" {
		t.Fatalf("reconnect should replay only the gap [gap1], got %v", got2Copy)
	}
}

// TestBadgerRouter_SaveOffsetConcurrentNoConflictError verifies saveOffset
// retries on optimistic-concurrency conflict: many concurrent commits to the
// same offset key (drain saving per message + replay's catch-up save under a
// connection storm) must all succeed with no "Transaction Conflict" error.
// Without the retry a conflicting save fails, the offset stalls, and the
// consumer re-replays + re-sheds the same backlog on the next reconnect.
func TestBadgerRouter_SaveOffsetConcurrentNoConflictError(t *testing.T) {
	db := openTestDB(t)
	r := NewBadgerRouter(db)
	defer r.Close()

	const n = 100
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			<-start
			if err := r.saveOffset("hot", "c1", seq); err != nil {
				errs <- err
			}
		}(uint64(i + 1))
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent saveOffset failed (retry-on-conflict missing?): %v", err)
	}

	// A final read-back must return one of the written offsets (a valid commit),
	// proving the key is not left in a wedged/uncommitted state.
	off, err := r.loadOffset("hot", "c1")
	if err != nil {
		t.Fatalf("loadOffset after concurrent saves: %v", err)
	}
	if off < 1 || off > n {
		t.Fatalf("final offset %d out of written range [1,%d]", off, n)
	}
}
