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
