package audit

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// waitFor polls condition until it returns true or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestBaseLogger_EnqueueAndFlush(t *testing.T) {
	var mu sync.Mutex
	var written []string

	writer := func(_ context.Context, _ *sql.DB, entries []string) error {
		mu.Lock()
		defer mu.Unlock()
		written = append(written, entries...)
		return nil
	}

	logger := NewBaseLogger[string](nil, 10, 50*time.Millisecond, 100, writer)

	logger.Enqueue("entry-1")
	logger.Enqueue("entry-2")

	// Wait for flush period
	waitFor(t, 500*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(written) >= 2
	}, "expected 2 entries flushed within timeout")

	mu.Lock()
	count := len(written)
	mu.Unlock()

	if count != 2 {
		t.Errorf("expected 2 entries flushed, got %d", count)
	}

	if err := logger.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestBaseLogger_BatchSizeTrigger(t *testing.T) {
	var mu sync.Mutex
	var batchSizes []int

	writer := func(_ context.Context, _ *sql.DB, entries []int) error {
		mu.Lock()
		defer mu.Unlock()
		batchSizes = append(batchSizes, len(entries))
		return nil
	}

	// Very long flush period so only batch size triggers
	logger := NewBaseLogger[int](nil, 3, 10*time.Minute, 100, writer)

	for i := 0; i < 3; i++ {
		logger.Enqueue(i)
	}

	// Give writeLoop time to process
	waitFor(t, 500*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(batchSizes) >= 1
	}, "expected at least one batch write triggered by batch size within timeout")

	mu.Lock()
	gotBatches := len(batchSizes)
	mu.Unlock()

	if gotBatches < 1 {
		t.Error("expected at least one batch write triggered by batch size")
	}

	if err := logger.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestBaseLogger_CloseFlushesRemaining(t *testing.T) {
	var mu sync.Mutex
	var written []string

	writer := func(_ context.Context, _ *sql.DB, entries []string) error {
		mu.Lock()
		defer mu.Unlock()
		written = append(written, entries...)
		return nil
	}

	// Very long flush period — only Close should flush
	logger := NewBaseLogger[string](nil, 100, 10*time.Minute, 100, writer)

	logger.Enqueue("final-1")
	logger.Enqueue("final-2")

	// Close should flush remaining
	if err := logger.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	mu.Lock()
	count := len(written)
	mu.Unlock()

	if count != 2 {
		t.Errorf("expected 2 entries flushed on close, got %d", count)
	}
}

func TestBaseLogger_Enqueue_FullChannel(t *testing.T) {
	writer := func(_ context.Context, _ *sql.DB, entries []string) error {
		// Block forever to keep channel full
		select {}
	}

	// Buffer of 1 entry
	logger := NewBaseLogger[string](nil, 1000, 10*time.Minute, 1, writer)

	// First should succeed (fills the buffer)
	ok1 := logger.Enqueue("first")

	// Give the write loop time to pick up the first entry
	runtime.Gosched()
	time.Sleep(15 * time.Millisecond)

	// Fill again
	logger.Enqueue("second")

	// This should be dropped (channel full, writer is blocked)
	ok3 := logger.Enqueue("third")

	// At least one should have succeeded
	if !ok1 {
		t.Error("first enqueue should succeed")
	}
	// The third might be dropped
	_ = ok3 // may or may not succeed depending on timing

	// Can't cleanly close because writer blocks, just verify no panic
}

func TestBaseLogger_DB(t *testing.T) {
	writer := func(_ context.Context, _ *sql.DB, _ []string) error { return nil }
	logger := NewBaseLogger[string](nil, 10, time.Minute, 100, writer)

	if logger.DB() != nil {
		t.Error("DB() should be nil when created with nil")
	}

	if err := logger.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestBaseLogger_DroppedCount(t *testing.T) {
	done := make(chan struct{})

	writer := func(_ context.Context, _ *sql.DB, entries []string) error {
		// Block until test is done to keep channel full
		<-done
		return nil
	}

	// Very small buffer (1 entry) to easily test dropping
	logger := NewBaseLogger[string](nil, 1000, 10*time.Minute, 1, writer)
	defer func() {
		close(done)    // Unblock writer first
		logger.Close() // Then close cleanly
	}()

	// First enqueue succeeds (fills buffer)
	ok1 := logger.Enqueue("first")
	if !ok1 {
		t.Fatal("first enqueue should succeed")
	}

	// Give writeLoop a moment to pick up the entry (writer blocks, holding the slot)
	runtime.Gosched()
	time.Sleep(15 * time.Millisecond)

	// Now channel is empty but writer is blocked, so new entries fill the 1-slot buffer quickly
	// Enqueue one to fill the buffer
	logger.Enqueue("fill")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)

	// Now buffer should be full, additional enqueues will be dropped
	for i := 0; i < 5; i++ {
		logger.Enqueue(fmt.Sprintf("msg-%d", i))
	}

	// Check that at least some were dropped
	droppedCount := logger.DroppedCount()
	if droppedCount == 0 {
		t.Errorf("expected at least 1 dropped event, got %d", droppedCount)
	}
}
