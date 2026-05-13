package audit

import (
	"context"
	"database/sql"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scitrera/aether/internal/logging"
)

// BatchWriter is a function that writes a batch of entries to the database.
type BatchWriter[T any] func(ctx context.Context, db *sql.DB, entries []T) error

// BaseLogger provides a generic batched async logger.
// It buffers entries in a channel and periodically flushes them in batches.
type BaseLogger[T any] struct {
	db           *sql.DB
	entries      chan T
	batchSize    int
	flushPeriod  time.Duration
	writer       BatchWriter[T]
	wg           sync.WaitGroup
	stopCh       chan struct{}
	closeOnce    sync.Once
	droppedCount atomic.Int64
}

// NewBaseLogger creates a new base logger with the given configuration.
func NewBaseLogger[T any](db *sql.DB, batchSize int, flushPeriod time.Duration, channelBuffer int, writer BatchWriter[T]) *BaseLogger[T] {
	l := &BaseLogger[T]{
		db:          db,
		entries:     make(chan T, channelBuffer),
		batchSize:   batchSize,
		flushPeriod: flushPeriod,
		writer:      writer,
		stopCh:      make(chan struct{}),
	}
	l.wg.Add(1)
	go l.writeLoop()
	return l
}

// Enqueue adds an entry to the write queue (non-blocking).
// Returns false if the channel is full (entry dropped).
func (l *BaseLogger[T]) Enqueue(entry T) bool {
	select {
	case l.entries <- entry:
		return true
	default:
		count := l.droppedCount.Add(1)
		// Log every 10th drop to avoid spam
		if count%10 == 1 {
			logging.Logger.Warn().Int64("total_dropped", count).Msg("audit event dropped (channel full)")
		}
		return false
	}
}

// LogEventSync logs an entry synchronously, blocking if the queue is full.
// Use this for security-critical events (auth failures, ACL denials) that
// must not be dropped.
func (l *BaseLogger[T]) LogEventSync(entry T) {
	l.entries <- entry // blocking send
}

// writeLoop is the background goroutine that batches and writes entries.
func (l *BaseLogger[T]) writeLoop() {
	defer l.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "auditWriteLoop").Msg("recovered from panic in background goroutine")
		}
	}()
	batch := make([]T, 0, l.batchSize)
	ticker := time.NewTicker(l.flushPeriod)
	defer ticker.Stop()

	for {
		select {
		case entry := <-l.entries:
			batch = append(batch, entry)
			if len(batch) >= l.batchSize {
				if err := l.writer(context.Background(), l.db, batch); err != nil {
					logging.Logger.Error().Err(err).Msg("failed to write audit log batch")
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := l.writer(context.Background(), l.db, batch); err != nil {
					logging.Logger.Error().Err(err).Msg("failed to write audit log batch")
				}
				batch = batch[:0]
			}
		case <-l.stopCh:
			// Drain any remaining entries from the channel
			for {
				select {
				case entry := <-l.entries:
					batch = append(batch, entry)
				default:
					goto done
				}
			}
		done:
			if len(batch) > 0 {
				if err := l.writer(context.Background(), l.db, batch); err != nil {
					logging.Logger.Error().Err(err).Msg("failed to write final audit log batch")
				}
			}
			return
		}
	}
}

// DB returns the underlying database connection.
func (l *BaseLogger[T]) DB() *sql.DB {
	return l.db
}

// DroppedCount returns the total number of events dropped due to channel buffer overflow.
func (l *BaseLogger[T]) DroppedCount() int64 {
	return l.droppedCount.Load()
}

// Close stops the logger and flushes remaining entries.
// Safe to call multiple times.
func (l *BaseLogger[T]) Close() error {
	l.closeOnce.Do(func() {
		close(l.stopCh)
		l.wg.Wait()
		close(l.entries)
	})
	return nil
}
