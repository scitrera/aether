// Package lite provides in-process backend implementations for AetherLite mode.
// All subsystems share a single Badger database instance with key-prefix namespacing.
package lite

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
)

// Key prefixes for subsystem isolation within the shared Badger instance.
const (
	PrefixSession    = "sess:"
	PrefixKV         = "kv:"
	PrefixCheckpoint = "ckpt:"
	PrefixToken      = "tok:"
	PrefixMessage    = "msg:"
	PrefixOffset     = "off:"
	PrefixSequence   = "seq:"
)

// OpenBadger opens a Badger database at the given directory.
// If dir is empty, an in-memory database is used.
func OpenBadger(dir string) (*badger.DB, error) {
	opts := badger.DefaultOptions(dir)
	if dir == "" {
		opts = opts.WithInMemory(true)
	}
	opts = opts.WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// RunGC starts a background goroutine that periodically runs Badger's value log GC.
// It stops when the context is cancelled.
func RunGC(ctx context.Context, db *badger.DB, interval time.Duration) {
	if interval == 0 {
		interval = 5 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for {
					err := db.RunValueLogGC(0.5)
					if err != nil {
						break // no more GC needed
					}
					log.Debug().Msg("badger value log GC cycle completed")
				}
			}
		}
	}()
}
