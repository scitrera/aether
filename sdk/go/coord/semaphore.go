package coord

import (
	"context"
	"fmt"
	"time"
)

// Semaphore is a distributed counting semaphore over a single KV counter key.
// It admits up to N concurrent permit holders cluster-wide, backed by the
// atomic guarded-counter ops (IncrementIf ceiling=N / DecrementIf floor=0).
//
// Caveat: permits are not leased. A holder that crashes without calling
// Release leaks its permit until an operator resets the counter. For
// crash-safe single-holder exclusion use Mutex (TTL-leased) instead; Semaphore
// fits cooperative concurrency limiting where holders reliably release.
type Semaphore struct {
	counter Counter
	key     string
	limit   int64
}

// NewSemaphore creates an N-permit Semaphore over key. limit must be >= 1.
func NewSemaphore(counter Counter, key string, limit int64) (*Semaphore, error) {
	if limit < 1 {
		return nil, fmt.Errorf("coord: semaphore limit must be >= 1, got %d", limit)
	}
	return &Semaphore{counter: counter, key: key, limit: limit}, nil
}

// TryAcquire takes one permit without blocking. Returns true iff a permit was
// granted (i.e. fewer than N were outstanding).
func (s *Semaphore) TryAcquire(ctx context.Context) (bool, error) {
	_, applied, err := s.counter.IncrementIf(ctx, s.key, 1, s.limit)
	if err != nil {
		return false, err
	}
	return applied, nil
}

// Acquire blocks until a permit is granted or ctx is cancelled. Failed
// attempts are retried after pollInterval (jittered); a non-positive
// pollInterval uses DefaultRenewInterval.
func (s *Semaphore) Acquire(ctx context.Context, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = DefaultRenewInterval
	}
	for {
		ok, err := s.TryAcquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if !sleepCtx(ctx, jitter(pollInterval)) {
			return ctx.Err()
		}
	}
}

// Release returns one permit. Returns true iff a permit was released (the
// counter was above its floor of 0).
func (s *Semaphore) Release(ctx context.Context) (bool, error) {
	_, applied, err := s.counter.DecrementIf(ctx, s.key, 1, 0)
	if err != nil {
		return false, err
	}
	return applied, nil
}
