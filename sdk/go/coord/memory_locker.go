package coord

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// MemoryLocker is an in-process implementation of both Locker and Counter,
// honoring TTL at access time. It is intended for tests and for single-process
// coordination where a shared external backend is unnecessary. It is
// goroutine-safe.
type MemoryLocker struct {
	mu sync.Mutex
	m  map[string]memEntry
}

type memEntry struct {
	value     string
	expiresAt time.Time // zero = no expiry
}

// NewMemoryLocker returns an empty in-memory locker/counter.
func NewMemoryLocker() *MemoryLocker {
	return &MemoryLocker{m: make(map[string]memEntry)}
}

// liveLocked returns the live value at key (honoring TTL), deleting it if
// expired. Caller must hold m.mu.
func (l *MemoryLocker) liveLocked(key string, now time.Time) (string, bool) {
	e, ok := l.m[key]
	if !ok {
		return "", false
	}
	if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
		delete(l.m, key)
		return "", false
	}
	return e.value, true
}

func (l *MemoryLocker) setLocked(key, value string, ttl time.Duration) {
	e := memEntry{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	l.m[key] = e
}

// TryAcquire implements Locker.
func (l *MemoryLocker) TryAcquire(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.liveLocked(key, time.Now()); ok {
		return false, nil
	}
	l.setLocked(key, owner, ttl)
	return true, nil
}

// Refresh implements Locker.
func (l *MemoryLocker) Refresh(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.liveLocked(key, time.Now())
	if !ok || cur != owner {
		return false, nil
	}
	l.setLocked(key, owner, ttl)
	return true, nil
}

// Release implements Locker.
func (l *MemoryLocker) Release(_ context.Context, key, owner string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.liveLocked(key, time.Now())
	if !ok || cur != owner {
		return false, nil
	}
	delete(l.m, key)
	return true, nil
}

// Peek implements Locker.
func (l *MemoryLocker) Peek(_ context.Context, key string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, _ := l.liveLocked(key, time.Now())
	return cur, nil
}

// IncrementIf implements Counter.
func (l *MemoryLocker) IncrementIf(_ context.Context, key string, delta, ceiling int64) (int64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := l.counterLocked(key)
	proposed := cur + delta
	if proposed > ceiling {
		return cur, false, nil
	}
	l.m[key] = memEntry{value: strconv.FormatInt(proposed, 10)}
	return proposed, true, nil
}

// DecrementIf implements Counter.
func (l *MemoryLocker) DecrementIf(_ context.Context, key string, delta, floor int64) (int64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := l.counterLocked(key)
	proposed := cur - delta
	if proposed < floor {
		return cur, false, nil
	}
	l.m[key] = memEntry{value: strconv.FormatInt(proposed, 10)}
	return proposed, true, nil
}

func (l *MemoryLocker) counterLocked(key string) int64 {
	cur, ok := l.liveLocked(key, time.Now())
	if !ok || cur == "" {
		return 0
	}
	n, _ := strconv.ParseInt(cur, 10, 64)
	return n
}

// ForceExpire immediately expires key, simulating TTL elapse or holder death.
// Test helper.
func (l *MemoryLocker) ForceExpire(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.m[key]; ok {
		e.expiresAt = time.Now().Add(-time.Second)
		l.m[key] = e
	}
}
