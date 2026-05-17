package natscodec

import (
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// defaultCacheCapacity is the per-shard LRU capacity. Small enough to be
// memory-cheap (~64KB worst case for very long identity strings), large
// enough to cover the realistic active session set of one gateway process.
const defaultCacheCapacity = 1024

// escapeCache is a sharded LRU around hashicorp/golang-lru/v2.
//
// Each Escape* variant gets its own shard so a hot subject path cannot evict
// consumer-name entries (and vice versa). Hit/miss counters are exposed via
// stats() for use by tests (see TestCache_HitOnRepeatedInput).
type escapeCache struct {
	mu       sync.RWMutex
	lru      *lru.Cache[string, string]
	capacity int
	hits     atomic.Uint64
	misses   atomic.Uint64
}

func newEscapeCache(capacity int) *escapeCache {
	c, err := lru.New[string, string](capacity)
	if err != nil {
		// lru.New only errors when capacity <= 0, which we control internally.
		panic("natscodec: invalid cache capacity: " + err.Error())
	}
	return &escapeCache{lru: c, capacity: capacity}
}

func (c *escapeCache) get(k string) (string, bool) {
	c.mu.RLock()
	v, ok := c.lru.Get(k)
	c.mu.RUnlock()
	if ok {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return v, ok
}

func (c *escapeCache) add(k, v string) {
	c.mu.Lock()
	c.lru.Add(k, v)
	c.mu.Unlock()
}

// resize replaces the underlying LRU with a new one of the requested capacity.
// All previously-cached entries are discarded. Safe for concurrent use.
func (c *escapeCache) resize(capacity int) {
	if capacity <= 0 {
		capacity = defaultCacheCapacity
	}
	nc, err := lru.New[string, string](capacity)
	if err != nil {
		panic("natscodec: invalid cache capacity: " + err.Error())
	}
	c.mu.Lock()
	c.lru = nc
	c.capacity = capacity
	c.mu.Unlock()
}

// stats returns (hits, misses) atomically. Used by tests only.
func (c *escapeCache) stats() (uint64, uint64) {
	return c.hits.Load(), c.misses.Load()
}

// resetStats zeros the hit/miss counters. Test helper only.
func (c *escapeCache) resetStats() {
	c.hits.Store(0)
	c.misses.Store(0)
}

var (
	subjectCache      = newEscapeCache(defaultCacheCapacity)
	kvKeyCache        = newEscapeCache(defaultCacheCapacity)
	consumerNameCache = newEscapeCache(defaultCacheCapacity)
)

// SetCacheCapacity resizes every escape-cache shard to n entries (per shard).
// Passing n <= 0 restores the package default. All prior cached entries are
// discarded by the resize; callers should invoke this at startup, not on the
// hot path.
func SetCacheCapacity(n int) {
	subjectCache.resize(n)
	kvKeyCache.resize(n)
	consumerNameCache.resize(n)
}
