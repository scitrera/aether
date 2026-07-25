package gateway

import (
	"sync"
	"time"

	"github.com/scitrera/aether/server/internal/audit"
)

// auditCoalescer suppresses bursts of identical successful message-route /
// proxy-route audit events so a high-volume stream (chat token streaming emits
// hundreds–thousands of OpMessageReceived/OpMessageRouted rows per turn) is
// recorded once per window — the first event is the authorization record and
// the rest are dropped. This is the streaming-chatter trim that keeps
// comprehensive_audit_log from growing unbounded.
//
// Design constraints (this runs on the message hot path across many goroutines):
//   - Concurrency-safe via a fixed array of independently-locked shards. The
//     critical section is a single map lookup + optional insert.
//   - Bounded memory: each shard's map is capped at coalesceMaxEntriesPerShard.
//     When an insert would exceed the cap the shard is first swept of entries
//     older than the window (lazy eviction); if still full, the single oldest
//     entry is evicted. No entry ever outlives the window by more than one
//     insert cycle on a busy shard, and the total map size is hard-capped
//     regardless of traffic, so there is no unbounded-growth path.
//   - Fail toward auditing: only a small allowlist of high-volume SUCCESSFUL
//     ops is ever coalesced. Every failure, denial, auth/task/kv/control event,
//     and any op not on the allowlist is always written.
//
// Disabled mode: when the configured window is <= 0 the constructor returns
// nil and callers treat a nil *auditCoalescer as a zero-overhead passthrough
// (every event is written, exactly as before this type existed).
type auditCoalescer struct {
	window time.Duration
	shards [coalesceShardCount]coalesceShard
}

// coalesceShardCount is the fixed number of independently-locked shards. A
// power of two so the mask in shardIndex is a cheap bitwise AND.
const coalesceShardCount = 64

// coalesceMaxEntriesPerShard caps the live keys per shard. With 64 shards the
// worst-case footprint is ~128K keys before evict-on-insert kicks in — small
// (each entry is a short string + a time.Time) and, crucially, hard-bounded.
const coalesceMaxEntriesPerShard = 2048

type coalesceShard struct {
	mu   sync.Mutex
	last map[string]time.Time // key -> time the authorization record was written
}

// coalescableOps is the allowlist of high-volume SUCCESSFUL routing/receive
// operations whose within-window repeats are suppressed. These are the
// per-message and per-proxy-request events that dominate audit volume during
// chat streaming; the first occurrence per (sender, target, op) key is the
// authorization record and later identical events add no security signal.
//
// Deliberately EXCLUDED (never coalesced, so they are always audited):
//   - Any success=false event (route/proxy failures, denials) — gated below,
//     not by op.
//   - OpMessageRouteFailed / OpProxyHttpFailed / OpTunnelOpenFailed.
//   - Tunnel lifecycle (OpTunnelOpened / OpTunnelClosed): these are discrete,
//     comparatively low-volume authorization events (one per tunnel, not one
//     per byte/chunk — the per-stream close is OpProxyHttpStreamClosed), so per
//     the "when in doubt, do not coalesce" rule they are kept fully audited.
//   - Auth, identity, task, KV, ACL, admin, control ops — not present here.
var coalescableOps = map[string]struct{}{
	audit.OpMessageReceived: {},
	audit.OpMessageRouted:   {},
	audit.OpProxyHttpRouted: {},
}

// newAuditCoalescer builds a coalescer for the given window. A window <= 0
// disables coalescing entirely: the constructor returns nil and the caller's
// nil check makes auditLog a zero-overhead passthrough.
func newAuditCoalescer(window time.Duration) *auditCoalescer {
	if window <= 0 {
		return nil
	}
	c := &auditCoalescer{window: window}
	for i := range c.shards {
		c.shards[i].last = make(map[string]time.Time)
	}
	return c
}

// shouldLog reports whether event must be written to the audit log now.
//
// It returns true for every event that is not a coalescable high-volume
// success event (always audit). For a coalescable event it returns true only
// for the first occurrence of the (actor, target, op) key within the window —
// the authorization record — and false for subsequent identical events until
// the window elapses, at which point the next event re-admits (re-stamped and
// written).
//
// A nil receiver (coalescing disabled) always returns true.
func (c *auditCoalescer) shouldLog(event *audit.AuditEvent) bool {
	if c == nil || event == nil {
		return true
	}

	// Fail toward auditing: never coalesce failures or non-allowlisted ops.
	if !event.Success {
		return true
	}
	if _, ok := coalescableOps[event.Operation]; !ok {
		return true
	}

	// Key = (sender identity, target topic, op). ActorID is the sender and
	// ResourceID is the target topic for message/proxy events (see
	// audit.NewMessageEvent). The NUL separators keep the composite key
	// unambiguous across field boundaries.
	key := event.ActorID + "\x00" + event.ResourceID + "\x00" + event.Operation
	now := time.Now()
	sh := &c.shards[shardIndex(key)]

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if last, ok := sh.last[key]; ok && now.Sub(last) < c.window {
		return false // within window — suppress this repeat
	}

	// First in window (or the prior record has expired): this is a fresh
	// authorization record we must keep. Enforce the per-shard bound before
	// inserting so the map can never grow without limit.
	if len(sh.last) >= coalesceMaxEntriesPerShard {
		c.evictExpiredLocked(sh, now)
		if len(sh.last) >= coalesceMaxEntriesPerShard {
			evictOldestLocked(sh)
		}
	}
	sh.last[key] = now
	return true
}

// evictExpiredLocked removes every entry whose window has elapsed. Caller must
// hold sh.mu.
func (c *auditCoalescer) evictExpiredLocked(sh *coalesceShard, now time.Time) {
	for k, t := range sh.last {
		if now.Sub(t) >= c.window {
			delete(sh.last, k)
		}
	}
}

// evictOldestLocked removes the single oldest entry. Only reached when a shard
// is at cap and no entries have expired — a pathological all-fresh-keys burst.
// Caller must hold sh.mu.
func evictOldestLocked(sh *coalesceShard) {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, t := range sh.last {
		if first || t.Before(oldestTime) {
			oldestKey, oldestTime, first = k, t, false
		}
	}
	if !first {
		delete(sh.last, oldestKey)
	}
}

// shardIndex maps a key to a shard via FNV-1a, masked to the shard count.
func shardIndex(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	var h uint32 = offset32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h & (coalesceShardCount - 1)
}
