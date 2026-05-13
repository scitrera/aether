package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pkgerrors "github.com/scitrera/aether/pkg/errors"
)

// rateLimiter is a simple sliding window counter for per-identity message rate limiting.
type rateLimiter struct {
	mu          sync.Mutex
	count       int
	windowStart time.Time
}

// allow checks the rate limit and increments the counter. Returns false if the limit is exceeded.
func (r *rateLimiter) allow(limit int, window time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if now.Sub(r.windowStart) >= window {
		// Window has expired; start a new one.
		r.windowStart = now
		r.count = 0
	}

	r.count++
	return r.count <= limit
}

// MemoryQuotaManager is an in-memory implementation of the QuotaChecker interface.
// It is intended for lite/embedded mode where Redis is not available.
type MemoryQuotaManager struct {
	defaults DefaultQuotas

	// connCounts maps workspace -> *atomic.Int64
	connCounts sync.Map

	// msgRates maps "workspace:identity" -> *rateLimiter
	msgRates sync.Map
}

// NewMemoryQuotaManager creates a MemoryQuotaManager with the given default quotas.
func NewMemoryQuotaManager(defaults DefaultQuotas) *MemoryQuotaManager {
	return &MemoryQuotaManager{
		defaults: defaults,
	}
}

// CheckAndIncrementConnections atomically checks and increments the connection count
// for a workspace. Returns QuotaExceededError if the limit is reached.
func (m *MemoryQuotaManager) CheckAndIncrementConnections(ctx context.Context, workspace string) error {
	limit := m.defaults.MaxConnectionsPerWorkspace
	if limit <= 0 {
		// Unlimited — just increment.
		m.getConnCounter(workspace).Add(1)
		return nil
	}

	counter := m.getConnCounter(workspace)
	newVal := counter.Add(1)
	if int(newVal) > limit {
		// Exceeded — roll back the increment.
		counter.Add(-1)
		return &pkgerrors.QuotaExceededError{
			Resource:  "connections",
			Workspace: workspace,
			Current:   limit,
			Limit:     limit,
		}
	}
	return nil
}

// DecrementConnections decrements the connection count for a workspace, clamping to zero.
func (m *MemoryQuotaManager) DecrementConnections(ctx context.Context, workspace string) error {
	counter := m.getConnCounter(workspace)
	for {
		cur := counter.Load()
		if cur <= 0 {
			counter.CompareAndSwap(cur, 0)
			return nil
		}
		if counter.CompareAndSwap(cur, cur-1) {
			return nil
		}
	}
}

// CheckMessageQuota verifies that the identity has not exceeded its per-second message
// rate limit using a simple sliding window counter.
func (m *MemoryQuotaManager) CheckMessageQuota(ctx context.Context, workspace, identity string) error {
	limit := m.defaults.MaxMessageRatePerIdentity
	if limit <= 0 {
		return nil // unlimited
	}

	key := workspace + ":" + identity
	rl := m.getMsgRateLimiter(key)
	if !rl.allow(int(limit), msgRateWindow) {
		return &pkgerrors.QuotaExceededError{
			Resource:  "message_rate",
			Workspace: workspace,
			Identity:  identity,
			Current:   int(limit) + 1,
			Limit:     int(limit),
		}
	}
	return nil
}

// CheckKVValueSize verifies that a KV value does not exceed the size limit.
func (m *MemoryQuotaManager) CheckKVValueSize(ctx context.Context, workspace string, valueSize int) error {
	limit := m.defaults.MaxKVValueSize
	if limit <= 0 {
		return nil // unlimited
	}

	if valueSize > limit {
		return &pkgerrors.QuotaExceededError{
			Resource:  "kv_value_size",
			Workspace: workspace,
			Current:   valueSize,
			Limit:     limit,
		}
	}
	return nil
}

// getConnCounter returns the atomic counter for a workspace, creating it if needed.
func (m *MemoryQuotaManager) getConnCounter(workspace string) *atomic.Int64 {
	v, _ := m.connCounts.LoadOrStore(workspace, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// getMsgRateLimiter returns the rateLimiter for a key, creating it if needed.
func (m *MemoryQuotaManager) getMsgRateLimiter(key string) *rateLimiter {
	v, _ := m.msgRates.LoadOrStore(key, &rateLimiter{windowStart: time.Now()})
	return v.(*rateLimiter)
}
