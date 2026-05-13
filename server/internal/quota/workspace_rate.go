package quota

import (
	"sync"

	"golang.org/x/time/rate"
)

// WorkspaceRateLimiter manages per-workspace message rate limits.
// Each workspace can have its own rate limit, falling back to the global default.
// Rate limiting is enforced in-memory using token bucket limiters.
type WorkspaceRateLimiter struct {
	mu             sync.RWMutex
	defaultRate    float64
	limiters       map[string]*rate.Limiter // workspace -> active limiter
	workspaceRates map[string]float64       // workspace -> configured rate override
}

// NewWorkspaceRateLimiter creates a WorkspaceRateLimiter with the given default rate.
// A defaultRate of 0 means unlimited.
func NewWorkspaceRateLimiter(defaultRate float64) *WorkspaceRateLimiter {
	return &WorkspaceRateLimiter{
		defaultRate:    defaultRate,
		limiters:       make(map[string]*rate.Limiter),
		workspaceRates: make(map[string]float64),
	}
}

// Allow checks if a message from the given workspace is allowed under its rate limit.
// Returns true if allowed (or if the workspace has no limit configured).
func (w *WorkspaceRateLimiter) Allow(workspace string) bool {
	limiter := w.getLimiter(workspace)
	if limiter == nil {
		return true // unlimited
	}
	return limiter.Allow()
}

// getLimiter returns the rate.Limiter for a workspace, creating one on first use.
// Returns nil when the effective rate is 0 (unlimited).
func (w *WorkspaceRateLimiter) getLimiter(workspace string) *rate.Limiter {
	w.mu.RLock()
	limiter, ok := w.limiters[workspace]
	w.mu.RUnlock()
	if ok {
		return limiter
	}

	// Create a new limiter under write lock.
	w.mu.Lock()
	defer w.mu.Unlock()
	// Re-check after acquiring write lock (double-checked locking).
	if limiter, ok = w.limiters[workspace]; ok {
		return limiter
	}

	effectiveRate := w.effectiveRate(workspace)
	if effectiveRate <= 0 {
		return nil // unlimited — don't store a limiter
	}

	// Burst is set to the rate rounded up, providing one second of burst capacity.
	burst := int(effectiveRate)
	if burst < 1 {
		burst = 1
	}
	limiter = rate.NewLimiter(rate.Limit(effectiveRate), burst)
	w.limiters[workspace] = limiter
	return limiter
}

// effectiveRate returns the configured rate for a workspace (must be called with at least RLock held).
func (w *WorkspaceRateLimiter) effectiveRate(workspace string) float64 {
	if r, ok := w.workspaceRates[workspace]; ok {
		return r
	}
	return w.defaultRate
}

// SetWorkspaceRate sets a custom rate limit (messages per second) for a specific workspace.
// A rate of 0 means use the global default. The in-memory limiter is replaced immediately.
func (w *WorkspaceRateLimiter) SetWorkspaceRate(workspace string, messagesPerSecond float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if messagesPerSecond <= 0 {
		// Revert to default: remove override and drop the cached limiter so it is
		// re-created from the default on the next Allow call.
		delete(w.workspaceRates, workspace)
		delete(w.limiters, workspace)
		return
	}

	w.workspaceRates[workspace] = messagesPerSecond

	// Replace the active limiter immediately so the new rate takes effect at once.
	burst := int(messagesPerSecond)
	if burst < 1 {
		burst = 1
	}
	w.limiters[workspace] = rate.NewLimiter(rate.Limit(messagesPerSecond), burst)
}

// GetWorkspaceRate returns the effective rate for a workspace.
// Returns the workspace-specific override if set, otherwise the default.
func (w *WorkspaceRateLimiter) GetWorkspaceRate(workspace string) float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.effectiveRate(workspace)
}

// RemoveWorkspaceRate removes any custom rate override for a workspace, reverting to the default.
func (w *WorkspaceRateLimiter) RemoveWorkspaceRate(workspace string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.workspaceRates, workspace)
	delete(w.limiters, workspace)
}

// ListWorkspaceRates returns all explicitly configured workspace rate overrides.
// The returned map is a snapshot copy; it does not include workspaces using the default.
func (w *WorkspaceRateLimiter) ListWorkspaceRates() map[string]float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make(map[string]float64, len(w.workspaceRates))
	for ws, r := range w.workspaceRates {
		result[ws] = r
	}
	return result
}

// DefaultRate returns the global default rate.
func (w *WorkspaceRateLimiter) DefaultRate() float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.defaultRate
}
