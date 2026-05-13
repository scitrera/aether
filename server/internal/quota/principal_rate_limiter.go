package quota

import (
	"sync"

	"golang.org/x/time/rate"
)

// PrincipalRateLimiter enforces per-principal rate limits keyed by the
// authenticated identity string. It mirrors WorkspaceRateLimiter's API but
// scopes limits to individual principals rather than workspaces — useful for
// flows like foreign audit submission where DoS resistance matters per caller
// rather than per workspace.
//
// Same algorithm (token bucket via golang.org/x/time/rate) and the same
// double-checked-locking limiter-cache pattern. A defaultRate of 0 means
// unlimited.
type PrincipalRateLimiter struct {
	mu             sync.RWMutex
	defaultRate    float64
	limiters       map[string]*rate.Limiter // principal identity -> active limiter
	principalRates map[string]float64       // principal identity -> configured override
}

// NewPrincipalRateLimiter creates a PrincipalRateLimiter with the given default
// rate. A defaultRate of 0 means unlimited.
func NewPrincipalRateLimiter(defaultRate float64) *PrincipalRateLimiter {
	return &PrincipalRateLimiter{
		defaultRate:    defaultRate,
		limiters:       make(map[string]*rate.Limiter),
		principalRates: make(map[string]float64),
	}
}

// Allow checks if an event from the given principal is allowed under its rate
// limit. Returns true if allowed (or if the principal has no limit configured).
func (p *PrincipalRateLimiter) Allow(principal string) bool {
	limiter := p.getLimiter(principal)
	if limiter == nil {
		return true // unlimited
	}
	return limiter.Allow()
}

// getLimiter returns the rate.Limiter for a principal, creating one on first use.
// Returns nil when the effective rate is 0 (unlimited).
func (p *PrincipalRateLimiter) getLimiter(principal string) *rate.Limiter {
	p.mu.RLock()
	limiter, ok := p.limiters[principal]
	p.mu.RUnlock()
	if ok {
		return limiter
	}

	// Create a new limiter under write lock.
	p.mu.Lock()
	defer p.mu.Unlock()
	// Re-check after acquiring write lock (double-checked locking).
	if limiter, ok = p.limiters[principal]; ok {
		return limiter
	}

	effectiveRate := p.effectiveRate(principal)
	if effectiveRate <= 0 {
		return nil // unlimited — don't store a limiter
	}

	// Burst is set to the rate rounded up, providing one second of burst capacity.
	burst := int(effectiveRate)
	if burst < 1 {
		burst = 1
	}
	limiter = rate.NewLimiter(rate.Limit(effectiveRate), burst)
	p.limiters[principal] = limiter
	return limiter
}

// effectiveRate returns the configured rate for a principal (must be called with at least RLock held).
func (p *PrincipalRateLimiter) effectiveRate(principal string) float64 {
	if r, ok := p.principalRates[principal]; ok {
		return r
	}
	return p.defaultRate
}

// SetPrincipalRate sets a custom rate limit (events per second) for a specific
// principal. A rate of 0 means use the global default. The in-memory limiter
// is replaced immediately.
func (p *PrincipalRateLimiter) SetPrincipalRate(principal string, eventsPerSecond float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if eventsPerSecond <= 0 {
		delete(p.principalRates, principal)
		delete(p.limiters, principal)
		return
	}

	p.principalRates[principal] = eventsPerSecond

	burst := int(eventsPerSecond)
	if burst < 1 {
		burst = 1
	}
	p.limiters[principal] = rate.NewLimiter(rate.Limit(eventsPerSecond), burst)
}

// GetPrincipalRate returns the effective rate for a principal.
// Returns the principal-specific override if set, otherwise the default.
func (p *PrincipalRateLimiter) GetPrincipalRate(principal string) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.effectiveRate(principal)
}

// RemovePrincipalRate removes any custom rate override for a principal,
// reverting to the default.
func (p *PrincipalRateLimiter) RemovePrincipalRate(principal string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.principalRates, principal)
	delete(p.limiters, principal)
}

// DefaultRate returns the global default rate.
func (p *PrincipalRateLimiter) DefaultRate() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.defaultRate
}
