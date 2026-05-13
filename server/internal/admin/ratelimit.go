package admin

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	rateLimiterMaxEntries   = 10000
	rateLimiterTTL          = 10 * time.Minute
	rateLimiterCleanupEvery = 1 * time.Minute
)

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type ipRateLimiter struct {
	mu         sync.Mutex
	limiters   map[string]*limiterEntry
	r          rate.Limit
	burst      int
	trustProxy bool
	stop       chan struct{}
}

func newIPRateLimiter(r rate.Limit, burst int, trustProxy bool) *ipRateLimiter {
	rl := &ipRateLimiter{
		limiters:   make(map[string]*limiterEntry),
		r:          r,
		burst:      burst,
		trustProxy: trustProxy,
		stop:       make(chan struct{}),
	}
	go rl.cleanupLoop()
	return rl
}

// Close stops the background cleanup goroutine.
func (i *ipRateLimiter) Close() {
	close(i.stop)
}

func (i *ipRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rateLimiterCleanupEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			i.cleanup()
		case <-i.stop:
			return
		}
	}
}

// cleanup removes entries that have not been seen for more than rateLimiterTTL.
func (i *ipRateLimiter) cleanup() {
	threshold := time.Now().Add(-rateLimiterTTL)
	i.mu.Lock()
	defer i.mu.Unlock()
	for ip, entry := range i.limiters {
		if entry.lastSeen.Before(threshold) {
			delete(i.limiters, ip)
		}
	}
}

// evictOldest removes the single oldest entry from the map.
// Must be called with i.mu held.
func (i *ipRateLimiter) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for ip, entry := range i.limiters {
		if first || entry.lastSeen.Before(oldestTime) {
			oldestKey = ip
			oldestTime = entry.lastSeen
			first = false
		}
	}
	if oldestKey != "" {
		delete(i.limiters, oldestKey)
	}
}

func (i *ipRateLimiter) getLimiter(ip string) *rate.Limiter {
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, exists := i.limiters[ip]
	if !exists {
		// Enforce cap before adding a new entry.
		if len(i.limiters) >= rateLimiterMaxEntries {
			i.evictOldest()
		}
		entry = &limiterEntry{
			limiter:  rate.NewLimiter(i.r, i.burst),
			lastSeen: time.Now(),
		}
		i.limiters[ip] = entry
	} else {
		entry.lastSeen = time.Now()
	}
	return entry.limiter
}

// extractIP derives the rate-limit key from the request.
// When trustProxy is true the first IP in X-Forwarded-For is used (client IP);
// otherwise RemoteAddr is used directly. Port is always stripped.
func (i *ipRateLimiter) extractIP(r *http.Request) string {
	if i.trustProxy {
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			// Take only the first (client-supplied) IP in the list.
			first := strings.SplitN(forwarded, ",", 2)[0]
			first = strings.TrimSpace(first)
			if first != "" {
				return first
			}
		}
	}
	// Strip port from RemoteAddr (format is "host:port" or "[::1]:port").
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// Fallback: use as-is if parsing fails.
		return r.RemoteAddr
	}
	return host
}

func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := s.rateLimiter.extractIP(r)
		limiter := s.rateLimiter.getLimiter(ip)
		if !limiter.Allow() {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
