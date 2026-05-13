package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"
)

// TestRateLimiter_UnderLimit verifies that requests under the burst limit succeed.
func TestRateLimiter_UnderLimit(t *testing.T) {
	rl := newIPRateLimiter(rate.Limit(100), 5, false)
	defer rl.Close()

	for i := 0; i < 5; i++ {
		limiter := rl.getLimiter("192.0.2.1")
		if !limiter.Allow() {
			t.Errorf("request %d should have been allowed", i+1)
		}
	}
}

// TestRateLimiter_OverLimit verifies that requests exceeding the burst return false.
func TestRateLimiter_OverLimit(t *testing.T) {
	// burst=2, rate=0 so tokens never refill
	rl := newIPRateLimiter(rate.Limit(0), 2, false)
	defer rl.Close()

	rl.getLimiter("10.0.0.1").Allow() // consume token 1
	rl.getLimiter("10.0.0.1").Allow() // consume token 2

	// Third request should be denied
	if rl.getLimiter("10.0.0.1").Allow() {
		t.Error("third request should have been denied")
	}
}

// TestRateLimiter_SeparateIPs verifies that different IPs have independent limiters.
func TestRateLimiter_SeparateIPs(t *testing.T) {
	// burst=1 so each IP gets exactly one token
	rl := newIPRateLimiter(rate.Limit(0), 1, false)
	defer rl.Close()

	ip1 := "10.0.0.1"
	ip2 := "10.0.0.2"

	if !rl.getLimiter(ip1).Allow() {
		t.Error("ip1 first request should be allowed")
	}
	if !rl.getLimiter(ip2).Allow() {
		t.Error("ip2 first request should be allowed (separate bucket)")
	}

	// Both are now exhausted
	if rl.getLimiter(ip1).Allow() {
		t.Error("ip1 second request should be denied")
	}
	if rl.getLimiter(ip2).Allow() {
		t.Error("ip2 second request should be denied")
	}
}

// TestRateLimiterMiddleware_Allow verifies that the middleware passes through
// requests when under the rate limit.
func TestRateLimiterMiddleware_Allow(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{
		connections: []*ConnectionInfo{},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req.RemoteAddr = "192.0.2.1:12345"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// TestRateLimiterMiddleware_Blocked verifies that requests over the limit
// receive 429 Too Many Requests.
func TestRateLimiterMiddleware_Blocked(t *testing.T) {
	cfg := ServerConfig{
		InsecureNoAuth: true,
		// Very low limit: burst=1, rate=0 (no refill)
		RateLimit:      0.0001,
		RateLimitBurst: 1,
	}
	srv := NewServer(cfg, &mockStateProvider{
		connections: []*ConnectionInfo{},
	})

	router := buildTestRouter(srv, false)
	t.Cleanup(func() { srv.rateLimiter.Close() })

	ip := "10.1.2.3:9999"

	// First request — consumes the single burst token
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req1.RemoteAddr = ip
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request expected 200, got %d", rec1.Code)
	}

	// Second request — burst exhausted, rate too low to refill
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req2.RemoteAddr = ip
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("second request expected 429, got %d", rec2.Code)
	}
}

// TestExtractIP_RemoteAddr verifies IP extraction from RemoteAddr (no proxy).
func TestExtractIP_RemoteAddr(t *testing.T) {
	rl := newIPRateLimiter(rate.Limit(10), 10, false)
	defer rl.Close()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.42:54321"

	ip := rl.extractIP(req)
	if ip != "203.0.113.42" {
		t.Errorf("expected 203.0.113.42, got %s", ip)
	}
}

// TestExtractIP_TrustProxy verifies that X-Forwarded-For is used when trustProxy=true.
func TestExtractIP_TrustProxy(t *testing.T) {
	rl := newIPRateLimiter(rate.Limit(10), 10, true)
	defer rl.Close()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:9999"
	req.Header.Set("X-Forwarded-For", "203.0.113.99, 10.0.0.1")

	ip := rl.extractIP(req)
	if ip != "203.0.113.99" {
		t.Errorf("expected 203.0.113.99, got %s", ip)
	}
}

// TestExtractIP_NoProxy_IgnoresForwardedFor verifies that X-Forwarded-For is
// ignored when trustProxy=false.
func TestExtractIP_NoProxy_IgnoresForwardedFor(t *testing.T) {
	rl := newIPRateLimiter(rate.Limit(10), 10, false)
	defer rl.Close()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.5:8080"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")

	ip := rl.extractIP(req)
	if ip != "10.0.0.5" {
		t.Errorf("expected 10.0.0.5 (RemoteAddr), got %s", ip)
	}
}
