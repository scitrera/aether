package quota

import "testing"

// TestPrincipalRateLimiter_UnderLimit verifies up-to-burst events pass.
func TestPrincipalRateLimiter_UnderLimit(t *testing.T) {
	t.Parallel()
	rl := NewPrincipalRateLimiter(100)
	for i := 0; i < 100; i++ {
		if !rl.Allow("agent-a") {
			t.Fatalf("request %d should be allowed under burst", i+1)
		}
	}
}

// TestPrincipalRateLimiter_AtLimit_Rejects verifies the limiter rejects once
// the bucket is empty and refills are slow.
func TestPrincipalRateLimiter_AtLimit_Rejects(t *testing.T) {
	t.Parallel()
	rl := NewPrincipalRateLimiter(1)
	if !rl.Allow("agent-strict") {
		t.Fatal("first request should be allowed (burst token)")
	}
	if rl.Allow("agent-strict") {
		t.Fatal("second immediate request should be rejected at the limit")
	}
}

// TestPrincipalRateLimiter_ZeroRate_Unlimited verifies a default rate of 0
// means uncapped — Allow always returns true.
func TestPrincipalRateLimiter_ZeroRate_Unlimited(t *testing.T) {
	t.Parallel()
	rl := NewPrincipalRateLimiter(0)
	for i := 0; i < 1000; i++ {
		if !rl.Allow("agent-unlimited") {
			t.Fatalf("unlimited principal rejected on call %d", i+1)
		}
	}
}

// TestPrincipalRateLimiter_DistinctPrincipals_Independent verifies each
// principal has its own bucket.
func TestPrincipalRateLimiter_DistinctPrincipals_Independent(t *testing.T) {
	t.Parallel()
	rl := NewPrincipalRateLimiter(1)
	if !rl.Allow("agent-a") {
		t.Fatal("agent-a first call should pass")
	}
	if !rl.Allow("agent-b") {
		t.Fatal("agent-b should have its own bucket and pass")
	}
}

// TestPrincipalRateLimiter_SetPrincipalRate_Override applies a per-principal
// override and verifies the new rate takes effect.
func TestPrincipalRateLimiter_SetPrincipalRate_Override(t *testing.T) {
	t.Parallel()
	rl := NewPrincipalRateLimiter(1)
	rl.SetPrincipalRate("agent-fast", 1000)
	if got := rl.GetPrincipalRate("agent-fast"); got != 1000 {
		t.Errorf("expected override rate 1000, got %v", got)
	}
	// 100 quick allows should all pass under burst 1000.
	for i := 0; i < 100; i++ {
		if !rl.Allow("agent-fast") {
			t.Fatalf("override should permit %d, got rejection", i+1)
		}
	}
}
