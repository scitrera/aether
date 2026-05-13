package authproxy

import (
	"net/http"
	"testing"
)

func TestEffectiveTenantID_ResolverOverride(t *testing.T) {
	m := &AuthMiddleware{tenantID: "static-default"}
	authed := &AuthenticatedRequest{
		Resolved: &ResolvedIdentity{DefaultTenantID: "from-resolver"},
	}
	if got := m.effectiveTenantID(authed); got != "from-resolver" {
		t.Errorf("expected resolver tenant id, got %q", got)
	}
}

func TestEffectiveTenantID_FallbackToStatic(t *testing.T) {
	m := &AuthMiddleware{tenantID: "static-default"}

	t.Run("nil resolved", func(t *testing.T) {
		authed := &AuthenticatedRequest{}
		if got := m.effectiveTenantID(authed); got != "static-default" {
			t.Errorf("expected static fallback, got %q", got)
		}
	})

	t.Run("empty resolved tenant", func(t *testing.T) {
		authed := &AuthenticatedRequest{Resolved: &ResolvedIdentity{}}
		if got := m.effectiveTenantID(authed); got != "static-default" {
			t.Errorf("expected static fallback, got %q", got)
		}
	})

	t.Run("nil authed", func(t *testing.T) {
		if got := m.effectiveTenantID(nil); got != "static-default" {
			t.Errorf("expected static fallback for nil authed, got %q", got)
		}
	})
}

func TestApplyExtraHeaders_DropsReserved(t *testing.T) {
	h := http.Header{}
	applyExtraHeaders(h, map[string]string{
		"X-Scitrera-User":       "alice@scitrera.com",
		"X-Scitrera-Tenants":    "acme,beta",
		"X-Auth-Tenant-ID":      "spoof",       // reserved — must be dropped
		"X-Aether-Caller-Topic": "spoof.topic", // reserved — must be dropped
		"x-auth-user-id":        "spoof",       // reserved (case-insensitive)
		"X-Custom-Header":       "ok",
	})

	if got := h.Get("X-Scitrera-User"); got != "alice@scitrera.com" {
		t.Errorf("X-Scitrera-User: got %q, want alice@scitrera.com", got)
	}
	if got := h.Get("X-Scitrera-Tenants"); got != "acme,beta" {
		t.Errorf("X-Scitrera-Tenants: got %q", got)
	}
	if got := h.Get("X-Custom-Header"); got != "ok" {
		t.Errorf("X-Custom-Header: got %q", got)
	}
	if got := h.Get("X-Auth-Tenant-ID"); got != "" {
		t.Errorf("reserved X-Auth-Tenant-ID must be dropped, got %q", got)
	}
	if got := h.Get("X-Aether-Caller-Topic"); got != "" {
		t.Errorf("reserved X-Aether-Caller-Topic must be dropped, got %q", got)
	}
	if got := h.Get("X-Auth-User-ID"); got != "" {
		t.Errorf("reserved X-Auth-User-ID (lowercase input) must be dropped, got %q", got)
	}
}

func TestIsReservedHeader(t *testing.T) {
	cases := []struct {
		k    string
		want bool
	}{
		{"X-Auth-Tenant-ID", true},
		{"x-auth-user-id", true},
		{"X-Aether-Grant-ID", true},
		{"x-aether-caller-topic", true},
		{"X-Scitrera-User", false},
		{"Authorization", false},
		{"X-Custom", false},
	}
	for _, tc := range cases {
		if got := isReservedHeader(tc.k); got != tc.want {
			t.Errorf("isReservedHeader(%q) = %v, want %v", tc.k, got, tc.want)
		}
	}
}

func TestAuthedToIdentity_ResolverOverridesUserID(t *testing.T) {
	authed := &AuthenticatedRequest{
		UserID:        "raw-oid-from-jwt",
		PrincipalType: "User",
		Resolved: &ResolvedIdentity{
			UserID:        "alice@scitrera.com",
			PrincipalType: "User",
		},
	}
	ident := authedToIdentity(authed)
	if ident.UserID != "alice@scitrera.com" {
		t.Errorf("expected resolver UserID, got %q", ident.UserID)
	}
	if ident.CallerTopic != "alice@scitrera.com" {
		t.Errorf("expected CallerTopic to mirror resolved UserID, got %q", ident.CallerTopic)
	}
}

func TestAuthedToIdentity_NoResolvedFallsBackToRaw(t *testing.T) {
	authed := &AuthenticatedRequest{
		UserID:        "raw-oid-from-jwt",
		PrincipalType: "User",
	}
	ident := authedToIdentity(authed)
	if ident.UserID != "raw-oid-from-jwt" {
		t.Errorf("expected raw UserID fallback, got %q", ident.UserID)
	}
}
