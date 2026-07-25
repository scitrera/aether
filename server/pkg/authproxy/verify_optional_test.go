package authproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scitrera/aether/server/internal/auth"
)

// newVerifyTestServer builds a verify-mode Server whose middleware carries an
// EMPTY composite authenticator. An empty composite returns (nil, nil) for any
// credentials, which AuthMiddleware.Authenticate maps to a 401 ("no
// authenticator matched"). This lets us exercise the credential-present path
// (fail closed) without a database or real authenticator, while the no-cred
// optional path never reaches Authenticate at all.
func newVerifyTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := &Config{Mode: ModeVerify, ListenAddr: ":0"}
	m := &AuthMiddleware{
		authenticator: auth.NewCompositeAuthenticator(),
		tenantID:      "test-tenant",
	}
	srv, err := NewServer(cfg, m)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

// assertNoIdentityHeaders fails if any trusted identity header carries a value.
// Used on fail-closed (401) paths where Authenticate returns before any header
// is stamped or cleared, so the identity headers must be entirely absent.
func assertNoIdentityHeaders(t *testing.T, h http.Header) {
	t.Helper()
	for _, name := range []string{HeaderTenantID, HeaderUserID, HeaderPrincipalType, HeaderWorkspaceAccess, HeaderActorType, HeaderActorID, HeaderAuthorityMode} {
		if v := h.Get(name); v != "" {
			t.Errorf("expected no %s header value, got %q", name, v)
		}
	}
}

// assertClearedIdentityHeaders fails if a trusted identity header is missing or
// carries a non-empty value. On the anonymous ext_authz passthrough the handler
// must EMIT every identity header with an empty value so Envoy's authz-response
// override overwrites any client-supplied/spoofed identity header with empty.
func assertClearedIdentityHeaders(t *testing.T, h http.Header) {
	t.Helper()
	for _, name := range []string{HeaderTenantID, HeaderUserID, HeaderPrincipalType, HeaderWorkspaceAccess, HeaderActorType, HeaderActorID, HeaderAuthorityMode} {
		vals, present := h[http.CanonicalHeaderKey(name)]
		if !present {
			t.Errorf("expected %s to be present (empty) on anonymous passthrough, but it was absent", name)
			continue
		}
		if len(vals) != 1 || vals[0] != "" {
			t.Errorf("expected %s present with empty value on anonymous passthrough, got %v", name, vals)
		}
	}
}

func TestVerifyOptional_NoCredentials_AnonymousPassthrough(t *testing.T) {
	srv := newVerifyTestServer(t)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/auth/verify-optional", nil)
	srv.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for no-cred optional verify, got %d (body=%s)", w.Code, w.Body.String())
	}
	assertClearedIdentityHeaders(t, w.Header())
}

func TestVerifyOptional_SessionCookieConfigured_ButAbsent_AnonymousPassthrough(t *testing.T) {
	srv := newVerifyTestServer(t)
	srv.middleware.SetSessionCookieName("sid")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/auth/verify-optional", nil)
	// A cookie with a different name must not count as a credential.
	r.AddCookie(&http.Cookie{Name: "other", Value: "x"})
	srv.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 when configured session cookie absent, got %d", w.Code)
	}
	assertClearedIdentityHeaders(t, w.Header())
}

func TestVerifyOptional_SessionCookiePresent_FailsClosed(t *testing.T) {
	srv := newVerifyTestServer(t)
	srv.middleware.SetSessionCookieName("sid")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/auth/verify-optional", nil)
	r.AddCookie(&http.Cookie{Name: "sid", Value: "some-session-value"})
	srv.httpServer.Handler.ServeHTTP(w, r)

	// A credential is present (session cookie) but the empty authenticator
	// rejects it -> fail closed with 401, exactly like /auth/verify.
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for presented (but invalid) session cookie, got %d", w.Code)
	}
	assertNoIdentityHeaders(t, w.Header())
}

func TestVerifyOptional_AuthorizationHeader_FailsClosed(t *testing.T) {
	srv := newVerifyTestServer(t)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/auth/verify-optional", nil)
	r.Header.Set("Authorization", "Bearer opaque-token-value")
	srv.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for presented (but invalid) Authorization credential, got %d", w.Code)
	}
	assertNoIdentityHeaders(t, w.Header())
}

// TestVerify_NoCredentials_Unchanged pins the existing /auth/verify behaviour:
// a no-credential request must still be rejected 401. This guards against the
// optional path leaking into the non-optional route.
func TestVerify_NoCredentials_Unchanged(t *testing.T) {
	srv := newVerifyTestServer(t)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/auth/verify", nil)
	srv.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for no-cred /auth/verify, got %d", w.Code)
	}
}

// extraHeaderResolver is a test IdentityResolver that also implements the
// optional ExtraHeaderNaming interface, declaring a resolver-specific extra
// header. Used to prove the anonymous passthrough clears resolver ExtraHeaders.
type extraHeaderResolver struct{ NoOpResolver }

func (extraHeaderResolver) ExtraHeaderNames() []string { return []string{"X-Scitrera-User"} }

// TestVerifyOptional_Anonymous_ClearsClientSuppliedSpoof pins the security
// invariant: a client that supplies its own X-Auth-* / X-Scitrera-* identity
// headers on an anonymous (no-credential) verify-optional request gets those
// headers overwritten to EMPTY on the authz response, so Envoy's override
// clears the spoof rather than forwarding it upstream.
func TestVerifyOptional_Anonymous_ClearsClientSuppliedSpoof(t *testing.T) {
	cfg := &Config{Mode: ModeVerify, ListenAddr: ":0"}
	m := &AuthMiddleware{
		authenticator:    auth.NewCompositeAuthenticator(),
		tenantID:         "test-tenant",
		identityResolver: extraHeaderResolver{},
	}
	srv, err := NewServer(cfg, m)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/auth/verify-optional", nil)
	// Client attempts to spoof identity.
	r.Header.Set("X-Auth-Tenant-ID", "evil-tenant")
	r.Header.Set("X-Auth-User-ID", "attacker")
	r.Header.Set("X-Scitrera-User", "attacker@evil.example")
	srv.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for spoofed no-cred optional verify, got %d", w.Code)
	}
	assertClearedIdentityHeaders(t, w.Header())

	// The resolver-declared extra header must also be cleared (present, empty).
	vals, present := w.Header()["X-Scitrera-User"]
	if !present {
		t.Errorf("expected X-Scitrera-User present (empty) to clear the spoof, but it was absent")
	} else if len(vals) != 1 || vals[0] != "" {
		t.Errorf("expected X-Scitrera-User present with empty value, got %v", vals)
	}
}

func TestHasCredentials(t *testing.T) {
	m := &AuthMiddleware{tenantID: "test"}

	// No Authorization, no session cookie configured.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if m.HasCredentials(r) {
		t.Error("expected no credentials for bare request")
	}

	// Authorization header present.
	r = httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer abc")
	if !m.HasCredentials(r) {
		t.Error("expected credentials when Authorization header set")
	}

	// Session cookie configured but not present.
	m.SetSessionCookieName("sid")
	r = httptest.NewRequest(http.MethodGet, "/", nil)
	if m.HasCredentials(r) {
		t.Error("expected no credentials when session cookie configured but absent")
	}

	// Session cookie configured and present.
	r = httptest.NewRequest(http.MethodGet, "/", nil)
	r.AddCookie(&http.Cookie{Name: "sid", Value: "v"})
	if !m.HasCredentials(r) {
		t.Error("expected credentials when session cookie present")
	}

	// Empty session cookie value does not count.
	r = httptest.NewRequest(http.MethodGet, "/", nil)
	r.AddCookie(&http.Cookie{Name: "sid", Value: ""})
	if m.HasCredentials(r) {
		t.Error("expected no credentials when session cookie value is empty")
	}
}

func TestSessionCookieName_Accessor(t *testing.T) {
	m := &AuthMiddleware{tenantID: "test"}
	if m.SessionCookieName() != "" {
		t.Errorf("expected empty session cookie name by default, got %q", m.SessionCookieName())
	}
	m.SetSessionCookieName("sid")
	if m.SessionCookieName() != "sid" {
		t.Errorf("expected sid, got %q", m.SessionCookieName())
	}
}
