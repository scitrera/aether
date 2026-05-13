package authproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/identityheaders"
	"github.com/scitrera/aether/pkg/models"
)

func TestExtractCredentials_BearerAPIKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer some-opaque-api-key")

	creds := extractCredentials(r)

	if creds["api_key"] != "some-opaque-api-key" {
		t.Errorf("expected api_key credential, got %v", creds)
	}
	if _, ok := creds["bearer_token"]; ok {
		t.Error("opaque token should not be routed to bearer_token")
	}
}

func TestExtractCredentials_BearerJWT(t *testing.T) {
	// JWT has 3 dot-separated segments
	jwt := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.signature"
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer "+jwt)

	creds := extractCredentials(r)

	if creds["bearer_token"] != jwt {
		t.Errorf("expected bearer_token credential for JWT, got %v", creds)
	}
	if _, ok := creds["api_key"]; ok {
		t.Error("JWT should not be routed to api_key")
	}
}

func TestExtractCredentials_CaseInsensitiveBearer(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "bearer my-key")

	creds := extractCredentials(r)

	if creds["api_key"] != "my-key" {
		t.Errorf("expected api_key from lowercase bearer, got %v", creds)
	}
}

func TestExtractCredentials_NoHeader(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	creds := extractCredentials(r)

	if len(creds) != 0 {
		t.Errorf("expected empty credentials, got %v", creds)
	}
}

func TestExtractCredentials_NonBearerScheme(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")

	creds := extractCredentials(r)

	if len(creds) != 0 {
		t.Errorf("expected empty credentials for Basic auth, got %v", creds)
	}
}

func TestExtractWorkspace_Header(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderWorkspaceID, "ws-from-header")

	ws := extractWorkspace(r)
	if ws != "ws-from-header" {
		t.Errorf("expected ws-from-header, got %s", ws)
	}
}

func TestExtractWorkspace_QueryParam(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/?workspace_id=ws-from-query", nil)

	ws := extractWorkspace(r)
	if ws != "ws-from-query" {
		t.Errorf("expected ws-from-query, got %s", ws)
	}
}

func TestExtractWorkspace_JSONBody(t *testing.T) {
	body := `{"workspace_id":"ws-from-body","other":"data"}`
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	ws := extractWorkspace(r)
	if ws != "ws-from-body" {
		t.Errorf("expected ws-from-body, got %s", ws)
	}

	// Verify body is still readable after extraction
	remaining, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("body should still be readable: %v", err)
	}
	if string(remaining) != body {
		t.Errorf("body was not restored correctly: got %q", remaining)
	}
}

func TestExtractWorkspace_Default(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	ws := extractWorkspace(r)
	if ws != defaultWorkspace {
		t.Errorf("expected default workspace %q, got %s", defaultWorkspace, ws)
	}
}

func TestExtractWorkspace_HeaderTakesPrecedence(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/?workspace_id=from-query", nil)
	r.Header.Set(HeaderWorkspaceID, "from-header")

	ws := extractWorkspace(r)
	if ws != "from-header" {
		t.Errorf("header should take precedence, got %s", ws)
	}
}

func TestExtractWorkspaceFromBody_NonJSON(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not json"))

	ws := extractWorkspaceFromBody(r)
	if ws != "" {
		t.Errorf("expected empty for non-JSON body, got %s", ws)
	}
}

func TestExtractWorkspaceFromBody_EmptyBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))

	ws := extractWorkspaceFromBody(r)
	if ws != "" {
		t.Errorf("expected empty for empty body, got %s", ws)
	}
}

func TestExtractWorkspaceFromBody_LargeBody(t *testing.T) {
	// Create a body larger than maxBodyReadSize
	large := bytes.Repeat([]byte("x"), maxBodyReadSize+100)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(large))

	ws := extractWorkspaceFromBody(r)
	if ws != "" {
		t.Errorf("expected empty for oversized body, got %s", ws)
	}
}

func TestStripTrustedHeaders(t *testing.T) {
	// Legacy alias: verify the renamed function still strips X-Auth-* headers.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Auth-User-ID", "spoofed-admin")
	r.Header.Set("X-Auth-Tenant-ID", "spoofed-tenant")
	r.Header.Set("X-Auth-Principal-Type", "Agent")
	r.Header.Set("X-Auth-Scopes", "admin")
	r.Header.Set("X-Other-Header", "keep-this")
	r.Header.Set("Authorization", "Bearer keep-this-too")

	stripInboundIdentityHeaders(r)

	if r.Header.Get("X-Auth-User-ID") != "" {
		t.Error("X-Auth-User-ID should have been stripped")
	}
	if r.Header.Get("X-Auth-Tenant-ID") != "" {
		t.Error("X-Auth-Tenant-ID should have been stripped")
	}
	if r.Header.Get("X-Auth-Principal-Type") != "" {
		t.Error("X-Auth-Principal-Type should have been stripped")
	}
	if r.Header.Get("X-Auth-Scopes") != "" {
		t.Error("X-Auth-Scopes should have been stripped")
	}
	if r.Header.Get("X-Other-Header") != "keep-this" {
		t.Error("non-X-Auth headers should be preserved")
	}
	if r.Header.Get("Authorization") != "Bearer keep-this-too" {
		t.Error("Authorization should be preserved (stripped separately)")
	}
}

func TestStripInboundIdentityHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	// X-Auth-* headers must be stripped.
	r.Header.Set("X-Auth-User-ID", "spoofed")
	r.Header.Set("X-Auth-Tenant-ID", "spoofed-tenant")
	// X-Aether-* headers must also be stripped.
	r.Header.Set("X-Aether-Grant-ID", "spoofed-grant")
	r.Header.Set("X-Aether-Authority-Mode", "on_behalf_of")
	r.Header.Set("X-Aether-Subject-Type", "User")
	r.Header.Set("X-Aether-Subject-ID", "alice")
	// Non-prefixed headers must be preserved.
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Request-ID", "req-123")

	stripInboundIdentityHeaders(r)

	for _, h := range []string{"X-Auth-User-ID", "X-Auth-Tenant-ID", "X-Aether-Grant-ID", "X-Aether-Authority-Mode", "X-Aether-Subject-Type", "X-Aether-Subject-ID"} {
		if r.Header.Get(h) != "" {
			t.Errorf("%s should have been stripped", h)
		}
	}
	if r.Header.Get("Content-Type") != "application/json" {
		t.Error("Content-Type should be preserved")
	}
	if r.Header.Get("X-Request-ID") != "req-123" {
		t.Error("X-Request-ID should be preserved")
	}
}

func TestInjectHeaders(t *testing.T) {
	m := &AuthMiddleware{tenantID: "test-tenant"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	// Pre-set spoofed headers that should be stripped
	r.Header.Set("X-Auth-User-ID", "spoofed")
	r.Header.Set("X-Aether-Grant-ID", "spoofed-grant")
	r.Header.Set("Authorization", "Bearer secret-token")

	authed := &AuthenticatedRequest{
		UserID:          "real-user",
		PrincipalType:   "User",
		WorkspaceAccess: 20,
		Scopes:          "read,write",
		APIKeyID:        "key-123",
	}

	m.InjectHeaders(r, authed)

	if r.Header.Get(HeaderTenantID) != "test-tenant" {
		t.Errorf("expected tenant test-tenant, got %s", r.Header.Get(HeaderTenantID))
	}
	if r.Header.Get(HeaderUserID) != "real-user" {
		t.Errorf("expected user real-user, got %s", r.Header.Get(HeaderUserID))
	}
	if r.Header.Get(HeaderPrincipalType) != "User" {
		t.Errorf("expected principal User, got %s", r.Header.Get(HeaderPrincipalType))
	}
	if r.Header.Get(HeaderWorkspaceAccess) != "20" {
		t.Errorf("expected access 20, got %s", r.Header.Get(HeaderWorkspaceAccess))
	}
	if r.Header.Get(HeaderScopes) != "read,write" {
		t.Errorf("expected scopes read,write, got %s", r.Header.Get(HeaderScopes))
	}
	if r.Header.Get(HeaderAPIKeyID) != "key-123" {
		t.Errorf("expected api key key-123, got %s", r.Header.Get(HeaderAPIKeyID))
	}
	if r.Header.Get("Authorization") != "" {
		t.Error("Authorization header should have been stripped")
	}
	// X-Aether-* hints must be stripped.
	if r.Header.Get("X-Aether-Grant-ID") != "" {
		t.Error("X-Aether-Grant-ID should have been stripped")
	}
	// Direct mode: actor/authority-mode headers.
	if r.Header.Get(HeaderActorType) != "User" {
		t.Errorf("expected actor type User, got %s", r.Header.Get(HeaderActorType))
	}
	if r.Header.Get(HeaderActorID) != "real-user" {
		t.Errorf("expected actor id real-user, got %s", r.Header.Get(HeaderActorID))
	}
	if r.Header.Get(HeaderAuthorityMode) != AuthorityModeDirect {
		t.Errorf("expected authority mode direct, got %s", r.Header.Get(HeaderAuthorityMode))
	}
	// No grant headers in direct mode.
	if r.Header.Get(HeaderGrantID) != "" {
		t.Error("HeaderGrantID should be absent in direct mode")
	}
}

func TestSetResponseHeaders(t *testing.T) {
	m := &AuthMiddleware{tenantID: "resp-tenant"}
	w := httptest.NewRecorder()

	authed := &AuthenticatedRequest{
		UserID:          "user-1",
		PrincipalType:   "Agent",
		WorkspaceAccess: 30,
	}

	m.SetResponseHeaders(w, authed)

	if w.Header().Get(HeaderTenantID) != "resp-tenant" {
		t.Errorf("expected resp-tenant, got %s", w.Header().Get(HeaderTenantID))
	}
	if w.Header().Get(HeaderUserID) != "user-1" {
		t.Errorf("expected user-1, got %s", w.Header().Get(HeaderUserID))
	}
	if w.Header().Get(HeaderPrincipalType) != "Agent" {
		t.Errorf("expected Agent, got %s", w.Header().Get(HeaderPrincipalType))
	}
	if w.Header().Get(HeaderWorkspaceAccess) != "30" {
		t.Errorf("expected 30, got %s", w.Header().Get(HeaderWorkspaceAccess))
	}
	// Optional headers should be absent when empty
	if w.Header().Get(HeaderScopes) != "" {
		t.Error("Scopes should be empty when not set")
	}
	if w.Header().Get(HeaderAPIKeyID) != "" {
		t.Error("APIKeyID should be empty when not set")
	}
	// Direct mode actor/authority-mode headers.
	if w.Header().Get(HeaderActorType) != "Agent" {
		t.Errorf("expected actor type Agent, got %s", w.Header().Get(HeaderActorType))
	}
	if w.Header().Get(HeaderActorID) != "user-1" {
		t.Errorf("expected actor id user-1, got %s", w.Header().Get(HeaderActorID))
	}
	if w.Header().Get(HeaderAuthorityMode) != AuthorityModeDirect {
		t.Errorf("expected authority mode direct, got %s", w.Header().Get(HeaderAuthorityMode))
	}
}

func TestMatchesWorkspacePattern(t *testing.T) {
	tests := []struct {
		name      string
		patterns  []string
		workspace string
		want      bool
	}{
		{"exact match", []string{"prod"}, "prod", true},
		{"glob match", []string{"prod-*"}, "prod-us-east", true},
		{"no match", []string{"prod-*"}, "staging", false},
		{"multiple patterns", []string{"prod-*", "staging"}, "staging", true},
		{"wildcard all", []string{"*"}, "anything", true},
		{"empty patterns", []string{}, "anything", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesWorkspacePattern(tt.patterns, tt.workspace)
			if got != tt.want {
				t.Errorf("matchesWorkspacePattern(%v, %q) = %v, want %v", tt.patterns, tt.workspace, got, tt.want)
			}
		})
	}
}

func TestWriteJSONError(t *testing.T) {
	w := httptest.NewRecorder()

	writeJSONError(w, http.StatusForbidden, "access denied", "not allowed")

	if w.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"error"`) || !strings.Contains(body, `"detail"`) {
		t.Errorf("expected JSON error response, got %s", body)
	}
}

func TestContextWithAuth_RoundTrip(t *testing.T) {
	authed := &AuthenticatedRequest{
		UserID:        "ctx-user",
		PrincipalType: "User",
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := ContextWithAuth(r.Context(), authed)

	recovered, ok := AuthFromContext(ctx)
	if !ok {
		t.Fatal("expected to recover auth from context")
	}
	if recovered.UserID != "ctx-user" {
		t.Errorf("expected ctx-user, got %s", recovered.UserID)
	}
}

func TestAuthFromContext_Missing(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	_, ok := AuthFromContext(r.Context())
	if ok {
		t.Error("expected ok=false when no auth in context")
	}
}

// ---------------------------------------------------------------------------
// OBO / authority helpers
// ---------------------------------------------------------------------------

func TestInjectHeaders_DirectMode_SetsActorAndMode(t *testing.T) {
	m := &AuthMiddleware{tenantID: "t1"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	authed := &AuthenticatedRequest{
		UserID:        "alice",
		PrincipalType: "User",
	}
	m.InjectHeaders(r, authed)

	if got := r.Header.Get(HeaderAuthorityMode); got != AuthorityModeDirect {
		t.Errorf("AuthorityMode: want %q, got %q", AuthorityModeDirect, got)
	}
	if got := r.Header.Get(HeaderActorType); got != "User" {
		t.Errorf("ActorType: want User, got %q", got)
	}
	if got := r.Header.Get(HeaderActorID); got != "alice" {
		t.Errorf("ActorID: want alice, got %q", got)
	}
	if got := r.Header.Get(HeaderGrantID); got != "" {
		t.Errorf("GrantID should be absent in direct mode, got %q", got)
	}
}

func TestInjectHeaders_OBOMode_SubjectOverridesUserID(t *testing.T) {
	m := &AuthMiddleware{tenantID: "t1"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	authed := &AuthenticatedRequest{
		UserID:        "sv::foo::bar",
		PrincipalType: "Service",
		Authority: &AuthenticatedAuthority{
			ActorType:      "Service",
			ActorID:        "sv::foo::bar",
			GrantID:        "g1",
			SubjectType:    "User",
			SubjectID:      "alice",
			AudienceType:   "service",
			AudienceID:     "sv::foo::bar",
			MaxAccessLevel: 20,
			WorkspaceScope: []string{"ws1", "ws2"},
		},
	}
	m.InjectHeaders(r, authed)

	// Backward-compat: UserID/PrincipalType must be the subject.
	if got := r.Header.Get(HeaderUserID); got != "alice" {
		t.Errorf("HeaderUserID: want alice, got %q", got)
	}
	if got := r.Header.Get(HeaderPrincipalType); got != "User" {
		t.Errorf("HeaderPrincipalType: want User, got %q", got)
	}
	// Actor headers.
	if got := r.Header.Get(HeaderActorType); got != "Service" {
		t.Errorf("ActorType: want Service, got %q", got)
	}
	if got := r.Header.Get(HeaderActorID); got != "sv::foo::bar" {
		t.Errorf("ActorID: want sv.foo.bar, got %q", got)
	}
	// Authority mode.
	if got := r.Header.Get(HeaderAuthorityMode); got != AuthorityModeOnBehalfOf {
		t.Errorf("AuthorityMode: want %q, got %q", AuthorityModeOnBehalfOf, got)
	}
	// Grant-scoped headers.
	if got := r.Header.Get(HeaderGrantID); got != "g1" {
		t.Errorf("GrantID: want g1, got %q", got)
	}
	if got := r.Header.Get(HeaderSubjectType); got != "User" {
		t.Errorf("SubjectType: want User, got %q", got)
	}
	if got := r.Header.Get(HeaderSubjectID); got != "alice" {
		t.Errorf("SubjectID: want alice, got %q", got)
	}
	if got := r.Header.Get(HeaderMaxAccessLevel); got != "20" {
		t.Errorf("MaxAccessLevel: want 20, got %q", got)
	}
	if got := r.Header.Get(HeaderWorkspaceScope); got != "ws1,ws2" {
		t.Errorf("WorkspaceScope: want ws1,ws2, got %q", got)
	}
}

func TestInjectHeaders_OBOMode_EmptyWorkspaceScope_OmitsHeader(t *testing.T) {
	m := &AuthMiddleware{tenantID: "t1"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	authed := &AuthenticatedRequest{
		UserID:        "sv::foo::bar",
		PrincipalType: "Service",
		Authority: &AuthenticatedAuthority{
			ActorType:      "Service",
			ActorID:        "sv::foo::bar",
			GrantID:        "g2",
			SubjectType:    "User",
			SubjectID:      "bob",
			AudienceType:   "service",
			AudienceID:     "sv::foo::bar",
			MaxAccessLevel: 10,
			WorkspaceScope: nil, // empty → any → header must be absent
		},
	}
	m.InjectHeaders(r, authed)

	if got := r.Header.Get(HeaderWorkspaceScope); got != "" {
		t.Errorf("WorkspaceScope should be absent when scope is empty (means any), got %q", got)
	}
}

func TestInjectHeaders_DirectMode_CallerTopic(t *testing.T) {
	m := &AuthMiddleware{tenantID: "t1"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	authed := &AuthenticatedRequest{
		UserID:        "alice",
		PrincipalType: "User",
	}
	m.InjectHeaders(r, authed)

	// In direct mode CallerTopic == UserID; CallerSubject absent.
	if got := r.Header.Get(HeaderXAetherCallerTopic); got != "alice" {
		t.Errorf("CallerTopic: want %q, got %q", "alice", got)
	}
	if got := r.Header.Get(HeaderXAetherCallerSubject); got != "" {
		t.Errorf("CallerSubject should be absent in direct mode, got %q", got)
	}
}

func TestInjectHeaders_OBOMode_CallerTopicAndSubject(t *testing.T) {
	m := &AuthMiddleware{tenantID: "t1"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	authed := &AuthenticatedRequest{
		UserID:        "sv::foo::bar",
		PrincipalType: "Service",
		Authority: &AuthenticatedAuthority{
			ActorType:      "Service",
			ActorID:        "sv::foo::bar",
			GrantID:        "g1",
			SubjectType:    "User",
			SubjectID:      "alice",
			AudienceType:   "service",
			AudienceID:     "sv::foo::bar",
			MaxAccessLevel: 20,
		},
	}
	m.InjectHeaders(r, authed)

	// CallerTopic == actor (the authenticated service); CallerSubject == OBO subject.
	if got := r.Header.Get(HeaderXAetherCallerTopic); got != "sv::foo::bar" {
		t.Errorf("CallerTopic: want %q, got %q", "sv::foo::bar", got)
	}
	if got := r.Header.Get(HeaderXAetherCallerSubject); got != "alice" {
		t.Errorf("CallerSubject: want %q, got %q", "alice", got)
	}
}

// TestCallerHeaderParity_AuthProxy_vs_Sidecar verifies that auth-proxy and
// sidecar mint produce byte-equal output for the same input. The sidecar path
// is exercised via pkg/identityheaders.Mint directly (with equivalent inputs).
func TestCallerHeaderParity_AuthProxy_vs_Sidecar(t *testing.T) {
	// Auth-proxy path
	m := &AuthMiddleware{tenantID: "tenant-1"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	authed := &AuthenticatedRequest{
		UserID:          "alice",
		PrincipalType:   "User",
		WorkspaceAccess: 20,
		Scopes:          "read",
		APIKeyID:        "key-1",
	}
	m.InjectHeaders(r, authed)

	// Sidecar/identityheaders path: same inputs, same expected output.
	sidecarH := identityheaders.Mint(context.Background(), "tenant-1", identityheaders.Identity{
		UserID:          "alice",
		PrincipalType:   "User",
		WorkspaceAccess: 20,
		Scopes:          "read",
		APIKeyID:        "key-1",
		CallerTopic:     "alice",
	})

	for _, hdr := range []string{
		HeaderTenantID, HeaderUserID, HeaderPrincipalType, HeaderWorkspaceAccess,
		HeaderScopes, HeaderAPIKeyID, HeaderActorType, HeaderActorID, HeaderAuthorityMode,
		HeaderXAetherCallerTopic,
	} {
		want := sidecarH.Get(hdr)
		got := r.Header.Get(hdr)
		if got != want {
			t.Errorf("parity %s: auth-proxy=%q sidecar=%q", hdr, got, want)
		}
	}
}

func TestMapAuthorityErrorToStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"invalid context", acl.ErrInvalidAuthorityContext, http.StatusBadRequest},
		{"grant not found", acl.ErrAuthorityGrantNotFound, http.StatusUnauthorized},
		{"grant expired", acl.ErrAuthorityGrantExpired, http.StatusUnauthorized},
		{"grant revoked", acl.ErrAuthorityGrantRevoked, http.StatusUnauthorized},
		{"delegate mismatch", acl.ErrAuthorityGrantDelegateMismatch, http.StatusForbidden},
		{"subject mismatch", acl.ErrAuthorityGrantSubjectMismatch, http.StatusForbidden},
		{"audience mismatch", acl.ErrAuthorityGrantAudienceMismatch, http.StatusForbidden},
		{"wrapped invalid context", fmt.Errorf("wrap: %w", acl.ErrInvalidAuthorityContext), http.StatusBadRequest},
		{"wrapped not found", fmt.Errorf("wrap: %w", acl.ErrAuthorityGrantNotFound), http.StatusUnauthorized},
		{"other error", errors.New("some internal error"), http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapAuthorityErrorToStatus(tt.err)
			if got != tt.want {
				t.Errorf("mapAuthorityErrorToStatus(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

func TestBuildSubjectIdentity(t *testing.T) {
	tests := []struct {
		name            string
		principalType   string
		principalID     string
		wantType        models.PrincipalType
		wantCanonicalID string
		wantErr         bool
	}{
		{"user lowercase", "user", "alice", models.PrincipalUser, "alice", false},
		{"User titlecase", "User", "alice", models.PrincipalUser, "alice", false},
		{"service", "service", "sv::foo::bar", models.PrincipalService, "sv::foo::bar", false},
		{"agent", "agent", "ag::ws::impl::spec", models.PrincipalAgent, "ag::ws::impl::spec", false},
		{"user empty id", "user", "", models.PrincipalUser, "", true},
		{"unknown type", "unknown", "x", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildSubjectIdentity(tt.principalType, tt.principalID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildSubjectIdentity(%q, %q) error = %v, wantErr %v", tt.principalType, tt.principalID, err, tt.wantErr)
			}
			if !tt.wantErr {
				if got.Type != tt.wantType {
					t.Errorf("Type: want %q, got %q", tt.wantType, got.Type)
				}
				if canonID := got.CanonicalPrincipalID(); canonID != tt.wantCanonicalID {
					t.Errorf("CanonicalPrincipalID: want %q, got %q", tt.wantCanonicalID, canonID)
				}
			}
		})
	}
}

func TestWorkspaceInGrantScope(t *testing.T) {
	tests := []struct {
		name      string
		workspace string
		scope     []string
		want      bool
	}{
		{"nil scope means any", "ws1", nil, true},
		{"empty scope means any", "ws1", []string{}, true},
		{"exact match", "ws1", []string{"ws1"}, true},
		{"no match", "ws1", []string{"ws2"}, false},
		{"wildcard star", "ws1", []string{"*"}, true},
		{"glob match", "ws-prod-east", []string{"ws-prod-*"}, true},
		{"multiple entries match", "ws1", []string{"ws2", "ws1"}, true},
		{"multiple entries no match", "ws3", []string{"ws1", "ws2"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := workspaceInGrantScope(tt.workspace, tt.scope)
			if got != tt.want {
				t.Errorf("workspaceInGrantScope(%q, %v) = %v, want %v", tt.workspace, tt.scope, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// mockResolver for resolveAuthority unit tests
// ---------------------------------------------------------------------------

type mockResolver struct {
	result *acl.ResolvedAuthority
	err    error
}

func (m *mockResolver) ResolveAuthority(_ context.Context, _ models.Identity, _ acl.RequestAuthorityContext, _ acl.GrantAudienceContext) (*acl.ResolvedAuthority, error) {
	return m.result, m.err
}

func TestResolveAuthority_Success(t *testing.T) {
	grant := &acl.AuthorityGrant{
		GrantID:        "g1",
		SubjectType:    "user",
		SubjectID:      "alice",
		AudienceType:   "service",
		AudienceID:     "sv::foo::bar",
		MaxAccessLevel: 20,
		WorkspaceScope: []string{"ws1"},
	}
	actor := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	resolver := &mockResolver{
		result: &acl.ResolvedAuthority{
			Actor:   actor,
			Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
			Grant:   grant,
		},
	}

	m := &AuthMiddleware{resolver: resolver}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderAetherSubjectType, "user")
	r.Header.Set(HeaderAetherSubjectID, "alice")

	authed := &AuthenticatedRequest{UserID: "sv::foo::bar", PrincipalType: "Service"}
	err := m.resolveAuthority(w, r, authed, actor, "ws1", "g1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authed.Authority == nil {
		t.Fatal("expected Authority to be populated")
	}
	if authed.Authority.GrantID != "g1" {
		t.Errorf("GrantID: want g1, got %q", authed.Authority.GrantID)
	}
	if authed.Authority.SubjectID != "alice" {
		t.Errorf("SubjectID: want alice, got %q", authed.Authority.SubjectID)
	}
	if authed.Authority.MaxAccessLevel != 20 {
		t.Errorf("MaxAccessLevel: want 20, got %d", authed.Authority.MaxAccessLevel)
	}
}

func TestResolveAuthority_InvalidGrant_Returns401(t *testing.T) {
	resolver := &mockResolver{err: acl.ErrAuthorityGrantNotFound}
	m := &AuthMiddleware{resolver: resolver}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderAetherSubjectType, "user")
	r.Header.Set(HeaderAetherSubjectID, "alice")

	authed := &AuthenticatedRequest{}
	actor := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	err := m.resolveAuthority(w, r, authed, actor, "ws1", "g-missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if authed.Authority != nil {
		t.Error("Authority should be nil on failure")
	}
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestResolveAuthority_SubjectMismatch_Returns403(t *testing.T) {
	resolver := &mockResolver{err: acl.ErrAuthorityGrantSubjectMismatch}
	m := &AuthMiddleware{resolver: resolver}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderAetherSubjectType, "user")
	r.Header.Set(HeaderAetherSubjectID, "bob")

	authed := &AuthenticatedRequest{}
	actor := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	err := m.resolveAuthority(w, r, authed, actor, "ws1", "g1")
	if err == nil {
		t.Fatal("expected error")
	}
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", w.Code)
	}
}

func TestResolveAuthority_MissingSubjectHeaders_Returns400(t *testing.T) {
	resolver := &mockResolver{}
	m := &AuthMiddleware{resolver: resolver}
	w := httptest.NewRecorder()
	// No X-Aether-Subject-Type or X-Aether-Subject-ID set.
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	authed := &AuthenticatedRequest{}
	actor := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	err := m.resolveAuthority(w, r, authed, actor, "ws1", "g1")
	if err == nil {
		t.Fatal("expected error for missing subject headers")
	}
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	if authed.Authority != nil {
		t.Error("Authority should be nil")
	}
}

// TestAuthorityFromResolved_CarriesResourceScope confirms that the resolved
// authority preserves the grant's ResourceScope map so downstream matchers
// (proxy_path, tunnel_target) can read it without a second store fetch.
func TestAuthorityFromResolved_CarriesResourceScope(t *testing.T) {
	grant := &acl.AuthorityGrant{
		GrantID:        "g1",
		SubjectType:    "user",
		SubjectID:      "alice",
		AudienceType:   "service",
		AudienceID:     "sv::foo::bar",
		MaxAccessLevel: 20,
		ResourceScope: map[string][]string{
			"proxy_path": {"_default::POST /memory/*"},
		},
	}
	actor := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	resolver := &mockResolver{
		result: &acl.ResolvedAuthority{
			Actor:   actor,
			Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
			Grant:   grant,
		},
	}

	m := &AuthMiddleware{resolver: resolver}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/memory/store", nil)
	r.Header.Set(HeaderAetherSubjectType, "user")
	r.Header.Set(HeaderAetherSubjectID, "alice")

	authed := &AuthenticatedRequest{}
	if err := m.resolveAuthority(w, r, authed, actor, "ws1", "g1"); err != nil {
		t.Fatalf("resolveAuthority: %v", err)
	}
	if authed.Authority == nil {
		t.Fatal("authority must be populated")
	}
	got := authed.Authority.ResourceScope["proxy_path"]
	want := []string{"_default::POST /memory/*"}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("ResourceScope[proxy_path]: got %v, want %v", got, want)
	}
}

// TestProxyPathScope_AuthProxyEnforcement_AllowDeny exercises the matcher with
// the auth-proxy default backend convention: grants targeted at `_default`
// (or "*") apply to the auth-proxy direct path.
func TestProxyPathScope_AuthProxyEnforcement_AllowDeny(t *testing.T) {
	patterns := []string{"_default::POST /memory/*"}

	if !identityheaders.MatchProxyPath(patterns, identityheaders.AuthProxyDefaultBackend, http.MethodPost, "/memory/store") {
		t.Error("expected POST /memory/store to be allowed under _default::POST /memory/*")
	}
	if identityheaders.MatchProxyPath(patterns, identityheaders.AuthProxyDefaultBackend, http.MethodGet, "/memory/store") {
		t.Error("expected GET to be denied under _default::POST /memory/*")
	}
	if identityheaders.MatchProxyPath(patterns, identityheaders.AuthProxyDefaultBackend, http.MethodPost, "/billing") {
		t.Error("expected /billing to be denied under _default::POST /memory/*")
	}

	// Wildcard backend works for the auth-proxy default name.
	if !identityheaders.MatchProxyPath([]string{"*::GET /health"}, identityheaders.AuthProxyDefaultBackend, http.MethodGet, "/health") {
		t.Error("expected wildcard backend pattern to apply to auth-proxy default backend")
	}
}

func TestResolveAuthority_WorkspaceOutOfScope_Returns403(t *testing.T) {
	grant := &acl.AuthorityGrant{
		GrantID:        "g1",
		SubjectType:    "user",
		SubjectID:      "alice",
		AudienceType:   "service",
		AudienceID:     "sv::foo::bar",
		MaxAccessLevel: 20,
		WorkspaceScope: []string{"ws-prod"},
	}
	actor := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	resolver := &mockResolver{
		result: &acl.ResolvedAuthority{
			Actor:   actor,
			Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
			Grant:   grant,
		},
	}
	m := &AuthMiddleware{resolver: resolver}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderAetherSubjectType, "user")
	r.Header.Set(HeaderAetherSubjectID, "alice")

	authed := &AuthenticatedRequest{}
	// Request workspace is ws-staging, not in scope ws-prod.
	err := m.resolveAuthority(w, r, authed, actor, "ws-staging", "g1")
	if err == nil {
		t.Fatal("expected error for workspace out of scope")
	}
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", w.Code)
	}
	if authed.Authority != nil {
		t.Error("Authority should be nil when workspace is out of scope")
	}
}
