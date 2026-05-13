package proxysidecar

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// TestHeaderMode_Strict_MintsByteIdenticalToAuthProxy asserts that strict
// mode produces the canonical X-Auth-* header set with exactly the same
// values that the auth-proxy would inject (via identityheaders.Mint, the
// same shared minter).
func TestHeaderMode_Strict_MintsByteIdenticalToAuthProxy(t *testing.T) {
	t.Parallel()

	cfg := BackendConfig{
		Name:         "default",
		Kind:         BackendKindHTTP,
		URL:          "http://example.invalid",
		AllowPaths:   []string{"/*"},
		AllowMethods: []string{"GET"},
		MaxBodyBytes: 1 << 20,
		HeaderMode:   HeaderModeStrict,
	}
	cfg.normalizeAndValidate(0)

	tenantID := "tenant-1"
	backend := newHTTPBackend(cfg, tenantID, nil)

	// Caller-supplied headers include a spoofed X-Auth header that strict
	// mode must remove before minting.
	req := &pb.ProxyHttpRequest{
		RequestId: "r1",
		Method:    "GET",
		Path:      "/v1/ping",
		Headers: map[string]string{
			"User-Agent":                        "test",
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
			identityheaders.HeaderScopes:        "read,write",
			identityheaders.HeaderAPIKeyID:      "key-123",
		},
		AppWorkspace: "prod",
	}

	httpReq := httptest.NewRequest("GET", "/v1/ping", nil)
	if _, err := backend.applyHeaders(context.Background(), req, httpReq); err != nil {
		t.Fatalf("applyHeaders: %v", err)
	}

	// Build the expected header set the same way auth-proxy would.
	expected := identityheaders.Mint(context.Background(), tenantID, identityheaders.Identity{
		UserID:        "alice",
		PrincipalType: "User",
		Scopes:        "read,write",
		APIKeyID:      "key-123",
	})

	for header, want := range map[string]string{
		identityheaders.HeaderTenantID:        tenantID,
		identityheaders.HeaderUserID:          "alice",
		identityheaders.HeaderPrincipalType:   "User",
		identityheaders.HeaderActorType:       "User",
		identityheaders.HeaderActorID:         "alice",
		identityheaders.HeaderAuthorityMode:   identityheaders.AuthorityModeDirect,
		identityheaders.HeaderScopes:          "read,write",
		identityheaders.HeaderAPIKeyID:        "key-123",
		identityheaders.HeaderWorkspaceAccess: strconv.Itoa(0),
		identityheaders.HeaderWorkspaceID:     "prod",
	} {
		got := httpReq.Header.Get(header)
		if got != want {
			t.Errorf("%s: got %q, want %q", header, got, want)
		}
		if header == identityheaders.HeaderWorkspaceID {
			continue // app_workspace is sidecar-only, not auth-proxy
		}
		if exp := expected.Get(header); exp != got {
			t.Errorf("%s diverges from auth-proxy mint: got %q, expected (auth-proxy) %q", header, got, exp)
		}
	}

	// And the caller-supplied user-agent must survive the strip+mint pass.
	if got := httpReq.Header.Get("User-Agent"); got != "test" {
		t.Errorf("User-Agent: got %q, want %q", got, "test")
	}
}

// TestHeaderMode_Passthrough_KeepsCallerHeadersAndDoesNotMint asserts the
// passthrough mode preserves caller headers verbatim and never mints fresh
// X-Auth-* values.
func TestHeaderMode_Passthrough_KeepsCallerHeadersAndDoesNotMint(t *testing.T) {
	t.Parallel()

	cfg := BackendConfig{HeaderMode: HeaderModePassthrough}
	cfg.normalizeAndValidate(0)
	cfg.HeaderMode = HeaderModePassthrough // normalizer doesn't override
	backend := newHTTPBackend(cfg, "tenant-9", nil)

	req := &pb.ProxyHttpRequest{
		RequestId: "r2",
		Method:    "GET",
		Path:      "/x",
		Headers: map[string]string{
			"X-Auth-User-ID": "preserved",
			"X-Custom":       "yes",
		},
	}
	httpReq := httptest.NewRequest("GET", "/x", nil)
	if _, err := backend.applyHeaders(context.Background(), req, httpReq); err != nil {
		t.Fatalf("applyHeaders: %v", err)
	}
	if got := httpReq.Header.Get("X-Auth-User-ID"); got != "preserved" {
		t.Errorf("passthrough must keep caller X-Auth-User-ID, got %q", got)
	}
	if got := httpReq.Header.Get(identityheaders.HeaderTenantID); got != "" {
		t.Errorf("passthrough must not mint X-Auth-Tenant-ID, got %q", got)
	}
	if got := httpReq.Header.Get("X-Custom"); got != "yes" {
		t.Errorf("X-Custom: got %q, want %q", got, "yes")
	}
}

// TestHeaderMode_Both_OverlaysMintedHeadersOnTopOfCallerHeaders asserts that
// both mode keeps caller headers but lets minted X-Auth-* values win on
// conflict.
func TestHeaderMode_Both_OverlaysMintedHeadersOnTopOfCallerHeaders(t *testing.T) {
	t.Parallel()

	cfg := BackendConfig{HeaderMode: HeaderModeBoth}
	cfg.normalizeAndValidate(0)
	cfg.HeaderMode = HeaderModeBoth
	backend := newHTTPBackend(cfg, "tenant-7", nil)

	req := &pb.ProxyHttpRequest{
		RequestId: "r3",
		Method:    "GET",
		Path:      "/y",
		Headers: map[string]string{
			"X-Custom":                          "keep",
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
	}
	httpReq := httptest.NewRequest("GET", "/y", nil)
	if _, err := backend.applyHeaders(context.Background(), req, httpReq); err != nil {
		t.Fatalf("applyHeaders: %v", err)
	}
	if got := httpReq.Header.Get(identityheaders.HeaderUserID); got != "alice" {
		t.Errorf("both must overlay minted X-Auth-User-ID with %q, got %q", "alice", got)
	}
	if got := httpReq.Header.Get("X-Custom"); got != "keep" {
		t.Errorf("both must keep X-Custom, got %q", got)
	}
	if got := httpReq.Header.Get(identityheaders.HeaderTenantID); got != "tenant-7" {
		t.Errorf("X-Auth-Tenant-ID minted: got %q", got)
	}
}
