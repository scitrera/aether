package proxysidecar

import (
	"context"
	"net/http/httptest"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/identityheaders"
	"github.com/scitrera/aether/pkg/models"
)

// proxyPathStubResolver returns a canned ResolvedAuthority whose grant carries
// the configured proxy_path resource scope. The test exercises the full
// dispatch path: applyHeaders → ResolveAndMint → MatchProxyPath.
type proxyPathStubResolver struct {
	resourceScope map[string][]string
}

func (s *proxyPathStubResolver) ResolveAuthority(_ context.Context, actor models.Identity, req acl.RequestAuthorityContext, _ acl.GrantAudienceContext) (*acl.ResolvedAuthority, error) {
	return &acl.ResolvedAuthority{
		Actor:   actor,
		Subject: req.Subject,
		Grant: &acl.AuthorityGrant{
			GrantID:        req.GrantID,
			SubjectType:    "user",
			SubjectID:      req.Subject.ID,
			DelegateType:   "service",
			DelegateID:     actor.ID,
			AudienceType:   "service",
			AudienceID:     actor.ID,
			MaxAccessLevel: 30,
			ResourceScope:  s.resourceScope,
		},
	}, nil
}

func makeProxyPathBackend(t *testing.T, name string, resolver identityheaders.AuthorityResolver) *httpBackend {
	t.Helper()
	cfg := BackendConfig{
		Name:         name,
		Kind:         BackendKindHTTP,
		URL:          "http://example.invalid",
		AllowPaths:   []string{"/*"},
		AllowMethods: []string{"*"},
		HeaderMode:   HeaderModeStrict,
	}
	cfg.normalizeAndValidate(0)
	return newHTTPBackend(cfg, "tenant-1", resolver)
}

func makeOBORequest(method, reqPath string) *pb.ProxyHttpRequest {
	return &pb.ProxyHttpRequest{
		RequestId: "r-test",
		Method:    method,
		Path:      reqPath,
		Headers: map[string]string{
			identityheaders.HeaderUserID:        "sv::foo::bar",
			identityheaders.HeaderPrincipalType: "Service",
		},
		Authorization: &pb.AuthorizationContext{
			AuthorityMode: identityheaders.AuthorityModeOnBehalfOf,
			GrantId:       "g1",
			Subject: &pb.PrincipalRef{
				PrincipalType: "user",
				PrincipalId:   "alice",
			},
		},
	}
}

// TestProxyPathScope_AllowOnExactBackendAndGlobPath: grant scope
// ["api-v1::GET /v1/*"] permits GET /v1/x on backend api-v1.
func TestProxyPathScope_AllowOnExactBackendAndGlobPath(t *testing.T) {
	t.Parallel()
	resolver := &proxyPathStubResolver{
		resourceScope: map[string][]string{
			identityheaders.ResourceTypeProxyPath: {"api-v1::GET /v1/*"},
		},
	}
	backend := makeProxyPathBackend(t, "api-v1", resolver)
	httpReq := httptest.NewRequest("GET", "/v1/x", nil)

	authority, err := backend.applyHeaders(context.Background(), makeOBORequest("GET", "/v1/x"), httpReq)
	if err != nil {
		t.Fatalf("applyHeaders: %v", err)
	}
	if authority == nil {
		t.Fatal("expected authority on OBO request")
	}
	patterns := authority.ResourceScope[identityheaders.ResourceTypeProxyPath]
	if !identityheaders.MatchProxyPath(patterns, backend.cfg.Name, "GET", "/v1/x") {
		t.Errorf("expected allow for api-v1 GET /v1/x with patterns %v", patterns)
	}
}

// TestProxyPathScope_DenyMatrix exercises the four "deny" arms from the task brief.
func TestProxyPathScope_DenyMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		backend     string
		method      string
		reqPath     string
		patterns    []string
		expectAllow bool
	}{
		{"hit api-v1 GET /v1/*", "api-v1", "GET", "/v1/x", []string{"api-v1::GET /v1/*"}, true},
		{"deny api-v2 GET /v1/*", "api-v2", "GET", "/v1/x", []string{"api-v1::GET /v1/*"}, false},
		{"deny POST on api-v1 /v1/*", "api-v1", "POST", "/v1/x", []string{"api-v1::GET /v1/*"}, false},
		{"deny api-v1 GET /v2/*", "api-v1", "GET", "/v2/x", []string{"api-v1::GET /v1/*"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := identityheaders.MatchProxyPath(tt.patterns, tt.backend, tt.method, tt.reqPath)
			if got != tt.expectAllow {
				t.Errorf("MatchProxyPath(%v, %q, %q, %q) = %v, want %v",
					tt.patterns, tt.backend, tt.method, tt.reqPath, got, tt.expectAllow)
			}
		})
	}
}

// TestProxyPathScope_DispatchDenied verifies the full sidecar path returns
// ACL_DENIED when the grant scope rejects the request. Uses dispatch (not
// just applyHeaders) so the matcher invocation site is exercised end-to-end.
func TestProxyPathScope_DispatchDenied(t *testing.T) {
	t.Parallel()
	resolver := &proxyPathStubResolver{
		resourceScope: map[string][]string{
			identityheaders.ResourceTypeProxyPath: {"api-v1::GET /v1/*"},
		},
	}
	backend := makeProxyPathBackend(t, "api-v1", resolver)

	// Method mismatch (POST not in scope) — expect ACL_DENIED.
	req := makeOBORequest("POST", "/v1/x")
	_, perr := backend.dispatch(context.Background(), req, nil)
	if perr == nil {
		t.Fatal("expected ACL_DENIED for POST /v1/x outside grant scope")
	}
	if perr.Kind != pb.ProxyError_ACL_DENIED {
		t.Errorf("kind: want ACL_DENIED, got %s", perr.Kind)
	}
	if got := perr.Message; got == "" || !contains(got, "proxy_path_scope_denied") {
		t.Errorf("expected proxy_path_scope_denied in message, got %q", got)
	}
}

// TestProxyPathScope_NoScope_BlanketAllow: grants without proxy_path scope
// preserve legacy blanket-allow behaviour.
func TestProxyPathScope_NoScope_BlanketAllow(t *testing.T) {
	t.Parallel()
	resolver := &proxyPathStubResolver{
		resourceScope: nil, // no proxy_path key
	}
	backend := makeProxyPathBackend(t, "api-v1", resolver)

	req := makeOBORequest("DELETE", "/anything/at/all")
	_, perr := backend.dispatch(context.Background(), req, nil)
	// dispatch will fail on the actual HTTP call (invalid URL), but it must
	// not fail with ACL_DENIED — the scope check must pass through.
	if perr != nil && perr.Kind == pb.ProxyError_ACL_DENIED {
		t.Errorf("expected blanket allow with no proxy_path scope, got ACL_DENIED: %s", perr.Message)
	}
}

// TestProxyPathScope_StarPattern_Allows: `["*"]` preserves blanket allow.
func TestProxyPathScope_StarPattern_Allows(t *testing.T) {
	t.Parallel()
	resolver := &proxyPathStubResolver{
		resourceScope: map[string][]string{
			identityheaders.ResourceTypeProxyPath: {"*"},
		},
	}
	backend := makeProxyPathBackend(t, "api-v1", resolver)

	req := makeOBORequest("DELETE", "/foo")
	_, perr := backend.dispatch(context.Background(), req, nil)
	if perr != nil && perr.Kind == pb.ProxyError_ACL_DENIED {
		t.Errorf("expected blanket allow with [\"*\"], got ACL_DENIED: %s", perr.Message)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
