package proxysidecar

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

func TestTerminator_DispatchAllowList_DeniesDisallowedMethod(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].AllowMethods = []string{"GET"}

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId: "r-deny-method",
		Method:    "DELETE",
		Path:      "/v1/whatever",
	}
	resp, _ := t1.HandleProxyRequest(context.Background(), req, nil)
	assertProxyError(t, resp, pb.ProxyError_ACL_DENIED)
}

func TestTerminator_DispatchAllowList_DeniesDisallowedPath(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].AllowPaths = []string{"/v1/*"}

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId: "r-deny-path",
		Method:    "GET",
		Path:      "/admin/secrets",
	}
	resp, _ := t1.HandleProxyRequest(context.Background(), req, nil)
	assertProxyError(t, resp, pb.ProxyError_ACL_DENIED)
}

// assertProxyError asserts that resp carries a ProxyError of the given kind.
func assertProxyError(t *testing.T, resp *pb.ProxyHttpResponse, want pb.ProxyError_Kind) {
	t.Helper()
	if resp == nil {
		t.Fatal("expected non-nil ProxyHttpResponse")
	}
	if resp.GetError() == nil {
		t.Fatalf("expected ProxyError, got status=%d body=%q", resp.GetStatusCode(), string(resp.GetBody()))
	}
	if resp.GetError().GetKind() != want {
		t.Fatalf("ProxyError kind: got %s, want %s (msg=%q)",
			resp.GetError().GetKind(), want, resp.GetError().GetMessage())
	}
}

// terminatorTestConfig builds a minimal validated terminator config with one
// HTTP backend pointing at a placeholder URL.
func terminatorTestConfig() *Config {
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:0", Insecure: true},
		Service: ServiceConfig{Implementation: "memorylayer", Specifier: "test"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Name:         "default",
				Kind:         BackendKindHTTP,
				URL:          "http://example.invalid",
				AllowPaths:   []string{"/*"},
				AllowMethods: []string{"GET", "POST", "PUT", "DELETE"},
				MaxBodyBytes: 1 << 20,
				HeaderMode:   HeaderModePassthrough,
			}},
		},
		TenantID: "tenant-test",
	}
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return cfg
}
