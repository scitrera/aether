package proxysidecar

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

// multiBackendConfig builds a validated terminator config with two HTTP
// backends, two TCP backends, two WS backends, and two UDP backends — each
// pair distinguishable by Name and allow-list so tests can exercise
// BackendName-driven selection.
func multiBackendConfig() *Config {
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:0", Insecure: true},
		Service: ServiceConfig{Implementation: "memorylayer", Specifier: "test"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{
				{
					Name:         "primary",
					Kind:         BackendKindHTTP,
					URL:          "http://primary.invalid",
					AllowPaths:   []string{"/v1/*"},
					AllowMethods: []string{"GET", "POST"},
					HeaderMode:   HeaderModePassthrough,
				},
				{
					Name:         "admin",
					Kind:         BackendKindHTTP,
					URL:          "http://admin.invalid",
					AllowPaths:   []string{"/admin/*"},
					AllowMethods: []string{"DELETE"},
					HeaderMode:   HeaderModePassthrough,
				},
				{
					Name:             "tcp-a",
					Kind:             BackendKindTCP,
					URL:              "10.0.0.1:5000",
					AllowRemoteHints: []string{"10.0.0.1:*"},
				},
				{
					Name:             "tcp-b",
					Kind:             BackendKindTCP,
					URL:              "10.0.0.2:5000",
					AllowRemoteHints: []string{"10.0.0.2:*"},
				},
				{
					Name:             "ws-a",
					Kind:             BackendKindWS,
					URL:              "ws://ws-a.invalid",
					AllowRemoteHints: []string{"ws://ws-a.*"},
				},
				{
					Name:             "ws-b",
					Kind:             BackendKindWS,
					URL:              "ws://ws-b.invalid",
					AllowRemoteHints: []string{"ws://ws-b.*"},
				},
				{
					Name:             "udp-a",
					Kind:             BackendKindUDP,
					URL:              "10.0.0.10:5300",
					AllowRemoteHints: []string{"10.0.0.10:*"},
				},
				{
					Name:             "udp-b",
					Kind:             BackendKindUDP,
					URL:              "10.0.0.11:5300",
					AllowRemoteHints: []string{"10.0.0.11:*"},
				},
			},
		},
		TenantID: "tenant-test",
	}
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return cfg
}

// TestSelectBackend_ExplicitName_Match verifies that an explicit BackendName
// routes the request to the named backend even when another backend would
// also match the method/path.
func TestSelectBackend_ExplicitName_Match(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-named",
		Method:      "DELETE",
		Path:        "/admin/users/42",
		BackendName: "admin",
	}
	b, perr := term.selectBackend(req)
	if perr != nil {
		t.Fatalf("selectBackend: unexpected error %v", perr)
	}
	if b.cfg.Name != "admin" {
		t.Fatalf("selected backend %q, want admin", b.cfg.Name)
	}
}

// TestSelectBackend_ExplicitName_NotFound verifies that requesting a backend
// name that does not exist returns ACL_DENIED.
func TestSelectBackend_ExplicitName_NotFound(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-missing",
		Method:      "GET",
		Path:        "/v1/items",
		BackendName: "ghost",
	}
	resp, _ := term.HandleProxyRequest(context.Background(), req, nil)
	assertProxyError(t, resp, pb.ProxyError_ACL_DENIED)
}

// TestSelectBackend_ExplicitName_ACLStillApplies verifies that even when the
// backend exists, the named backend's allow-list is enforced and a request
// outside its scope is denied.
func TestSelectBackend_ExplicitName_ACLStillApplies(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	// admin only allows DELETE on /admin/* but the caller asks for GET on /v1.
	req := &pb.ProxyHttpRequest{
		RequestId:   "r-acl-violation",
		Method:      "GET",
		Path:        "/v1/items",
		BackendName: "admin",
	}
	resp, _ := term.HandleProxyRequest(context.Background(), req, nil)
	assertProxyError(t, resp, pb.ProxyError_ACL_DENIED)
}

// TestSelectBackend_NoNameFallsBackToFirstMatch verifies that an empty
// BackendName preserves the legacy behaviour: the first backend whose
// allow-list admits the request is selected.
func TestSelectBackend_NoNameFallsBackToFirstMatch(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId: "r-default",
		Method:    "GET",
		Path:      "/v1/items",
	}
	b, perr := term.selectBackend(req)
	if perr != nil {
		t.Fatalf("selectBackend: unexpected error %v", perr)
	}
	if b.cfg.Name != "primary" {
		t.Fatalf("selected backend %q, want primary", b.cfg.Name)
	}
}

// TestSelectTCPBackend_ExplicitName_Match verifies named TCP backend
// selection.
func TestSelectTCPBackend_ExplicitName_Match(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	open := &pb.TunnelOpen{
		TunnelId:    "t-1",
		Protocol:    pb.TunnelOpen_TCP,
		RemoteHint:  "10.0.0.2:5000",
		BackendName: "tcp-b",
	}
	b := term.selectTCPBackend(open)
	if b == nil {
		t.Fatal("selectTCPBackend: nil backend")
	}
	if b.cfg.Name != "tcp-b" {
		t.Fatalf("selected backend %q, want tcp-b", b.cfg.Name)
	}
}

// TestSelectTCPBackend_ExplicitName_NotFound verifies that requesting an
// unknown name returns nil even if another backend would match the hint.
func TestSelectTCPBackend_ExplicitName_NotFound(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	open := &pb.TunnelOpen{
		TunnelId:    "t-2",
		Protocol:    pb.TunnelOpen_TCP,
		RemoteHint:  "10.0.0.1:5000",
		BackendName: "tcp-ghost",
	}
	if got := term.selectTCPBackend(open); got != nil {
		t.Fatalf("selectTCPBackend: got %q, want nil", got.cfg.Name)
	}
}

// TestSelectTCPBackend_ExplicitName_ACLDeniesHint verifies that even when the
// named backend exists, its allow_remote_hints allow-list is enforced.
func TestSelectTCPBackend_ExplicitName_ACLDeniesHint(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	// tcp-a only admits 10.0.0.1:* but the caller asks for 10.0.0.2:5000.
	open := &pb.TunnelOpen{
		TunnelId:    "t-3",
		Protocol:    pb.TunnelOpen_TCP,
		RemoteHint:  "10.0.0.2:5000",
		BackendName: "tcp-a",
	}
	if got := term.selectTCPBackend(open); got != nil {
		t.Fatalf("selectTCPBackend: got %q, want nil (ACL deny)", got.cfg.Name)
	}
}

// TestSelectWSBackend_ExplicitName_Match verifies named WS backend selection.
func TestSelectWSBackend_ExplicitName_Match(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	open := &pb.TunnelOpen{
		TunnelId:    "ws-1",
		Protocol:    pb.TunnelOpen_WEBSOCKET,
		RemoteHint:  "ws://ws-b.example",
		BackendName: "ws-b",
	}
	b := term.selectWSBackend(open)
	if b == nil {
		t.Fatal("selectWSBackend: nil backend")
	}
	if b.cfg.Name != "ws-b" {
		t.Fatalf("selected backend %q, want ws-b", b.cfg.Name)
	}
}

// TestSelectUDPBackend_ExplicitName_Match verifies named UDP backend
// selection.
func TestSelectUDPBackend_ExplicitName_Match(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	open := &pb.TunnelOpen{
		TunnelId:    "udp-1",
		Protocol:    pb.TunnelOpen_UDP,
		RemoteHint:  "10.0.0.11:5300",
		BackendName: "udp-b",
	}
	b := term.selectUDPBackend(open)
	if b == nil {
		t.Fatal("selectUDPBackend: nil backend")
	}
	if b.cfg.Name != "udp-b" {
		t.Fatalf("selected backend %q, want udp-b", b.cfg.Name)
	}
}

// TestSelectTunnelBackend_NoNameFallsBackToFirstMatch verifies that an empty
// BackendName preserves the legacy first-match behaviour for tunnels.
func TestSelectTunnelBackend_NoNameFallsBackToFirstMatch(t *testing.T) {
	t.Parallel()
	term, err := NewTerminator(multiBackendConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	open := &pb.TunnelOpen{
		TunnelId:   "t-default",
		Protocol:   pb.TunnelOpen_TCP,
		RemoteHint: "10.0.0.1:5000",
	}
	b := term.selectTCPBackend(open)
	if b == nil {
		t.Fatal("selectTCPBackend: nil backend")
	}
	if b.cfg.Name != "tcp-a" {
		t.Fatalf("selected backend %q, want tcp-a", b.cfg.Name)
	}
}
