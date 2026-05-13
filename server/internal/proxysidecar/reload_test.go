package proxysidecar

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

// writeReloadConfig writes a minimal terminator config YAML to a temp file
// and returns the path.
func writeReloadConfig(t *testing.T, backendURL string, extraBackendURL string) string {
	t.Helper()
	var extra string
	if extraBackendURL != "" {
		extra = fmt.Sprintf(`
    - name: backend-b
      kind: http
      url: %s
      allow_paths:
        - "/b/*"
      allow_methods:
        - GET
      header_mode: passthrough
`, extraBackendURL)
	}
	yaml := fmt.Sprintf(`
gateway:
  address: localhost:50051
  insecure: true
service:
  implementation: test-impl
  specifier: test-spec
terminator:
  enabled: true
  backends:
    - name: backend-a
      kind: http
      url: %s
      allow_paths:
        - "/a/*"
      allow_methods:
        - GET
      header_mode: passthrough
%s`, backendURL, extra)

	f, err := os.CreateTemp(t.TempDir(), "proxy-sidecar-*.yaml")
	if err != nil {
		t.Fatalf("create temp config: %v", err)
	}
	if _, err := f.WriteString(yaml); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	f.Close()
	return f.Name()
}

// TestReload_AddBackend verifies that after a reload with an additional backend
// new requests can route to that backend.
func TestReload_AddBackend(t *testing.T) {
	// Backend A — accepts /a/* paths.
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend-a"))
	}))
	defer srvA.Close()

	// Backend B — accepts /b/* paths.
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend-b"))
	}))
	defer srvB.Close()

	// Initial config: only backend A.
	cfgPath := writeReloadConfig(t, srvA.URL, "")

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	term, err := NewTerminatorFromPath(cfg, cfgPath)
	if err != nil {
		t.Fatalf("new terminator: %v", err)
	}

	// Before reload: /b/* request should get ACL_DENIED.
	req := &pb.ProxyHttpRequest{RequestId: "req-1", Method: "GET", Path: "/b/thing"}
	resp, _ := term.HandleProxyRequest(t.Context(), req, nil)
	if resp.GetError() == nil || resp.GetError().GetKind() != pb.ProxyError_ACL_DENIED {
		t.Fatalf("expected ACL_DENIED before reload, got %v", resp)
	}

	// Rewrite config to include both backends and reload.
	if err := os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
gateway:
  address: localhost:50051
  insecure: true
service:
  implementation: test-impl
  specifier: test-spec
terminator:
  enabled: true
  backends:
    - name: backend-a
      kind: http
      url: %s
      allow_paths:
        - "/a/*"
      allow_methods:
        - GET
      header_mode: passthrough
    - name: backend-b
      kind: http
      url: %s
      allow_paths:
        - "/b/*"
      allow_methods:
        - GET
      header_mode: passthrough
`, srvA.URL, srvB.URL)), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}

	term.Reload()

	// After reload: /b/* should route to backend B.
	req2 := &pb.ProxyHttpRequest{RequestId: "req-2", Method: "GET", Path: "/b/thing"}
	resp2, body2 := term.HandleProxyRequest(t.Context(), req2, nil)
	if resp2.GetError() != nil {
		t.Fatalf("expected success after reload, got error: %v", resp2.GetError())
	}
	if string(body2) != "backend-b" {
		t.Fatalf("expected body 'backend-b', got %q", string(body2))
	}

	// /a/* still works.
	req3 := &pb.ProxyHttpRequest{RequestId: "req-3", Method: "GET", Path: "/a/thing"}
	resp3, body3 := term.HandleProxyRequest(t.Context(), req3, nil)
	if resp3.GetError() != nil {
		t.Fatalf("expected success for /a/ after reload, got error: %v", resp3.GetError())
	}
	if string(body3) != "backend-a" {
		t.Fatalf("expected body 'backend-a', got %q", string(body3))
	}
}

// TestReload_InvalidConfig verifies that a reload with invalid YAML keeps the
// original config intact.
func TestReload_InvalidConfig(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cfgPath := writeReloadConfig(t, srv.URL, "")
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	term, err := NewTerminatorFromPath(cfg, cfgPath)
	if err != nil {
		t.Fatalf("new terminator: %v", err)
	}

	// Overwrite with invalid YAML.
	if err := os.WriteFile(cfgPath, []byte(":::invalid yaml:::"), 0o600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}

	term.Reload() // should log error and keep old config

	// Original backend A still serves /a/*.
	req := &pb.ProxyHttpRequest{RequestId: "req-1", Method: "GET", Path: "/a/test"}
	resp, body := term.HandleProxyRequest(t.Context(), req, nil)
	if resp.GetError() != nil {
		t.Fatalf("expected success after failed reload, got: %v", resp.GetError())
	}
	if string(body) != "ok" {
		t.Fatalf("expected 'ok', got %q", string(body))
	}
}

// TestReload_SurfaceDisable verifies that attempting to flip
// terminator.enabled to false on reload is rejected and the old config is
// kept (surface enable/disable is not reloadable).
func TestReload_SurfaceDisable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cfgPath := writeReloadConfig(t, srv.URL, "")
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	term, err := NewTerminatorFromPath(cfg, cfgPath)
	if err != nil {
		t.Fatalf("new terminator: %v", err)
	}

	// Overwrite config to disable the terminator surface and enable
	// initiator (a hypothetical future deployment topology). The reload
	// must refuse this because surface enable/disable is not reloadable.
	if err := os.WriteFile(cfgPath, []byte(`
gateway:
  address: localhost:50051
  insecure: true
service:
  implementation: test-impl
  specifier: test-spec
terminator:
  enabled: false
initiator:
  enabled: true
  listen:
    bind: localhost:8888
  target:
    topic: sv::test::spec
`), 0o600); err != nil {
		t.Fatalf("write surface-flip config: %v", err)
	}

	term.Reload() // should reject the disable

	// Terminator still has original backends.
	term.backendMu.RLock()
	backendCount := len(term.backends)
	term.backendMu.RUnlock()
	if backendCount != 1 {
		t.Fatalf("expected 1 backend after rejected surface flip, got %d", backendCount)
	}

	// Original requests still work.
	req := &pb.ProxyHttpRequest{RequestId: "req-1", Method: "GET", Path: "/a/test"}
	resp, _ := term.HandleProxyRequest(t.Context(), req, nil)
	if resp.GetError() != nil {
		t.Fatalf("expected success after rejected surface flip, got: %v", resp.GetError())
	}
}

// TestReload_DevDefaults verifies that Reload is a no-op when cfgPath is empty
// (dev-defaults mode) and does not panic.
func TestReload_DevDefaults(t *testing.T) {
	cfg := &Config{
		Gateway: GatewayConfig{
			Address:  "localhost:50051",
			Insecure: true,
		},
		Service: ServiceConfig{
			Implementation: "test",
			Specifier:      "spec",
		},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Name:       "default",
				Kind:       BackendKindHTTP,
				URL:        "http://localhost:9999",
				AllowPaths: []string{"/*"},
			}},
		},
	}
	_ = cfg.Validate()
	term, err := NewTerminator(cfg) // no cfgPath
	if err != nil {
		t.Fatalf("new terminator: %v", err)
	}
	// Should not panic or crash.
	term.Reload()
}
