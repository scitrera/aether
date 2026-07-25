//go:build e2e

package integration_e2e

// http_edge_methods_test.go — coverage for §1 matrix gaps (lines 77–81):
//   HEAD method, OPTIONS method, PATCH method, 0-byte body POST,
//   query string preservation.
//
// All five tests use edgeMethodsHarness, a local harness that wires the same
// in-process HTTP backend approach as extE2EHarness (http_variants_test.go)
// but expands AllowMethods to include HEAD, OPTIONS, and PATCH, and mounts
// the additional backend routes needed for these edge cases.
//
// No t.Parallel() — matches the convention in http_variants_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/aether/server/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// edgeMethodsHarness — local test fixture for HTTP method / body / URL tests
// =============================================================================

type edgeMethodsHarness struct {
	GatewayAddr  string
	ServiceTopic string
}

func newEdgeMethodsHarness(t *testing.T) *edgeMethodsHarness {
	t.Helper()

	gw := getAetherlite(t)
	gwAddr := gw.grpcAddr

	backend := newEdgeMethodsBackend(t)

	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	relayListen := "unix://" + relayPath

	uniqueSpec := fmt.Sprintf("e2e-http-edge-%d", nextSidecarSpec.Add(1))

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gwAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "bp-sidecar",
			Specifier:      uniqueSpec,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:         "local",
				Kind:         proxysidecar.BackendKindHTTP,
				URL:          backend.URL,
				AllowPaths:   []string{"/*"},
				AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"},
				MaxBodyBytes: 32 << 20,
				// IdleTimeoutMs intentionally left 0 — config.go default fills it in.
				HeaderMode: proxysidecar.HeaderModePassthrough,
			}},
		},
		Relay: proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  relayListen,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
		},
		TenantID: "tenant-e2e-edge",
	}

	runner, err := proxysidecar.NewRunner(cfg, "")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		_ = runner.Run(runCtx)
	}()
	t.Cleanup(func() {
		runCancel()
		select {
		case <-runnerDone:
		case <-time.After(15 * time.Second):
			t.Logf("warning: runner did not exit within 15s of cancel")
		}
	})

	serviceTopic := fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
	if err := waitForSidecarReady(t, gwAddr, serviceTopic, backend.URL); err != nil {
		t.Fatalf("edge sidecar never reached ready state: %v", err)
	}

	return &edgeMethodsHarness{
		GatewayAddr:  gwAddr,
		ServiceTopic: serviceTopic,
	}
}

// newEdgeMethodsBackend mounts the routes needed by the five edge-case tests.
// It is intentionally separate from newExtHTTPBackend (http_variants_test.go)
// to keep each test file self-contained and to avoid modifying the shared
// harness.
func newEdgeMethodsBackend(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// /fast — required by waitForSidecarReady which probes GET /fast.
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})

	// /head-target — simulates a resource with a known Content-Length.
	// The handler always sets Content-Length: 42 and writes a 42-byte body.
	// When Go's net/http serves a HEAD request it will strip the body bytes
	// from the wire but preserve the Content-Length header, which is exactly
	// the HTTP/1.1 HEAD semantics we want to verify end-to-end.
	mux.HandleFunc("/head-target", func(w http.ResponseWriter, r *http.Request) {
		const bodyStr = "0123456789012345678901234567890123456789ab" // 42 bytes
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "42")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, bodyStr)
	})

	// /options-target — returns 204 with an Allow header listing the
	// methods this resource accepts.
	mux.HandleFunc("/options-target", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", "GET, POST, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
	})

	// /patch-echo — echoes the request method and body back as JSON so the
	// test can assert both the method was preserved and the body round-tripped.
	mux.HandleFunc("/patch-echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := map[string]string{
			"method": r.Method,
			"body":   string(body),
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	// /empty-post — records the Content-Length the backend saw (from the
	// parsed request) and echoes it back.  An empty POST should arrive with
	// Content-Length 0 (or no body) and produce a clean response.
	mux.HandleFunc("/empty-post", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := map[string]interface{}{
			"method":          r.Method,
			"content_length":  r.ContentLength,
			"body_bytes_read": len(body),
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	// /query-echo — records r.URL.RawQuery (preserving exact encoding) and
	// the parsed key/value pairs.
	mux.HandleFunc("/query-echo", func(w http.ResponseWriter, r *http.Request) {
		parsed := make(map[string]string)
		for k, vals := range r.URL.Query() {
			if len(vals) > 0 {
				parsed[k] = vals[0]
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := map[string]interface{}{
			"raw_query": r.URL.RawQuery,
			"parsed":    parsed,
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_HTTPMethod_HEAD_RoundTrip issues a HEAD request against a backend
// that would return a 42-byte body for GET. The proxy must forward status 200,
// an empty response body, and the Content-Length: 42 header intact.
func TestE2E_HTTPMethod_HEAD_RoundTrip(t *testing.T) {
	h := newEdgeMethodsHarness(t)
	client := dialAgentClientToAddr(t, h.GatewayAddr, "head-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "HEAD", "http://ignored/head-target", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP HEAD: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	// HEAD response body must be empty — the sidecar must not smuggle body
	// bytes through even if the backend handler wrote them (Go's net/http
	// strips them on HEAD, but the proxy chain must not re-add them).
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll body: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("HEAD response body: got %d bytes %q, want empty", len(body), body)
	}

	// Content-Length header must be preserved as "42" so callers can use it
	// to pre-allocate without issuing a GET.
	cl := resp.Header.Get("Content-Length")
	if cl != "42" {
		t.Errorf("Content-Length header: got %q, want %q", cl, "42")
	}
}

// TestE2E_HTTPMethod_OPTIONS_RoundTrip issues an OPTIONS request and asserts
// that the Allow header set by the backend is preserved by the proxy and the
// status code (204) round-trips correctly.
func TestE2E_HTTPMethod_OPTIONS_RoundTrip(t *testing.T) {
	h := newEdgeMethodsHarness(t)
	client := dialAgentClientToAddr(t, h.GatewayAddr, "options-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "OPTIONS", "http://ignored/options-target", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP OPTIONS: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", resp.StatusCode)
	}

	allow := resp.Header.Get("Allow")
	if allow == "" {
		t.Fatalf("Allow header missing from OPTIONS response")
	}
	// The backend returns "GET, POST, OPTIONS" — verify all three are present
	// without being brittle about ordering.
	for _, method := range []string{"GET", "POST", "OPTIONS"} {
		if !strings.Contains(allow, method) {
			t.Errorf("Allow header %q does not contain %q", allow, method)
		}
	}
}

// TestE2E_HTTPMethod_PATCH_BodyRoundTrip issues a PATCH request with a small
// JSON body. The backend echoes the received method and body back. The test
// asserts the sidecar preserved both the PATCH method and the request body,
// and that the response round-trips correctly.
func TestE2E_HTTPMethod_PATCH_BodyRoundTrip(t *testing.T) {
	h := newEdgeMethodsHarness(t)
	client := dialAgentClientToAddr(t, h.GatewayAddr, "patch-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	patchBody := `{"op":"replace","path":"/name","value":"updated"}`
	req, err := http.NewRequestWithContext(ctx, "PATCH", "http://ignored/patch-echo",
		strings.NewReader(patchBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP PATCH: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(rawBody, &got); err != nil {
		t.Fatalf("Unmarshal %q: %v", string(rawBody), err)
	}

	if got["method"] != "PATCH" {
		t.Errorf("backend saw method=%q, want PATCH", got["method"])
	}
	if got["body"] != patchBody {
		t.Errorf("backend saw body=%q, want %q", got["body"], patchBody)
	}
}

// TestE2E_HTTPBody_ZeroByte_POST issues a POST with an empty body. The backend
// records what it received; the test asserts no error from the proxy and that
// the backend observed a zero-byte body (proxy must not inject spurious bytes).
// The response body (a small JSON object) must also round-trip intact.
func TestE2E_HTTPBody_ZeroByte_POST(t *testing.T) {
	h := newEdgeMethodsHarness(t)
	client := dialAgentClientToAddr(t, h.GatewayAddr, "empty-post-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", "http://ignored/empty-post",
		strings.NewReader(""))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP empty POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(rawBody) == 0 {
		t.Fatalf("response body is empty — backend should have returned a JSON object")
	}

	var got map[string]interface{}
	if err := json.Unmarshal(rawBody, &got); err != nil {
		t.Fatalf("Unmarshal %q: %v", string(rawBody), err)
	}

	if method, _ := got["method"].(string); method != "POST" {
		t.Errorf("backend saw method=%q, want POST", method)
	}

	// body_bytes_read must be 0 — the proxy must not inject bytes.
	bytesRead, _ := got["body_bytes_read"].(float64)
	if bytesRead != 0 {
		t.Errorf("backend read %v body bytes, want 0 (proxy must not inject bytes into empty body)", bytesRead)
	}
}

// TestE2E_HTTPRequest_QueryString_Preserved issues GET /query-echo with a
// multi-key query string that includes a URL-encoded value (%20 for space).
// The test asserts the backend received the raw query string byte-for-byte
// identical and that all three key/value pairs parsed correctly.
func TestE2E_HTTPRequest_QueryString_Preserved(t *testing.T) {
	h := newEdgeMethodsHarness(t)
	client := dialAgentClientToAddr(t, h.GatewayAddr, "querystring-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The path includes a raw query string. http.NewRequestWithContext parses
	// the URL so the SDK receives the path + query as the caller intends.
	const rawQuery = "foo=bar&baz=qux&special=hello%20world"
	reqURL := "http://ignored/query-echo?" + rawQuery

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP query string: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(rawBody, &got); err != nil {
		t.Fatalf("Unmarshal %q: %v", string(rawBody), err)
	}

	// raw_query must be preserved exactly — same bytes including %20.
	gotRaw, _ := got["raw_query"].(string)
	if gotRaw != rawQuery {
		t.Errorf("raw_query=%q, want %q (encoding must not be normalised by proxy)", gotRaw, rawQuery)
	}

	// All three key/value pairs must be present and parsed correctly.
	parsed, _ := got["parsed"].(map[string]interface{})
	cases := []struct{ key, want string }{
		{"foo", "bar"},
		{"baz", "qux"},
		{"special", "hello world"}, // %20 decoded by net/url
	}
	for _, c := range cases {
		got, _ := parsed[c.key].(string)
		if got != c.want {
			t.Errorf("parsed[%q]=%q, want %q", c.key, got, c.want)
		}
	}
}
