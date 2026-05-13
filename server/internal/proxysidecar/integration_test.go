package proxysidecar

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// TestTerminator_RoundTrip_GET asserts a basic GET request against an
// httptest.Server backend completes end-to-end through the terminator's
// HandleProxyRequest entry point.
func TestTerminator_RoundTrip_GET(t *testing.T) {
	t.Parallel()

	var seenHeaders http.Header
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL
	cfg.Terminator.Backends[0].HeaderMode = HeaderModeStrict

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId: "r-get",
		Method:    "GET",
		Path:      "/v1/ping",
		Headers: map[string]string{
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
	}
	resp, body := t1.HandleProxyRequest(context.Background(), req, nil)
	if resp.GetError() != nil {
		t.Fatalf("unexpected error: %s: %s", resp.GetError().GetKind(), resp.GetError().GetMessage())
	}
	if resp.GetStatusCode() != 200 {
		t.Errorf("status: got %d, want 200", resp.GetStatusCode())
	}
	if got := string(body); got != `{"ok":true}` {
		t.Errorf("body: got %q, want %q", got, `{"ok":true}`)
	}
	if got := resp.GetHeaders()["Content-Type"]; got != "application/json" {
		t.Errorf("Content-Type header: got %q", got)
	}

	// The backend should have observed the minted X-Auth-* headers.
	if got := seenHeaders.Get(identityheaders.HeaderTenantID); got != "tenant-test" {
		t.Errorf("backend X-Auth-Tenant-ID: got %q, want %q", got, "tenant-test")
	}
	if got := seenHeaders.Get(identityheaders.HeaderUserID); got != "alice" {
		t.Errorf("backend X-Auth-User-ID: got %q, want %q", got, "alice")
	}
}

// TestTerminator_RoundTrip_POST_WithBody asserts a POST request with a JSON
// body is forwarded verbatim and the backend response is captured.
func TestTerminator_RoundTrip_POST_WithBody(t *testing.T) {
	t.Parallel()

	var receivedBody []byte
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(201)
		_, _ = io.WriteString(w, `{"created":true}`)
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	body := []byte(`{"hello":"world"}`)
	req := &pb.ProxyHttpRequest{
		RequestId: "r-post",
		Method:    "POST",
		Path:      "/v1/items",
		Headers:   map[string]string{"Content-Type": "application/json"},
	}
	resp, respBody := t1.HandleProxyRequest(context.Background(), req, body)
	if resp.GetError() != nil {
		t.Fatalf("unexpected error: %v", resp.GetError())
	}
	if resp.GetStatusCode() != 201 {
		t.Errorf("status: got %d, want 201", resp.GetStatusCode())
	}
	if !bytes.Equal(receivedBody, body) {
		t.Errorf("backend received %q, want %q", string(receivedBody), string(body))
	}
	if string(respBody) != `{"created":true}` {
		t.Errorf("response body: got %q", string(respBody))
	}
}

// TestTerminator_RoundTrip_BackendUnreachable_DialFailed asserts that an
// unreachable backend yields ProxyError DIAL_FAILED.
func TestTerminator_RoundTrip_BackendUnreachable_DialFailed(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = "http://127.0.0.1:1" // closed port

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId: "r-dial",
		Method:    "GET",
		Path:      "/anything",
	}
	resp, _ := t1.HandleProxyRequest(context.Background(), req, nil)
	if resp.GetError() == nil {
		t.Fatal("expected ProxyError")
	}
	if k := resp.GetError().GetKind(); k != pb.ProxyError_DIAL_FAILED && k != pb.ProxyError_TIMEOUT {
		t.Errorf("got %s, want DIAL_FAILED or TIMEOUT", k)
	}
}

// TestTerminator_TunnelStubsReturnUnimplementedClose asserts the v1 tunnel
// stubs respond with an error TunnelClose noting the deferred phase.
func TestTerminator_TunnelStubsReturnUnimplementedClose(t *testing.T) {
	t.Parallel()

	t1, err := NewTerminator(terminatorTestConfig())
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	close := t1.HandleTunnelOpen(context.Background(), &pb.TunnelOpen{
		TunnelId: "tn-1",
		Protocol: pb.TunnelOpen_TCP,
	}, nil)
	if close.GetReason() != pb.TunnelClose_ERROR {
		t.Errorf("reason: got %s, want ERROR", close.GetReason())
	}
	if !strings.Contains(close.GetDetail(), "not implemented") {
		t.Errorf("detail: %q should mention 'not implemented'", close.GetDetail())
	}
	if close.GetTunnelId() != "tn-1" {
		t.Errorf("tunnel id echoed: got %q, want %q", close.GetTunnelId(), "tn-1")
	}
}
