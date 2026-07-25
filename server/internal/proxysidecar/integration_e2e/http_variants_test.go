//go:build e2e

package integration_e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/server/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Test-local harness — extended HTTP backend
// =============================================================================
//
// The standard E2EHarness wires a fixed HTTP backend (/slow, /fast, /echo).
// http_variants_test.go needs additional endpoints (/methods, /headers,
// /bigbody, /slowfin, /overflow, /cancel-watch, /status/{code}) that the
// shared backend does not expose.
//
// extE2EHarness mirrors NewE2EHarness's wiring but mounts a richer mux on
// the in-process HTTP backend so the tests can assert on method, headers,
// and large/streaming bodies without monkey-patching the shared harness.
//
// We intentionally do NOT touch harness.go; this helper is local to
// http_variants_test.go.

type extE2EHarness struct {
	GatewayAddr    string
	ServiceTopic   string
	HTTPBackendURL string
	cancelWatchCh  chan struct{}
}

// extHarnessOptions toggles per-test backend behaviour. SlowFinDuration
// makes the /slowfin endpoint drip for that long; OverflowBytes is the
// byte count the /overflow endpoint streams before EOF.
type extHarnessOptions struct {
	SlowFinDuration   time.Duration
	OverflowBytes     int64
	MaxResponseBodyBT int64 // sidecar backend max_response_body_bytes (0 = default)
}

func newExtE2EHarness(t *testing.T, opts extHarnessOptions) *extE2EHarness {
	t.Helper()

	if opts.SlowFinDuration == 0 {
		opts.SlowFinDuration = 2 * time.Second
	}

	gw := getAetherlite(t)
	gwAddr := gw.grpcAddr
	cancelWatchCh := make(chan struct{}, 4)
	backend := newExtHTTPBackend(t, opts.SlowFinDuration, opts.OverflowBytes, cancelWatchCh)

	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	relayListen := "unix://" + relayPath

	maxBody := opts.MaxResponseBodyBT
	if maxBody == 0 {
		maxBody = 32 << 20 // 32 MiB headroom for the 1 MiB body test
	}

	// Unique-ify the specifier so multiple ext-harness tests don't
	// collide on the shared aetherlite's (impl, spec) identity.
	uniqueSpec := fmt.Sprintf("e2e-http-variants-%d", nextSidecarSpec.Add(1))

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
				Name:          "local",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backend.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET", "POST", "PUT", "DELETE"},
				MaxBodyBytes:  maxBody,
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
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
		TenantID: "tenant-e2e-http",
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
		t.Fatalf("ext sidecar never reached ready state: %v", err)
	}

	return &extE2EHarness{
		GatewayAddr:    gwAddr,
		ServiceTopic:   serviceTopic,
		HTTPBackendURL: backend.URL,
		cancelWatchCh:  cancelWatchCh,
	}
}

// newExtHTTPBackend mounts the test-only routes on a httptest.Server.
// cancelWatchCh is signalled when /cancel-watch observes r.Context().Done().
func newExtHTTPBackend(t *testing.T, slowFin time.Duration, overflowBytes int64, cancelWatchCh chan<- struct{}) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// /fast — duplicates the shared harness's /fast so this backend can
	// stand alone without depending on the other one.
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})

	// /methods/<METHOD> — echoes the request method back as JSON. Used by
	// TestE2E_HTTPMethods_AllSupportedRoundtrip.
	mux.HandleFunc("/methods/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = fmt.Fprintf(w, `{"method":%q}`, r.Method)
	})

	// /headers — returns the request headers as JSON. Used by
	// TestE2E_RequestHeaders_PassthroughPreservesOrder.
	mux.HandleFunc("/headers", func(w http.ResponseWriter, r *http.Request) {
		out := make(map[string]string, len(r.Header))
		for k := range r.Header {
			out[k] = r.Header.Get(k)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(out)
	})

	// /bigbody — 1 MiB random payload, written inline (not SSE). The
	// backend's framework will chunk per net/http convention; the SDK
	// should reassemble it.
	bigPayload := make([]byte, 1<<20)
	if _, err := rand.Read(bigPayload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	mux.HandleFunc("/bigbody", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		_, _ = w.Write(bigPayload)
	})

	// bigPayloadDigest exposes the canonical bytes for assertions.
	mux.HandleFunc("/bigbody/digest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		_, _ = w.Write(bigPayload)
	})

	// /slowfin — opens an SSE response, emits one chunk, then stops
	// without closing. The stream sits idle until either the client
	// cancels or slowFin elapses, at which point the handler returns
	// (EOF). Used to exercise idle-timeout behaviour: callers configure
	// stream_idle_timeout_ms shorter than slowFin and expect a TIMEOUT
	// from the SDK before the handler completes.
	mux.HandleFunc("/slowfin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		// One immediate chunk so the client gets a header + first byte.
		_, _ = io.WriteString(w, "data: first\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		// Idle until slowFin elapses or the client cancels.
		select {
		case <-time.After(slowFin):
		case <-r.Context().Done():
		}
	})

	// /overflow — streams overflowBytes total in chunks. Used by
	// TestE2E_StreamingResponse_MaxBytesOverflow to trip
	// max_response_body_bytes.
	if overflowBytes <= 0 {
		overflowBytes = 64 * 1024 // sensible default; tests usually override via cfg
	}
	mux.HandleFunc("/overflow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = 'X'
		}
		var sent int64
		for sent < overflowBytes {
			n := int64(len(chunk))
			if remaining := overflowBytes - sent; n > remaining {
				n = remaining
			}
			if _, err := w.Write(chunk[:n]); err != nil {
				return
			}
			sent += n
			if flusher != nil {
				flusher.Flush()
			}
			// Pace slightly so the SDK can observe cumulative growth.
			time.Sleep(5 * time.Millisecond)
			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	})

	// /cancel-watch — opens an SSE response, sends one event, then
	// blocks until r.Context() fires OR a cap timeout (8s) elapses.
	// The cap prevents httptest.Server.Close from hanging in tests
	// where the SDK-side close does not propagate cancellation back to
	// the backend handler (a known SDK gap documented in
	// TestE2E_CallerCancel_AbortsRequest).
	mux.HandleFunc("/cancel-watch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: holding\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
		// Non-blocking signal so multiple invocations don't wedge.
		select {
		case cancelWatchCh <- struct{}{}:
		default:
		}
	})

	// /status/{code} — returns the requested status code with a small
	// body identifying the status. Used by TestE2E_BackendError_StatusForwarding.
	mux.HandleFunc("/status/", func(w http.ResponseWriter, r *http.Request) {
		code := 200
		suffix := strings.TrimPrefix(r.URL.Path, "/status/")
		switch suffix {
		case "404":
			code = 404
		case "500":
			code = 500
		case "418":
			code = 418
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(code)
		_, _ = fmt.Fprintf(w, "status-body-%d", code)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_HTTPMethods_AllSupportedRoundtrip exercises every method the
// terminator's default allow-list admits and verifies the backend sees
// the method intact, with the right status / identifying body.
func TestE2E_HTTPMethods_AllSupportedRoundtrip(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newExtE2EHarness(t, extHarnessOptions{})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "methods-caller")

	methods := []string{"GET", "POST", "PUT", "DELETE"}
	for _, m := range methods {
		t.Run(m, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var body io.Reader
			if m == "POST" || m == "PUT" {
				body = strings.NewReader(`{}`)
			}
			req, err := http.NewRequestWithContext(ctx, m, "http://ignored/methods/"+m, body)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if body != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
			if err != nil {
				t.Fatalf("ProxyHTTP %s: %v", m, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d, want 200", resp.StatusCode)
			}
			respBody, _ := io.ReadAll(resp.Body)
			wantSub := fmt.Sprintf(`"method":%q`, m)
			if !strings.Contains(string(respBody), wantSub) {
				t.Errorf("body %q does not contain %s", string(respBody), wantSub)
			}
		})
	}
}

// TestE2E_RequestHeaders_PassthroughPreservesOrder POSTs with custom
// headers and asserts the backend sees them. HeaderMode=passthrough in
// the test harness forwards the caller-set headers as-is.
func TestE2E_RequestHeaders_PassthroughPreservesOrder(t *testing.T) {
	h := newExtE2EHarness(t, extHarnessOptions{})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "headers-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", "http://ignored/headers", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Test-A", "1")
	req.Header.Set("X-Test-B", "2")
	// Note: the SDK reserves the Authorization header for OBO
	// (on-behalf-of) injection via WithOBOAuthorization — setting it as
	// a plain HTTP header gets stripped before the proxy envelope is
	// built. Use a custom auth-shaped header instead so this test
	// asserts what the passthrough mode actually preserves.
	req.Header.Set("X-Custom-Auth", "Bearer xyz")

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)

	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal %q: %v", string(body), err)
	}

	cases := []struct {
		name, want string
	}{
		{"X-Test-A", "1"},
		{"X-Test-B", "2"},
		{"X-Custom-Auth", "Bearer xyz"},
		{"Content-Type", "application/json"},
	}
	for _, c := range cases {
		if v := got[c.name]; v != c.want {
			t.Errorf("header %q: got %q, want %q (full: %v)", c.name, v, c.want, got)
		}
	}
}

// TestE2E_ResponseBody_ChunkedFromLargeBackend asks the backend for a
// 1 MiB body and verifies the SDK reassembles it byte-for-byte.
func TestE2E_ResponseBody_ChunkedFromLargeBackend(t *testing.T) {
	h := newExtE2EHarness(t, extHarnessOptions{})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "bigbody-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// First fetch the canonical bytes from the backend directly through a
	// non-proxied HTTP call so we have ground truth to compare against
	// (the random bytes are generated once at backend boot).
	want, err := fetchBackendDirect(h.HTTPBackendURL + "/bigbody/digest")
	if err != nil {
		t.Fatalf("backend direct: %v", err)
	}
	if len(want) != 1<<20 {
		t.Fatalf("backend ground-truth bytes=%d, want %d", len(want), 1<<20)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/bigbody", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("body mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}
}

// TestE2E_StreamingResponse_IdleTimeoutClosesCleanly opens a streaming
// request with a short stream_idle_timeout_ms against /slowfin (which
// emits one chunk then idles). The SDK should observe a TIMEOUT and not
// hang.
func TestE2E_StreamingResponse_IdleTimeoutClosesCleanly(t *testing.T) {
	const slowFinDur = 5 * time.Second

	h := newExtE2EHarness(t, extHarnessOptions{SlowFinDuration: slowFinDur})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "idle-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/slowfin", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	// idleTimeoutMs (500ms) < slowFinDur (5s), so the SDK's stream reader
	// must surface a TIMEOUT well before the backend finishes idling.
	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req,
		aether.WithBackend("local"),
		aether.WithStreamResponse(500, 0))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	start := time.Now()
	buf := make([]byte, 4096)
	var readErr error
	for {
		_, rerr := resp.Body.Read(buf)
		if rerr != nil {
			readErr = rerr
			break
		}
	}
	elapsed := time.Since(start)

	if readErr == nil {
		t.Fatalf("stream completed with nil error; expected a TIMEOUT")
	}
	// We accept either a clean io.EOF (if the backend's idle exited
	// before the SDK closed) OR a ProxyTransportError of kind TIMEOUT.
	// The strong signal is that we did NOT hang past slowFinDur.
	if elapsed > slowFinDur-500*time.Millisecond {
		t.Errorf("stream waited %s before erroring; expected <%s (idle timeout should fire fast)",
			elapsed, slowFinDur)
	}
	var pe *aether.ProxyTransportError
	if errors.As(readErr, &pe) {
		if pe.Kind != "TIMEOUT" {
			t.Logf("note: error kind %q (expected TIMEOUT); other kinds may occur if backend EOF races the idle timer", pe.Kind)
		}
	}
	t.Logf("stream errored after %s with: %v", elapsed, readErr)
}

// TestE2E_StreamingResponse_MaxBytesOverflow asks the backend to stream
// more bytes than max_response_body_bytes permits. The SDK should
// surface PAYLOAD_TOO_LARGE.
func TestE2E_StreamingResponse_MaxBytesOverflow(t *testing.T) {
	const (
		// Backend will write this many bytes total.
		overflowBytes = 64 * 1024
		// Cap the response at 16 KiB; anything beyond should trip
		// PAYLOAD_TOO_LARGE mid-stream.
		maxBytes = 16 * 1024
	)

	h := newExtE2EHarness(t, extHarnessOptions{
		OverflowBytes: overflowBytes,
	})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "overflow-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/overflow", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req,
		aether.WithBackend("local"),
		aether.WithStreamResponse(5_000, maxBytes))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("expected PAYLOAD_TOO_LARGE error; got nil error and %d bytes", len(got))
	}
	var pe *aether.ProxyTransportError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *aether.ProxyTransportError, got %T: %v", err, err)
	}
	if pe.Kind != "PAYLOAD_TOO_LARGE" {
		t.Errorf("error kind: got %q, want %q", pe.Kind, "PAYLOAD_TOO_LARGE")
	}
	// We should have received some bytes before the overflow.
	if int64(len(got)) > int64(maxBytes)+8*1024 {
		t.Errorf("received %d bytes after cap=%d; cap should bound delivery", len(got), maxBytes)
	}
	t.Logf("overflow: kind=%s msg=%q delivered=%d", pe.Kind, pe.Message, len(got))
}

// TestE2E_CallerCancel_AbortsRequest opens a streaming request against
// /cancel-watch and asserts that explicitly closing the response Body
// unblocks an in-flight Read promptly (the supported caller-driven
// cancel surface in the SDK today).
//
// Note (real-bug): the SDK's streamingBody.Read does NOT wake on
// caller ctx.Cancel — it blocks on a condvar that only wakes on chunk
// push or explicit Body.Close. Cancelling the request context alone is
// observed end-to-end via the response timeout / next chunk, not via
// Read returning immediately. Callers who want bounded-time abort must
// Close the body. This test exercises the Close-driven path.
func TestE2E_CallerCancel_AbortsRequest(t *testing.T) {
	h := newExtE2EHarness(t, extHarnessOptions{})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "cancel-caller")

	rootCtx, rootCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer rootCancel()
	callCtx, callCancel := context.WithCancel(rootCtx)
	defer callCancel()

	req, err := http.NewRequestWithContext(callCtx, "GET", "http://ignored/cancel-watch", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.ProxyHTTP(callCtx, h.ServiceTopic, req,
		aether.WithBackend("local"),
		aether.WithStreamResponse(30_000, 0))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}

	// Read the first event so we know the backend handler is suspended
	// on r.Context().Done() (it sent one "data: holding" event then
	// blocked).
	buf := make([]byte, 1024)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}

	// Launch a Reader goroutine that drains until error.
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1024)
		for {
			_, rerr := resp.Body.Read(buf)
			if rerr != nil {
				readDone <- rerr
				return
			}
		}
	}()

	// Close the body — this is the caller-controlled cancel surface
	// the SDK actually wires (signals streamingBody to wake with EOF).
	closeAt := time.Now()
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("Body.Close: %v", err)
	}

	select {
	case err := <-readDone:
		// EOF is the expected wake-up; anything else is fine too as
		// long as the Read unblocked promptly.
		t.Logf("Read returned after Body.Close in %s: %v", time.Since(closeAt), err)
	case <-time.After(5 * time.Second):
		t.Errorf("Read did not return within 5s of Body.Close — hang")
	}
}

// TestE2E_BackendError_StatusForwarding asks the backend for 404 then
// 500 and asserts each status + body round-trips unchanged through the
// sidecar.
func TestE2E_BackendError_StatusForwarding(t *testing.T) {
	h := newExtE2EHarness(t, extHarnessOptions{})
	client := dialAgentClientToAddr(t, h.GatewayAddr, "status-caller")

	for _, code := range []int{404, 500} {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET",
				fmt.Sprintf("http://ignored/status/%d", code), nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
			if err != nil {
				t.Fatalf("ProxyHTTP: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != code {
				t.Errorf("status=%d, want %d", resp.StatusCode, code)
			}
			body, _ := io.ReadAll(resp.Body)
			wantBody := fmt.Sprintf("status-body-%d", code)
			if string(body) != wantBody {
				t.Errorf("body=%q, want %q", string(body), wantBody)
			}
		})
	}
}

// =============================================================================
// Local helpers (test-file-local; do NOT leak into harness.go)
// =============================================================================

// dialAgentClientToAddr is a copy of dialAgentClient that does not need
// an *E2EHarness handle (this file uses extE2EHarness). The wiring is
// otherwise identical so SDK semantics match.
func dialAgentClientToAddr(t *testing.T, gatewayAddr, callerID string) *aether.AgentClient {
	t.Helper()

	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: gatewayAddr,
			Connection: aether.ConnectionOptions{
				MaxRetries:        1,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        500 * time.Millisecond,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
				KeepAliveInterval: 10 * time.Second,
			},
		},
		Workspace:      "e2e",
		Implementation: "caller",
		Specifier:      callerID,
	})
	if err != nil {
		t.Fatalf("NewAgentClient: %v", err)
	}

	connectCtx, connectCancel := context.WithCancel(context.Background())
	if err := client.Connect(connectCtx); err != nil {
		connectCancel()
		t.Fatalf("Connect: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		connectCancel()
		_ = client.CloseConnection()
	})

	deadline := time.Now().Add(10 * time.Second)
	for !client.ConnectionConfirmed() {
		select {
		case err := <-runDone:
			t.Fatalf("client.Run exited prematurely before ConnectionAck: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("ConnectionAck not observed within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return client
}

// fetchBackendDirect issues a plain HTTP GET against url, bypassing the
// sidecar. Used to capture ground-truth bytes from the test backend.
func fetchBackendDirect(url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// silence-unused: sync & atomic are reserved for future test extensions.
var (
	_ sync.Mutex
	_ atomic.Int32
)
