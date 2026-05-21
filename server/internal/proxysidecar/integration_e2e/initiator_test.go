//go:build e2e

package integration_e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Initiator harness
// =============================================================================
//
// The initiator surface accepts plain HTTP on a local TCP listener and
// translates each request into a ProxyHttpRequest envelope addressed at
// a configured target service topic, via an injected proxyDispatcher.
//
// For e2e validation we want the dispatcher to actually round-trip
// through the existing composite-mode sidecar + fake gateway instead
// of mocking it out — that proves the surface's end-to-end wiring.
// We therefore:
//
//   1. Build a standard E2EHarness (gateway + composite sidecar).
//   2. Dial an aether.AgentClient against that gateway.
//   3. Wrap the client in a dispatcher that satisfies the initiator's
//      unexported proxyDispatcher interface (we are in the same package).
//   4. Spin up an Initiator with SetDispatcher and a local HTTP listener.
//   5. Issue plain HTTP requests to the initiator and assert the full
//      round-trip lands at the in-process HTTP backend behind the
//      sidecar's terminator.

// initiatorHarness bundles the shared E2EHarness with an initiator-only
// sidecar surface fronting it.
type initiatorHarness struct {
	Shared        *E2EHarness
	InitiatorAddr string // host:port the initiator listens on
}

// agentDispatcher wraps an *aether.AgentClient so it satisfies the
// initiator's unexported proxyDispatcher interface. The initiator
// invokes ProxyHTTP(ctx, target, req) — exactly the AgentClient's own
// signature minus options — so the wrapper is a thin pass-through.
type agentDispatcher struct {
	client *aether.AgentClient
}

// ProxyHTTP satisfies the unexported proxyDispatcher interface in the
// proxysidecar package. We do not import the interface directly (it is
// unexported); we just match the signature.
func (a *agentDispatcher) ProxyHTTP(ctx context.Context, target string, req *http.Request) (*http.Response, error) {
	return a.client.ProxyHTTP(ctx, target, req, aether.WithBackend("local"))
}

func newInitiatorHarness(t *testing.T) *initiatorHarness {
	t.Helper()

	shared := NewE2EHarness(t, E2EHarnessOptions{
		Implementation: "bp-sidecar",
		Specifier:      "e2e-initiator",
	})

	// Dial an SDK client against the same fake gateway the composite
	// sidecar is attached to. This client owns the gateway-side identity
	// the initiator's dispatched envelopes are routed under.
	client := dialAgentClient(t, shared, "initiator-bridge")

	// Pick a free local port for the initiator listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("initiator listener: %v", err)
	}
	initAddr := ln.Addr().String()
	_ = ln.Close() // release; the initiator will rebind. Race-free in practice for ephemeral ports.

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  shared.GatewayAddr,
			Insecure: true,
		},
		Initiator: proxysidecar.InitiatorConfig{
			Enabled: true,
			Listen:  proxysidecar.ListenConfig{Bind: initAddr},
			Target:  proxysidecar.TargetConfig{Topic: shared.ServiceTopic},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("initiator cfg validate: %v", err)
	}

	init, err := proxysidecar.NewInitiator(cfg)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	init.SetDispatcher(&agentDispatcher{client: client})

	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- init.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Logf("warning: initiator did not exit within 5s of cancel")
		}
	})

	// Wait for the initiator listener to come up.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", initAddr, 250*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &initiatorHarness{
		Shared:        shared,
		InitiatorAddr: initAddr,
	}
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_Initiator_LocalHTTPToUpstreamService issues a plain HTTP GET
// against the initiator's local listener and asserts the request is
// translated into a ProxyHttpRequest, routed through the fake gateway
// to the composite sidecar's terminator, dispatched to the in-process
// HTTP backend, and the response flows back unchanged.
func TestE2E_Initiator_LocalHTTPToUpstreamService(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newInitiatorHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s/fast", h.InitiatorAddr)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("initiator GET /fast: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(body, []byte(`"ok":true`)) {
		t.Errorf("body=%q does not contain ok:true", string(body))
	}
}

// TestE2E_Initiator_LargeRequest_ChunkedUpload POSTs a 1 MiB body
// through the initiator. The upstream side should reassemble the
// chunked request body and echo it back via /echo.
//
// The initiator reads the request body fully and hands the resulting
// outbound *http.Request to the dispatcher, which is the SDK's
// ProxyHTTP path; the SDK then splits bodies > 256 KiB into
// ProxyHttpBodyChunk frames. The terminator reassembles them server-
// side. This is the full initiator → SDK chunked upload code path.
func TestE2E_Initiator_LargeRequest_ChunkedUpload(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	const uploadSize = 1 * 1024 * 1024 // 1 MiB

	h := newInitiatorHarness(t)

	payload := make([]byte, uploadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s/echo", h.InitiatorAddr)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(payload))

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("initiator POST /echo: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("echo mismatch: got %d bytes, want %d bytes", len(got), len(payload))
		if len(got) > 0 && len(payload) > 0 {
			t.Logf("first 32: got=%x want=%x", got[:32], payload[:32])
		}
	}
	t.Logf("initiator chunked upload: %d bytes in %s", len(payload), time.Since(start))
}
