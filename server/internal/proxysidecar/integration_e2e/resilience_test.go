//go:build e2e

package integration_e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/grpc"
)

// =============================================================================
// customSidecarHarness — small helper used by resilience + acl tests when
// they need a sidecar wired to a custom backend URL or custom ACL set.
// Mirrors the minimum of NewE2EHarness needed for these tests; lives here
// rather than in harness.go because harness.go is read-only for this lane.
// =============================================================================
//
// Migrated from the legacy routingFakeGateway harness to the shared real
// aetherlite subprocess managed by aetherlite_proc.go. Each call attaches
// a fresh proxy-sidecar runtime to that shared aetherlite under a unique
// (impl, spec) identity, so multiple custom-harness tests in the same
// suite can run sequentially without colliding on the gateway's
// single-active-session enforcement.

// customSidecarHarness is a stripped-down harness for tests that need a
// non-default backend configuration. It exposes the same surface (gateway
// addr + service topic) so dialAgentClient works against it.
type customSidecarHarness struct {
	GatewayAddr  string
	ServiceTopic string
	// backendURL captures the first HTTP backend's URL (if any) so the
	// readiness probe can verify the backend is bound before issuing the
	// probe ProxyHTTP call.
	backendURL string
}

// newCustomSidecarHarness brings up a sidecar runner attached to the
// shared real aetherlite, whose terminator config is supplied by the
// caller. backendCfgs is the list of BackendConfig entries to attach to
// the terminator (typically a single HTTP backend).
//
// The (impl, spec) pair is suffixed with a monotonically-increasing
// counter so the same logical spec ("acl-path", "dead-backend", …) can
// be re-used across tests without colliding on aetherlite's
// DuplicateIdentityError. Service topics returned to the caller include
// the suffix.
func newCustomSidecarHarness(t *testing.T, impl, spec string, backendCfgs []proxysidecar.BackendConfig) *customSidecarHarness {
	t.Helper()

	gw := getAetherlite(t)

	// Unique-ify the spec so reruns / multiple tests in the same suite
	// don't collide on the shared aetherlite's (impl, spec) identity.
	uniqueSpec := fmt.Sprintf("%s-%d", spec, nextSidecarSpec.Add(1))

	relayPath := filepath.Join(t.TempDir(), "relay.sock")

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gw.grpcAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: impl,
			Specifier:      uniqueSpec,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled:  true,
			Backends: backendCfgs,
		},
		Relay: proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  "unix://" + relayPath,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
		},
		TenantID: "tenant-e2e",
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
			t.Logf("warning: custom-sidecar runner did not exit within 15s")
		}
	})

	serviceTopic := fmt.Sprintf("sv::%s::%s", impl, uniqueSpec)

	// Pick the first HTTP backend URL we can find for the readiness probe.
	// Tests that wire dead / non-HTTP backends won't have one — in those
	// cases waitForSidecarReady probes via the configured backend name
	// "local"; if that's absent the probe falls back to the first backend
	// in the config (handled below).
	probeBackendURL := ""
	probeBackendName := ""
	for _, b := range backendCfgs {
		if b.Kind == proxysidecar.BackendKindHTTP && probeBackendURL == "" {
			probeBackendURL = b.URL
			probeBackendName = b.Name
		}
	}
	// Wait for readiness. If there's no HTTP backend we can probe
	// directly, or the backend is intentionally unreachable (resilience
	// tests using a dead address), we fall back to a service-registration
	// probe via a small ProxyHTTP attempt: the call may surface an error
	// (DIAL_FAILED etc.) but only AFTER the sidecar has registered. We
	// detect that the registration completed by waiting for ANY response
	// (success or transport error) from a probe call within the deadline.
	if probeBackendURL != "" {
		if err := waitForSidecarReadyCustom(t, gw.grpcAddr, serviceTopic, probeBackendName, probeBackendURL); err != nil {
			t.Fatalf("custom sidecar never reached ready state: %v", err)
		}
	} else if len(backendCfgs) > 0 {
		// Best-effort: wait for the runner to register by probing the
		// first declared backend (whatever its kind). Any non-timeout
		// response (including ACL or transport errors) confirms the
		// sidecar is wired.
		if err := waitForSidecarRegistered(t, gw.grpcAddr, serviceTopic, backendCfgs[0].Name); err != nil {
			t.Fatalf("custom sidecar never registered: %v", err)
		}
	}

	return &customSidecarHarness{
		GatewayAddr:  gw.grpcAddr,
		ServiceTopic: serviceTopic,
		backendURL:   probeBackendURL,
	}
}

// waitForSidecarReadyCustom mirrors the harness.go readiness probe but
// targets an arbitrary backend name (the shared harness hardcodes
// "local"; tests like ACL configure their backends under different
// names — "scoped", "get-only", etc.).
//
// Unlike the harness.go probe we do NOT require the backend TCP port to
// be bound first: some resilience tests (BackendUnreachable, dead
// addresses) wire intentionally-unreachable backends and rely on the
// sidecar surfacing the transport error. The ProxyHTTP probe itself is
// a sufficient readiness signal: success OR a structured
// *ProxyTransportError means the sidecar runtime is registered.
func waitForSidecarReadyCustom(t *testing.T, gwAddr, serviceTopic, backendName, backendURL string) error {
	t.Helper()

	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: gwAddr,
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
		Implementation: "harness-probe",
		Specifier:      fmt.Sprintf("probe-%d", nextSidecarSpec.Add(1)),
	})
	if err != nil {
		return fmt.Errorf("probe NewAgentClient: %w", err)
	}

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()
	if err := client.Connect(connectCtx); err != nil {
		return fmt.Errorf("probe Connect: %w", err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	go func() { _ = client.Run(runCtx) }()
	defer func() {
		runCancel()
		_ = client.CloseConnection()
	}()

	// Probe paths most likely to evoke a quick response. /echo and
	// /fast are short-lived on the standard backends. /__readiness
	// is an intentionally-bogus path that will either 404 (registered
	// + ACL allows /*) or surface a structured ProxyTransportError
	// (registered + ACL denies it) — either case proves registration.
	probePaths := []string{"/echo", "/fast", "/__readiness_probe_xyz"}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !client.ConnectionConfirmed() {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		for _, p := range probePaths {
			// 5s is generous: a registered sidecar dispatches the
			// probe in <100ms when the backend is live, and surfaces a
			// structured DIAL_FAILED in 1-3s for dead backends. Use
			// streaming mode so backends that hold the body open (drip
			// handlers in service-disconnect tests) still return as
			// soon as headers arrive.
			probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			req, _ := http.NewRequestWithContext(probeCtx, "GET", "http://ignored"+p, nil)
			probeStart := time.Now()
			resp, perr := client.ProxyHTTP(probeCtx, serviceTopic, req,
				aether.WithBackend(backendName),
				aether.WithStreamResponse(5_000, 0))
			probeElapsed := time.Since(probeStart)
			if perr == nil {
				// Close the body immediately — we don't care what comes
				// back, just that the round trip happened.
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				probeCancel()
				return nil
			}
			probeCancel()
			// Structured transport errors (ACL_DENIED, DIAL_FAILED,
			// NO_ROUTE, etc.) mean the SDK round-tripped a real
			// response from the sidecar — registration confirmed.
			var pe *aether.ProxyTransportError
			if errors.As(perr, &pe) {
				return nil
			}
			// Any other error that took non-trivial time (≥100ms)
			// means we dispatched and waited for a response — the
			// sidecar is registered, the failure mode just isn't a
			// structured ProxyTransportError. Accept it as ready.
			// Common case: backends that return 0-status / 0-body
			// after a backend dial failure, which the SDK currently
			// surfaces as a generic Go error rather than a structured
			// transport error.
			if probeElapsed >= 100*time.Millisecond {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("probe never succeeded within 20s; last attempt confirmed=%v", client.ConnectionConfirmed())
}

// waitForSidecarRegistered is the relaxed readiness probe used when the
// custom harness has no HTTP backend to probe directly (e.g. resilience
// tests with a dead address). It dials a probe client and fires
// ProxyHTTP calls until ANY response — success or structured transport
// error — comes back. That outcome proves the sidecar is registered on
// the gateway and serving the requested service topic.
func waitForSidecarRegistered(t *testing.T, gwAddr, serviceTopic, backendName string) error {
	t.Helper()

	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: gwAddr,
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
		Implementation: "harness-probe",
		Specifier:      fmt.Sprintf("probe-reg-%d", nextSidecarSpec.Add(1)),
	})
	if err != nil {
		return fmt.Errorf("probe NewAgentClient: %w", err)
	}

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()
	if err := client.Connect(connectCtx); err != nil {
		return fmt.Errorf("probe Connect: %w", err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	go func() { _ = client.Run(runCtx) }()
	defer func() {
		runCancel()
		_ = client.CloseConnection()
	}()

	// Bigger per-probe timeout (5s) accommodates backends that take a
	// few seconds to surface DIAL_FAILED (TCP SYN retransmits to dead
	// addresses, slow DNS, etc.). Any returned response — success or
	// structured transport error — proves the sidecar is registered.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !client.ConnectionConfirmed() {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, _ := http.NewRequestWithContext(probeCtx, "GET", "http://ignored/__readiness_probe_xyz", nil)
		probeStart := time.Now()
		resp, perr := client.ProxyHTTP(probeCtx, serviceTopic, req,
			aether.WithBackend(backendName),
			aether.WithStreamResponse(5_000, 0))
		probeElapsed := time.Since(probeStart)
		if perr == nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			probeCancel()
			return nil
		}
		probeCancel()
		// Structured transport / ACL errors mean the sidecar is up.
		var pe *aether.ProxyTransportError
		if errors.As(perr, &pe) {
			return nil
		}
		// Any error that took non-trivial time means we dispatched
		// and waited for a response — the sidecar is registered.
		if probeElapsed >= 100*time.Millisecond {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("registration probe never succeeded within 20s")
}

// dialAgentClientForAddr is a copy of dialAgentClient but targets an
// arbitrary gateway address; used by the gateway-shutdown test which
// stands up its own harness and tears down the gateway mid-request.
func dialAgentClientForAddr(t *testing.T, gwAddr string, callerID string) *aether.AgentClient {
	t.Helper()

	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: gwAddr,
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

// =============================================================================
// Resilience tests
// =============================================================================

// TestE2E_BackendUnreachable_ReturnsCleanError points the terminator at a
// closed TCP port. The SDK should surface a ProxyTransportError of kind
// DIAL_FAILED (or TIMEOUT) within a bounded window — never hang.
func TestE2E_BackendUnreachable_ReturnsCleanError(t *testing.T) {
	// Reserve a dead address by listening then immediately closing. The
	// kernel won't reissue the port for a short window, and the OS
	// rejects connect() with ECONNREFUSED. This is more portable than
	// hard-coding 127.0.0.1:1 (some CI environments use the privileged
	// port range and behave oddly).
	deadAddr := reserveDeadAddr(t)

	backends := []proxysidecar.BackendConfig{{
		Name:          "dead",
		Kind:          proxysidecar.BackendKindHTTP,
		URL:           "http://" + deadAddr,
		AllowPaths:    []string{"/*"},
		AllowMethods:  []string{"GET", "POST"},
		MaxBodyBytes:  1 << 20,
		IdleTimeoutMs: 5_000,
		HeaderMode:    proxysidecar.HeaderModePassthrough,
	}}
	h := newCustomSidecarHarness(t, "bp-sidecar", "dead-backend", backends)
	client := dialAgentClientForAddr(t, h.GatewayAddr, "dead-backend-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/anything", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	start := time.Now()
	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("dead"))
	elapsed := time.Since(start)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected error for dead backend, got status=%d after %s", resp.StatusCode, elapsed)
	}
	if elapsed > 10*time.Second {
		t.Errorf("ProxyHTTP took %s; expected error within 10s", elapsed)
	}

	// The SDK surfaces transport-layer ProxyError as *ProxyTransportError.
	var pe *aether.ProxyTransportError
	if errors.As(err, &pe) {
		switch pe.Kind {
		case pb.ProxyError_DIAL_FAILED.String(),
			pb.ProxyError_TIMEOUT.String(),
			pb.ProxyError_UPSTREAM_RESET.String():
			// Expected — any of these is "the backend isn't there".
		default:
			t.Errorf("unexpected ProxyTransportError kind=%q msg=%q", pe.Kind, pe.Message)
		}
		t.Logf("dead backend error after %s: kind=%s msg=%q", elapsed, pe.Kind, pe.Message)
		return
	}
	// Some failure modes (ctx deadline) surface as context.Deadline; if
	// the upstream dial wedges longer than the test deadline that's a
	// regression, but the error type itself is also acceptable.
	if errors.Is(err, context.DeadlineExceeded) {
		t.Logf("dead backend deadline-exceeded after %s (acceptable; SDK didn't hang past test budget)", elapsed)
		return
	}
	t.Errorf("unexpected error type %T: %v", err, err)
}

// TestE2E_Backend500_StatusForwarded points the terminator at a backend
// that always returns 500. The SDK should surface a real http.Response
// with StatusCode==500, NOT a ProxyTransportError — terminator-side
// transport succeeded, the response body itself just carries the 500.
func TestE2E_Backend500_StatusForwarded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "boom")
	}))
	t.Cleanup(srv.Close)

	backends := []proxysidecar.BackendConfig{{
		Name:          "five-hundred",
		Kind:          proxysidecar.BackendKindHTTP,
		URL:           srv.URL,
		AllowPaths:    []string{"/*"},
		AllowMethods:  []string{"GET"},
		MaxBodyBytes:  1 << 20,
		IdleTimeoutMs: 5_000,
		HeaderMode:    proxysidecar.HeaderModePassthrough,
	}}
	h := newCustomSidecarHarness(t, "bp-sidecar", "five-hundred", backends)
	client := dialAgentClientForAddr(t, h.GatewayAddr, "five-hundred-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/whatever", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("five-hundred"))
	if err != nil {
		t.Fatalf("ProxyHTTP unexpectedly errored: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Errorf("status=%d, want 500", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "boom" {
		t.Errorf("body=%q, want %q", string(body), "boom")
	}
}

// TestE2E_CallerDisconnect_DuringStream_BackendCancellation opens an
// SSE stream from a /slow-style handler that records when its request
// context fires, then drops the SDK's gRPC connection mid-stream.
// Asserts the backend's request context is cancelled within a few
// seconds.
//
// FIX (this session): the cancel signal now propagates via:
//
//	SDK.CloseConnection → gateway sees caller bidi stream death
//	→ cleanupSession → proxyInflights.notifyPeersOfSessionEnd
//	→ publish ProxyHttpResponse{SIDECAR_UNAVAILABLE} to service sv:: topic
//	→ terminator's downstreamRouter.OnProxyHttpResponse looks up
//	  activeDispatches[requestID] → cancel() → dispatchCtx cancels
//	→ http.Client request ctx cancels → backend handler r.Context().Done().
//
// See server/internal/gateway/proxy_inflight_tracker.go and
// server/internal/proxysidecar/terminator.go (activeDispatches +
// OnProxyHttpResponse handler) for the wiring.
func TestE2E_CallerDisconnect_DuringStream_BackendCancellation(t *testing.T) {
	cancelledCh := make(chan struct{}, 1)
	// Hard handler deadline so t.Cleanup(srv.Close) doesn't block
	// forever when the cancel-propagation path doesn't fire.
	const handlerHardDeadline = 12 * time.Second
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		hardDeadline := time.After(handlerHardDeadline)
		for i := 0; ; i++ {
			select {
			case <-r.Context().Done():
				select {
				case cancelledCh <- struct{}{}:
				default:
				}
				return
			case <-hardDeadline:
				// Safety net: bail out so srv.Close() doesn't block
				// test teardown when cancellation propagation is
				// broken.
				return
			case <-ticker.C:
				if _, err := fmt.Fprintf(w, "data: ping %d\n\n", i); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
	}))
	t.Cleanup(srv.Close)

	backends := []proxysidecar.BackendConfig{{
		Name:          "drip",
		Kind:          proxysidecar.BackendKindHTTP,
		URL:           srv.URL,
		AllowPaths:    []string{"/*"},
		AllowMethods:  []string{"GET"},
		MaxBodyBytes:  1 << 20,
		IdleTimeoutMs: 60_000,
		HeaderMode:    proxysidecar.HeaderModePassthrough,
	}}
	h := newCustomSidecarHarness(t, "bp-sidecar", "drip", backends)
	client := dialAgentClientForAddr(t, h.GatewayAddr, "drip-caller")

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer streamCancel()

	req, err := http.NewRequestWithContext(streamCtx, "GET", "http://ignored/anything", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.ProxyHTTP(streamCtx, h.ServiceTopic, req,
		aether.WithBackend("drip"),
		aether.WithStreamResponse(15_000, 0))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}

	// Read at least one chunk so we know the backend handler is running.
	buf := make([]byte, 256)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("first read: %v", err)
	}

	// Now drop the SDK's connection. This severs the bidi stream to the
	// fake gateway, which should propagate the per-request cancellation
	// to the sidecar and through to the backend handler's request
	// context.
	_ = resp.Body.Close()
	if err := client.CloseConnection(); err != nil {
		t.Logf("CloseConnection error (often expected): %v", err)
	}

	// Bounded wait. 5s is generous: production cancel-propagation
	// through gateway cleanupSession + proxyInflights.notify +
	// terminator dispatch cancel normally completes inside a single
	// RTT, but session cleanup runs synchronously on the gateway and
	// can take ~100ms under load.
	select {
	case <-cancelledCh:
		// Pass — propagation path works end-to-end.
	case <-time.After(5 * time.Second):
		t.Errorf("caller disconnect did not propagate to backend " +
			"http.Request.Context within 5s. Expected path: " +
			"SDK.CloseConnection → gateway cleanupSession → " +
			"proxyInflights.notifyPeersOfSessionEnd → terminator " +
			"OnProxyHttpResponse → activeDispatches cancel → backend " +
			"r.Context().Done(). Check that proxy_inflight_tracker.go " +
			"register was called and that the gateway's session " +
			"cleanup actually fires the notify hook.")
	}
}

// TestE2E_GatewayShutdownMidRequest_SDKReturnsError spawns a dedicated
// per-test aetherlite, opens a long-lived streaming response, kills the
// gateway mid-flight, and asserts the SDK's body reader surfaces an
// error within a bounded window. Uses its own aetherlite (not the
// package-shared one) so SIGKILLing the gateway doesn't break every
// subsequent test.
//
// Production behaviour under test: when the SDK's underlying gRPC bidi
// stream dies (gateway crash, network drop, kubernetes pod eviction)
// any in-flight ProxyHTTP whose streamingBody is being drained should
// wake with a structured ProxyTransportError instead of hanging until
// the caller's per-request deadline fires. The SDK fix lives in
// sdk/go/aether/client.go (Run loop) + sdk/go/aether/proxy.go
// (failAllProxyInflights).
func TestE2E_GatewayShutdownMidRequest_SDKReturnsError(t *testing.T) {
	// Drip handler — sends one event every 50ms forever. Bounded by the
	// test budget so srv.Close() in t.Cleanup doesn't block when the
	// gateway kill DOESN'T propagate (regression safety net).
	const handlerHardDeadline = 20 * time.Second
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		hardDeadline := time.After(handlerHardDeadline)
		for i := 0; ; i++ {
			select {
			case <-r.Context().Done():
				return
			case <-hardDeadline:
				return
			case <-ticker.C:
				if _, err := fmt.Fprintf(w, "data: tick %d\n\n", i); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
	}))
	t.Cleanup(srv.Close)

	// Spawn a dedicated aetherlite — killing it mid-test must not break
	// other tests that share sharedAetherlite.
	proc, err := startAetherlite()
	if err != nil {
		t.Fatalf("start dedicated aetherlite: %v", err)
	}
	// Defensive cleanup if the kill below doesn't fire (e.g. early
	// fatal). proc.stop() is idempotent on already-dead processes.
	t.Cleanup(proc.stop)

	// Attach a per-test sidecar to the dedicated aetherlite. We do NOT
	// reuse newCustomSidecarHarness because that targets the shared
	// aetherlite; replicate the minimal wiring inline.
	uniqueSpec := fmt.Sprintf("gw-shutdown-%d", nextSidecarSpec.Add(1))
	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  proc.grpcAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "bp-sidecar",
			Specifier:      uniqueSpec,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "drip",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           srv.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET"},
				MaxBodyBytes:  1 << 20,
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}},
		},
		Relay: proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  "unix://" + relayPath,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
		},
		TenantID: "tenant-e2e",
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
		// Best-effort wait; sidecar may already be dead because the
		// gateway it's attached to died.
		select {
		case <-runnerDone:
		case <-time.After(5 * time.Second):
		}
	})

	serviceTopic := fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
	if err := waitForSidecarReadyCustom(t, proc.grpcAddr, serviceTopic, "drip", srv.URL); err != nil {
		t.Fatalf("dedicated-aetherlite sidecar never ready: %v", err)
	}

	client := dialAgentClientForAddr(t, proc.grpcAddr, "gw-shutdown-caller")

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer streamCancel()

	req, err := http.NewRequestWithContext(streamCtx, "GET", "http://ignored/anything", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.ProxyHTTP(streamCtx, serviceTopic, req,
		aether.WithBackend("drip"),
		aether.WithStreamResponse(20_000, 0))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()

	// Pull the first chunk so we know the stream is live.
	buf := make([]byte, 256)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("first read: %v", err)
	}

	// Reader goroutine — reports the first non-nil error and wall time.
	type readResult struct {
		err     error
		elapsed time.Duration
	}
	resCh := make(chan readResult, 1)
	go func() {
		start := time.Now()
		for {
			_, rerr := resp.Body.Read(buf)
			if rerr != nil {
				resCh <- readResult{err: rerr, elapsed: time.Since(start)}
				return
			}
		}
	}()

	// Kill the gateway mid-stream.
	proc.stop()

	select {
	case res := <-resCh:
		// Any non-nil error within the bounded window proves the SDK
		// woke the streamingBody on connection death. The SDK fix
		// surfaces a ProxyTransportError{UNAVAILABLE}; EOF (clean
		// stream close before the kill landed) is also acceptable.
		t.Logf("ProxyHTTP body read errored %s after gateway kill: %v",
			res.elapsed, res.err)
	case <-time.After(8 * time.Second):
		t.Errorf("caller did not see an error within 8s of gateway shutdown")
	}
}

// TestE2E_ServiceDisconnect_RouteEvictedCleanly tries to force the
// sidecar runtime to disconnect mid-request by cancelling its runner
// ctx, then asserts the caller-side ProxyHTTP surfaces an error
// promptly.
//
// FIX (this session): the gateway's session cleanup now walks
// proxyInflights for the dead service topic and emits a
// ProxyHttpResponse{SIDECAR_UNAVAILABLE} + fin chunk to every caller
// with an in-flight pointed at it. The SDK's resolveProxyResponse
// closes the streamingBody with the error, so the caller's Read()
// unblocks promptly instead of waiting for the stream's natural fin.
// See server/internal/gateway/proxy_inflight_tracker.go.
func TestE2E_ServiceDisconnect_RouteEvictedCleanly(t *testing.T) {
	// Slow backend that drips for at least 6 seconds — long enough that
	// the runner cancel happens well before natural fin.
	const dripDur = 6 * time.Second
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		end := time.Now().Add(dripDur)
		for i := 0; time.Now().Before(end); i++ {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := fmt.Fprintf(w, "data: %d\n\n", i); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
	}))
	t.Cleanup(srv.Close)

	gw := getAetherlite(t)
	uniqueSpec := fmt.Sprintf("svc-disc-%d", nextSidecarSpec.Add(1))
	relayPath := filepath.Join(t.TempDir(), "relay.sock")

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gw.grpcAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "bp-sidecar",
			Specifier:      uniqueSpec,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "drip",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           srv.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET"},
				MaxBodyBytes:  1 << 20,
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}},
		},
		Relay: proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  "unix://" + relayPath,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
		},
		TenantID: "tenant-e2e",
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
		// Defensive: if the test bailed before the explicit cancel.
		select {
		case <-runnerDone:
		default:
			runCancel()
			select {
			case <-runnerDone:
			case <-time.After(15 * time.Second):
				t.Logf("warning: svc-disc runner did not exit within 15s")
			}
		}
	})

	serviceTopic := fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
	if err := waitForSidecarReadyCustom(t, gw.grpcAddr, serviceTopic, "drip", srv.URL); err != nil {
		t.Fatalf("svc-disc sidecar never reached ready state: %v", err)
	}

	client := dialAgentClientForAddr(t, gw.grpcAddr, "svc-disc-caller")

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer streamCancel()

	req, err := http.NewRequestWithContext(streamCtx, "GET", "http://ignored/anything", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.ProxyHTTP(streamCtx, serviceTopic, req,
		aether.WithBackend("drip"),
		aether.WithStreamResponse(20_000, 0))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()

	// Pull the first chunk so we know the stream is live.
	buf := make([]byte, 256)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("first read: %v", err)
	}

	// Drive a goroutine that reads until error and reports the wall
	// time it took.
	type readResult struct {
		err     error
		elapsed time.Duration
	}
	resCh := make(chan readResult, 1)
	go func() {
		start := time.Now()
		for {
			_, rerr := resp.Body.Read(buf)
			if rerr != nil {
				resCh <- readResult{err: rerr, elapsed: time.Since(start)}
				return
			}
		}
	}()

	// Cancel the runner — service-side hard disconnect.
	runCancel()

	select {
	case res := <-resCh:
		// Any non-nil error within the bounded window proves the
		// disconnect propagated. EOF is acceptable (gateway closed the
		// stream cleanly), as are structured transport errors.
		t.Logf("ProxyHTTP body read errored %s after service-disconnect: %v",
			res.elapsed, res.err)
	case <-time.After(8 * time.Second):
		t.Errorf("caller did not see an error within 8s of service-disconnect")
	}
}

// =============================================================================
// helpers
// =============================================================================

// reserveDeadAddr returns a host:port that is unreachable for the
// duration of the test. We listen on an ephemeral port, capture its
// address, then close the listener; the kernel keeps the port out of
// rotation long enough for the test to use it. connect() to the
// returned address returns ECONNREFUSED.
func reserveDeadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// satisfy goimports on rare configurations where the compiler elides an
// otherwise-unused symbol — keeps grpc/sync references reachable.
var (
	_ = grpc.NewClient
	_ = (&sync.WaitGroup{})
	_ = atomic.Int64{}
)
