// In-process HTTP / TCP backends + composite-mode proxy-sidecar
// Runner wiring, hosted alongside a shared real aetherlite gateway
// subprocess that all tests in this package share.
//
// The fake-gateway era is over: previous versions of this harness used
// a hand-rolled routingFakeGateway that re-implemented just enough of
// the aether wire protocol to route envelopes between SDK clients and
// the sidecar. That fake leaked harness-only behaviour into our tests
// (request_id collisions, missing audit / ACL / KV flow, no real
// connection lifecycle) and failed to catch a class of bugs that only
// surface against a real gateway.
//
// This version connects every test to the shared aetherlite subprocess
// managed by aetherlite_proc_test.go (TestMain owns lifecycle). Each
// NewE2EHarness call attaches a fresh proxy-sidecar with a unique
// service spec so per-test config (ACLs, surface mix, backends) stays
// isolated even though the gateway is shared.
//
// Build tag: `e2e` — file is only compiled into the e2e test binary
// because TestMain in this package is e2e-only and the harness has no
// callers outside the e2e tests.

//go:build e2e

package integration_e2e

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/server/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// debugEnabled toggles verbose harness tracing — set via E2E_DEBUG=1.
// Cheap, off in normal runs.
var debugEnabled = os.Getenv("E2E_DEBUG") == "1"

// debugLog dumps a debugger-only line to stderr when E2E_DEBUG=1.
func debugLog(format string, args ...any) {
	if !debugEnabled {
		return
	}
	fmt.Fprintf(os.Stderr, "[e2e-harness] "+format+"\n", args...)
}

// nextSidecarSpec assigns a unique sidecar specifier per harness so
// concurrent or sequential tests attaching to the shared aetherlite
// don't collide on identity. Aetherlite enforces single-active-session
// per (impl, spec) — reusing a spec across tests within the same test
// run would fail with DuplicateIdentityError.
var nextSidecarSpec atomic.Uint64

// =============================================================================
// E2EHarness — public test fixture
// =============================================================================

// E2EHarness bundles everything a typical e2e test needs:
//
//   - GatewayAddr is the TCP address (127.0.0.1:<port>) of the shared
//     real aetherlite gateway. SDK clients dial this directly.
//   - RelayAddr is the unix socket path of the proxy-sidecar's relay
//     listener (for sandbox-side attachment scenarios).
//   - ServiceTopic is the sv::<impl>::<spec> address that targets this
//     test's sidecar runtime.
//   - HTTPBackendURL is the http://127.0.0.1:<port> address of the
//     in-process HTTP backend the terminator forwards to.
//   - TCPBackendAddr is the TCP echo server address for tunnel tests.
//   - SlowStreamDuration controls how long /slow drips for; defaults
//     to 5s and can be overridden by tests via the New constructor.
type E2EHarness struct {
	GatewayAddr        string
	RelayAddr          string
	ServiceTopic       string
	HTTPBackendURL     string
	TCPBackendAddr     string
	SlowStreamDuration time.Duration
}

// E2EHarnessOptions customises the harness. The zero value is sane.
type E2EHarnessOptions struct {
	// SlowStreamDuration is how long /slow drips data for. Defaults to
	// 5s — short enough to keep individual tests under 30s, long
	// enough to keep multiple streams in flight while fast calls fire.
	SlowStreamDuration time.Duration

	// Implementation identifies the sidecar's impl. Default: "bp-sidecar".
	// Specifier is auto-assigned per harness for uniqueness; if the
	// test really needs to pin a specific spec it can be set here, but
	// reusing a spec across tests will hit DuplicateIdentityError on
	// the shared aetherlite.
	Implementation string
	Specifier      string
}

// NewE2EHarness brings up an in-process HTTP backend + TCP echo +
// proxy-sidecar Runner attached to the shared aetherlite. All
// resources tear down via t.Cleanup.
func NewE2EHarness(t *testing.T, opts ...E2EHarnessOptions) *E2EHarness {
	t.Helper()

	o := E2EHarnessOptions{}
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.SlowStreamDuration == 0 {
		o.SlowStreamDuration = 5 * time.Second
	}
	if o.Implementation == "" {
		o.Implementation = "bp-sidecar"
	}
	if o.Specifier == "" {
		o.Specifier = fmt.Sprintf("e2e-%d", nextSidecarSpec.Add(1))
	}

	gw := getAetherlite(t)

	// --- 1. In-process HTTP backend -----------------------------------------
	backend := newHTTPBackend(t, o.SlowStreamDuration)

	// --- 2. In-process TCP echo backend -------------------------------------
	tcpAddr := startTCPEcho(t)

	// --- 3. Composite-mode proxy-sidecar Runner -----------------------------
	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	relayListen := "unix://" + relayPath

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gw.grpcAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: o.Implementation,
			Specifier:      o.Specifier,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "local",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backend.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET", "POST", "PUT", "DELETE"},
				MaxBodyBytes:  32 << 20, // 32 MiB headroom for chunked upload
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}, {
				Name:          "tcp-echo",
				Kind:          proxysidecar.BackendKindTCP,
				URL:           "tcp://" + tcpAddr,
				MaxBodyBytes:  32 << 20,
				IdleTimeoutMs: 60_000,
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
			t.Logf("warning: runner did not exit within 15s of cancel")
		}
	})

	serviceTopic := fmt.Sprintf("sv::%s::%s", o.Implementation, o.Specifier)

	// --- 4. Wait until the sidecar has registered with aetherlite -----------
	//
	// We probe by dialing an aether agent client and asking it to issue
	// a tiny ProxyHTTP at the service topic. The first successful round
	// trip means the sidecar is connected to aetherlite AND the
	// terminator backend handler is wired. Polling cap: 15s — generous
	// for slow CI, fast on a developer machine (<300ms typically).
	if err := waitForSidecarReady(t, gw.grpcAddr, serviceTopic, backend.URL); err != nil {
		t.Fatalf("sidecar never reached ready state: %v", err)
	}

	return &E2EHarness{
		GatewayAddr:        gw.grpcAddr,
		RelayAddr:          relayPath,
		ServiceTopic:       serviceTopic,
		HTTPBackendURL:     backend.URL,
		TCPBackendAddr:     tcpAddr,
		SlowStreamDuration: o.SlowStreamDuration,
	}
}

// waitForSidecarReady spins up a throwaway aether agent client and
// retries small ProxyHTTP /fast calls until one succeeds. The probe
// validates the FULL path: SDK → aetherlite → sidecar → backend →
// response back. Returns nil on success, error on timeout.
func waitForSidecarReady(t *testing.T, gwAddr, serviceTopic, backendURL string) error {
	t.Helper()

	// Confirm the in-process backend is bound first; otherwise we'd be
	// chasing a failure that has nothing to do with sidecar readiness.
	if u, err := parseBackendHostPort(backendURL); err == nil {
		if err := waitForTCP(u, 3*time.Second); err != nil {
			return fmt.Errorf("local backend %s never reached ready: %w", u, err)
		}
	}

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

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !client.ConnectionConfirmed() {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		req, _ := http.NewRequestWithContext(probeCtx, "GET", "http://ignored/fast", nil)
		resp, err := client.ProxyHTTP(probeCtx, serviceTopic, req, aether.WithBackend("local"))
		probeCancel()
		if err == nil {
			if resp != nil && resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("probe never succeeded within 15s; last attempt confirmed=%v", client.ConnectionConfirmed())
}

// parseBackendHostPort extracts "host:port" from "http://host:port" /
// "http://host:port/path".
func parseBackendHostPort(rawURL string) (string, error) {
	s := strings.TrimPrefix(rawURL, "http://")
	s = strings.TrimPrefix(s, "https://")
	if i := strings.Index(s, "/"); i >= 0 {
		s = s[:i]
	}
	if s == "" {
		return "", errors.New("empty backend host")
	}
	return s, nil
}

// =============================================================================
// In-process HTTP backend with /slow, /fast, /echo handlers
// =============================================================================

func newHTTPBackend(t *testing.T, slowDur time.Duration) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// /fast — immediate {"ok":true} response.
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})

	// /slow — chunked SSE drip for slowDur. Each tick emits one event.
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		const tickInterval = 200 * time.Millisecond
		end := time.Now().Add(slowDur)
		i := 0
		for time.Now().Before(end) {
			if _, err := fmt.Fprintf(w, "data: ping %d\n\n", i); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			i++
			select {
			case <-r.Context().Done():
				return
			case <-time.After(tickInterval):
			}
		}
	})

	// /echo — POST body echoed back verbatim.
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		w.WriteHeader(200)
		_, _ = w.Write(body)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// =============================================================================
// In-process TCP echo server
// =============================================================================

// startTCPEcho boots a TCP listener that echoes incoming bytes back on
// the same connection. Returns the listener's host:port.
func startTCPEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tcp echo listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 32*1024)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// =============================================================================
// gRPC dial helper
// =============================================================================

// dialGateway dials the gateway with insecure credentials. Caller
// owns the returned conn (defer conn.Close()).
func dialGateway(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// readSSE drains a chunked SSE response body and returns the number of
// "data:" events observed plus any read error.
func readSSE(body io.Reader) (int, error) {
	br := bufio.NewReader(body)
	events := 0
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 && strings.HasPrefix(line, "data:") {
			events++
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return events, nil
			}
			return events, err
		}
	}
}
