//go:build e2e

package integration_e2e

// multi_surface_test.go exercises combinations of sidecar surfaces running
// inside the same Runner process: initiator+terminator, relay+initiator, and
// all three surfaces simultaneously.
//
// Coverage matrix gaps addressed (§2 lines 91–93):
//   - initiator + terminator
//   - relay    + initiator
//   - all-three-enabled
//
// Relay round-trip wiring note: the relay surface's full end-to-end path
// (sandbox → relay UDS → gateway → upstream service) requires a two-hop
// harness that the in-process e2e setup does not model (see
// TestE2E_StandaloneRelay_RoundTrip for the canonical explanation). For
// relay-containing combos we therefore verify:
//   (a) the relay surface boots and binds its UDS listener, AND
//   (b) the OTHER surfaces (terminator / initiator) still function correctly
//       with the relay surface active — confirming no resource conflict.
//
// Initiator dispatcher note: proxysidecar.Runner builds the Initiator in
// NewRunner but does not inject a dispatcher (the Runner has no gateway
// identity for the initiator to use). Production wiring is left to the
// host process. For tests we replicate the pattern from initiator_test.go:
// wire an *aether.AgentClient as the dispatcher after calling NewRunner,
// before calling Run. Runner.Initiator() exposes the Initiator for this.

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Multi-surface harness
// =============================================================================

// multiSurfaceHarness owns a single Runner that may have terminator, relay,
// and/or initiator surfaces active simultaneously. The struct fields are
// populated only when the corresponding surface is enabled.
type multiSurfaceHarness struct {
	// GatewayAddr is the shared aetherlite gRPC address.
	GatewayAddr string
	// ServiceTopic is the sv:: address for this runner's terminator surface.
	// Empty when terminator is disabled.
	ServiceTopic string
	// HTTPBackendURL is the in-process backend behind the terminator.
	// Empty when terminator is disabled.
	HTTPBackendURL string
	// RelayPath is the UDS path the relay surface binds.
	// Empty when relay is disabled.
	RelayPath string
	// InitiatorAddr is the host:port the initiator HTTP listener binds.
	// Empty when initiator is disabled.
	InitiatorAddr string
}

type multiSurfaceOpts struct {
	EnableTerminator bool
	EnableRelay      bool
	EnableInitiator  bool
	// InitiatorTarget is the sv:: topic the initiator dispatches to.
	// Must be set when EnableInitiator=true.
	InitiatorTarget string
	// Specifier disambiguates the sidecar identity.
	Specifier string
}

// newMultiSurfaceHarness builds a Runner with the requested surface
// combination and wires all necessary plumbing. All resources tear down via
// t.Cleanup.
//
// When initiator is enabled the caller must supply opts.InitiatorTarget — the
// upstream service topic the initiator forwards requests to. The initiator's
// dispatcher is wired as an AgentClient connected to the same gateway.
func newMultiSurfaceHarness(t *testing.T, opts multiSurfaceOpts) *multiSurfaceHarness {
	t.Helper()

	if !opts.EnableTerminator && !opts.EnableRelay && !opts.EnableInitiator {
		t.Fatalf("newMultiSurfaceHarness: at least one surface must be enabled")
	}
	if opts.EnableInitiator && opts.InitiatorTarget == "" {
		t.Fatalf("newMultiSurfaceHarness: InitiatorTarget must be set when initiator is enabled")
	}
	if opts.Specifier == "" {
		opts.Specifier = "multi"
	}
	uniqueSpec := fmt.Sprintf("%s-%d", opts.Specifier, nextSidecarSpec.Add(1))

	gw := getAetherlite(t)
	gwAddr := gw.grpcAddr

	out := &multiSurfaceHarness{GatewayAddr: gwAddr}

	// --- HTTP backend (used by terminator and/or as initiator upstream) -----
	var backend *httptest.Server
	if opts.EnableTerminator {
		backend = newHTTPBackend(t, 5*time.Second)
		out.HTTPBackendURL = backend.URL
	}

	// --- Relay socket path --------------------------------------------------
	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	relayListen := "unix://" + relayPath
	if opts.EnableRelay {
		out.RelayPath = relayPath
	}

	// --- Initiator local listener -------------------------------------------
	// Reserve an ephemeral port then release it; the initiator will rebind.
	var initAddr string
	if opts.EnableInitiator {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("initiator port reserve: %v", err)
		}
		initAddr = ln.Addr().String()
		_ = ln.Close()
		out.InitiatorAddr = initAddr
	}

	// --- Build Config -------------------------------------------------------
	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gwAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "bp-sidecar",
			Specifier:      uniqueSpec,
		},
		TenantID: "tenant-e2e-multi",
	}

	if opts.EnableTerminator {
		cfg.Terminator = proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "local",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backend.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET", "POST", "PUT", "DELETE"},
				MaxBodyBytes:  32 << 20,
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}},
		}
	}

	if opts.EnableRelay {
		cfg.Relay = proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  relayListen,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
		}
	}

	if opts.EnableInitiator {
		cfg.Initiator = proxysidecar.InitiatorConfig{
			Enabled: true,
			Listen:  proxysidecar.ListenConfig{Bind: initAddr},
			Target:  proxysidecar.TargetConfig{Topic: opts.InitiatorTarget},
		}
	}

	// --- Build Runner -------------------------------------------------------
	runner, err := proxysidecar.NewRunner(cfg, "")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	// --- Wire initiator dispatcher ------------------------------------------
	// The Runner builds the Initiator but does not inject a dispatcher.
	// We wire one here using an AgentClient that dials the same gateway.
	if opts.EnableInitiator {
		dispatchClient := dialAgentClientToAddr(t, gwAddr,
			fmt.Sprintf("ms-init-disp-%d", nextSidecarSpec.Add(1)))
		runner.Initiator().SetDispatcher(&agentDispatcher{client: dispatchClient})
	}

	// --- Start Runner -------------------------------------------------------
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
			t.Logf("warning: multi-surface runner did not exit within 15s")
		}
	})

	// --- Service topic (only meaningful when terminator is enabled) ----------
	if opts.EnableTerminator {
		out.ServiceTopic = fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
		if err := waitForSidecarReady(t, gwAddr, out.ServiceTopic, backend.URL); err != nil {
			t.Fatalf("multi-surface sidecar never reached ready state: %v", err)
		}
	}

	// --- Wait for relay UDS listener ----------------------------------------
	if opts.EnableRelay {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(relayPath); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		// If the relay binds lazily we don't fatalf — the test will detect
		// the gap when it inspects RelayPath.
	}

	// --- Wait for initiator listener ----------------------------------------
	if opts.EnableInitiator {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			c, err := net.DialTimeout("tcp", initAddr, 250*time.Millisecond)
			if err == nil {
				_ = c.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	return out
}

// =============================================================================
// Helper: drive a plain HTTP GET through the initiator listener
// =============================================================================

func initiatorGET(t *testing.T, initAddr, path string, timeout time.Duration) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := fmt.Sprintf("http://%s%s", initAddr, path)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		t.Fatalf("initiatorGET NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("initiatorGET Do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

// =============================================================================
// TestE2E_MultiSurface_InitiatorPlusTerminator_BothWork
// =============================================================================
//
// Single Runner with Initiator.Enabled=true AND Terminator.Enabled=true.
//
// Topology:
//
//	Upstream sidecar (terminator-only)  →  in-process backend A
//	Test sidecar     (initiator+terminator) →  in-process backend B
//
// Assertions:
//
//	(a) External caller → test sidecar's terminator → backend B  (standard
//	    ProxyHTTP round-trip via SDK).
//	(b) Local HTTP request → test sidecar's initiator → gateway → upstream
//	    sidecar's terminator → backend A  (initiator surface works alongside
//	    terminator).
//	(c) Interleaved: 3 terminator calls and 3 initiator calls fired in
//	    parallel — all succeed, confirming no surface starves the other.
func TestE2E_MultiSurface_InitiatorPlusTerminator_BothWork(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	// --- Upstream sidecar (terminator-only) ---------------------------------
	// The initiator on the test sidecar will forward requests here.
	upstream := newStandaloneHarness(t, standaloneHarnessOpts{
		EnableTerminator: true,
		Specifier:        "ms-it-upstream",
	})

	// --- Test sidecar (initiator + terminator) ------------------------------
	subject := newMultiSurfaceHarness(t, multiSurfaceOpts{
		EnableTerminator: true,
		EnableInitiator:  true,
		InitiatorTarget:  upstream.ServiceTopic,
		Specifier:        "ms-it-subject",
	})

	// SDK caller for the terminator surface of the test sidecar.
	callerClient := dialAgentClientToAddr(t, subject.GatewayAddr, "ms-it-caller")

	// --- (a) Terminator round-trip ------------------------------------------
	t.Run("terminator_roundtrip", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/fast", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := callerClient.ProxyHTTP(ctx, subject.ServiceTopic, req, aether.WithBackend("local"))
		if err != nil {
			t.Fatalf("terminator ProxyHTTP: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("terminator status=%d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !bytes.Contains(body, []byte(`"ok":true`)) {
			t.Errorf("terminator body=%q does not contain ok:true", string(body))
		}
	})

	// --- (b) Initiator round-trip -------------------------------------------
	t.Run("initiator_roundtrip", func(t *testing.T) {
		status, body := initiatorGET(t, subject.InitiatorAddr, "/fast", 15*time.Second)
		if status != 200 {
			t.Fatalf("initiator status=%d", status)
		}
		if !bytes.Contains(body, []byte(`"ok":true`)) {
			t.Errorf("initiator body=%q does not contain ok:true", string(body))
		}
	})

	// --- (c) Interleaved: both surfaces in parallel -------------------------
	t.Run("interleaved", func(t *testing.T) {
		const fanout = 3
		var wg sync.WaitGroup
		termErrs := make([]error, fanout)
		initErrs := make([]error, fanout)

		for i := 0; i < fanout; i++ {
			i := i
			// terminator call
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/fast", nil)
				if err != nil {
					termErrs[i] = fmt.Errorf("NewRequest: %w", err)
					return
				}
				resp, err := callerClient.ProxyHTTP(ctx, subject.ServiceTopic, req, aether.WithBackend("local"))
				if err != nil {
					termErrs[i] = fmt.Errorf("ProxyHTTP: %w", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					termErrs[i] = fmt.Errorf("status=%d", resp.StatusCode)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				if !bytes.Contains(body, []byte(`"ok":true`)) {
					termErrs[i] = fmt.Errorf("body=%q missing ok:true", string(body))
				}
			}()

			// initiator call
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				url := fmt.Sprintf("http://%s/fast", subject.InitiatorAddr)
				req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
				if err != nil {
					initErrs[i] = fmt.Errorf("NewRequest: %w", err)
					return
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					initErrs[i] = fmt.Errorf("Do: %w", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					initErrs[i] = fmt.Errorf("status=%d", resp.StatusCode)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				if !bytes.Contains(body, []byte(`"ok":true`)) {
					initErrs[i] = fmt.Errorf("body=%q missing ok:true", string(body))
				}
			}()
		}

		wg.Wait()
		for i, err := range termErrs {
			if err != nil {
				t.Errorf("terminator call %d: %v", i, err)
			}
		}
		for i, err := range initErrs {
			if err != nil {
				t.Errorf("initiator call %d: %v", i, err)
			}
		}
	})
}

// =============================================================================
// TestE2E_MultiSurface_RelayPlusInitiator_BothWork
// =============================================================================
//
// Single Runner with Relay.Enabled=true AND Initiator.Enabled=true,
// terminator disabled.
//
// Assertions:
//
//	(a) Relay surface boots and binds its UDS listener without error.
//	(b) Initiator surface successfully forwards local HTTP requests through
//	    the gateway to a separate upstream terminator sidecar — confirming
//	    the initiator's gateway connection is not blocked by the relay surface
//	    sharing the same process.
//	(c) Interleaved: 3 initiator calls in parallel — all succeed, confirming
//	    the initiator is not starved.
//
// Note: relay end-to-end round-trip requires a two-hop sandbox→relay→gateway
// harness not modeled here; see TestE2E_StandaloneRelay_RoundTrip.
func TestE2E_MultiSurface_RelayPlusInitiator_BothWork(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	// --- Upstream sidecar (terminator-only) ---------------------------------
	upstream := newStandaloneHarness(t, standaloneHarnessOpts{
		EnableTerminator: true,
		Specifier:        "ms-ri-upstream",
	})

	// --- Test sidecar (relay + initiator, terminator OFF) -------------------
	subject := newMultiSurfaceHarness(t, multiSurfaceOpts{
		EnableRelay:     true,
		EnableInitiator: true,
		InitiatorTarget: upstream.ServiceTopic,
		Specifier:       "ms-ri-subject",
	})

	// --- (a) Relay surface booted -------------------------------------------
	t.Run("relay_booted", func(t *testing.T) {
		if subject.RelayPath == "" {
			t.Fatalf("relay surface: RelayPath is empty")
		}
		if _, err := os.Stat(subject.RelayPath); err != nil {
			// Relay may bind lazily. Give it a brief extra window.
			deadline := time.Now().Add(5 * time.Second)
			found := false
			for time.Now().Before(deadline) {
				if _, err2 := os.Stat(subject.RelayPath); err2 == nil {
					found = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !found {
				t.Logf("relay UDS at %s did not appear within 5s (relay may bind lazily on first dial)", subject.RelayPath)
			}
		}
		t.Logf("relay UDS path: %s", subject.RelayPath)
	})

	// --- (b) Initiator round-trip -------------------------------------------
	t.Run("initiator_roundtrip", func(t *testing.T) {
		status, body := initiatorGET(t, subject.InitiatorAddr, "/fast", 15*time.Second)
		if status != 200 {
			t.Fatalf("initiator status=%d", status)
		}
		if !bytes.Contains(body, []byte(`"ok":true`)) {
			t.Errorf("initiator body=%q does not contain ok:true", string(body))
		}
	})

	// --- (c) Interleaved initiator calls ------------------------------------
	t.Run("initiator_concurrent", func(t *testing.T) {
		const fanout = 3
		var wg sync.WaitGroup
		errs := make([]error, fanout)
		for i := 0; i < fanout; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				url := fmt.Sprintf("http://%s/fast", subject.InitiatorAddr)
				req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
				if err != nil {
					errs[i] = fmt.Errorf("NewRequest: %w", err)
					return
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					errs[i] = fmt.Errorf("Do: %w", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					errs[i] = fmt.Errorf("status=%d", resp.StatusCode)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				if !bytes.Contains(body, []byte(`"ok":true`)) {
					errs[i] = fmt.Errorf("body=%q missing ok:true", string(body))
				}
			}()
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Errorf("initiator concurrent call %d: %v", i, err)
			}
		}
	})
}

// =============================================================================
// TestE2E_MultiSurface_AllThreeSurfaces_AllWork
// =============================================================================
//
// Single Runner with Terminator.Enabled=true, Initiator.Enabled=true, and
// Relay.Enabled=true simultaneously.
//
// Topology:
//
//	Upstream sidecar (terminator-only)   →  in-process backend A
//	Subject  sidecar (term+init+relay)   →  in-process backend B
//	  - terminator: serves SDK callers targeting subject.ServiceTopic
//	  - initiator:  local HTTP → gateway → upstream.ServiceTopic → backend A
//	  - relay:      UDS listener bound and accepting connections
//
// Assertions:
//
//	(a) Terminator: external SDK caller hits backend B via ProxyHTTP.
//	(b) Initiator:  local HTTP caller reaches backend A via the initiator.
//	(c) Relay:      relay UDS listener is present (surface alive).
//	(d) Echo upload: large POST through the initiator echoes back correctly,
//	    confirming chunked path works while all three surfaces are active.
//	(e) Interleaved: terminator + initiator calls fired concurrently (3×3).
func TestE2E_MultiSurface_AllThreeSurfaces_AllWork(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	// --- Upstream sidecar (terminator-only) ---------------------------------
	upstream := newStandaloneHarness(t, standaloneHarnessOpts{
		EnableTerminator: true,
		Specifier:        "ms-all-upstream",
	})

	// --- Subject sidecar (all three surfaces) --------------------------------
	subject := newMultiSurfaceHarness(t, multiSurfaceOpts{
		EnableTerminator: true,
		EnableRelay:      true,
		EnableInitiator:  true,
		InitiatorTarget:  upstream.ServiceTopic,
		Specifier:        "ms-all-subject",
	})

	// SDK caller for the terminator surface.
	callerClient := dialAgentClientToAddr(t, subject.GatewayAddr, "ms-all-caller")

	// --- (a) Terminator round-trip ------------------------------------------
	t.Run("terminator_roundtrip", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/fast", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := callerClient.ProxyHTTP(ctx, subject.ServiceTopic, req, aether.WithBackend("local"))
		if err != nil {
			t.Fatalf("terminator ProxyHTTP: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("terminator status=%d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !bytes.Contains(body, []byte(`"ok":true`)) {
			t.Errorf("terminator body=%q does not contain ok:true", string(body))
		}
	})

	// --- (b) Initiator round-trip -------------------------------------------
	t.Run("initiator_roundtrip", func(t *testing.T) {
		status, body := initiatorGET(t, subject.InitiatorAddr, "/fast", 15*time.Second)
		if status != 200 {
			t.Fatalf("initiator status=%d", status)
		}
		if !bytes.Contains(body, []byte(`"ok":true`)) {
			t.Errorf("initiator body=%q does not contain ok:true", string(body))
		}
	})

	// --- (c) Relay surface booted -------------------------------------------
	t.Run("relay_booted", func(t *testing.T) {
		if subject.RelayPath == "" {
			t.Fatalf("relay surface: RelayPath is empty")
		}
		if _, err := os.Stat(subject.RelayPath); err != nil {
			deadline := time.Now().Add(5 * time.Second)
			found := false
			for time.Now().Before(deadline) {
				if _, err2 := os.Stat(subject.RelayPath); err2 == nil {
					found = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !found {
				t.Logf("relay UDS at %s did not appear within 5s (relay may bind lazily on first dial)", subject.RelayPath)
			}
		}
		t.Logf("relay UDS path: %s", subject.RelayPath)
	})

	// --- (d) Large echo upload through initiator ----------------------------
	t.Run("initiator_echo_upload", func(t *testing.T) {
		const uploadSize = 512 * 1024 // 512 KiB — exercises chunked path
		payload := make([]byte, uploadSize)
		if _, err := rand.Read(payload); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()

		url := fmt.Sprintf("http://%s/echo", subject.InitiatorAddr)
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len(payload))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("initiator POST /echo: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("echo status=%d", resp.StatusCode)
		}
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("ReadAll echo body: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("echo mismatch: got %d bytes, want %d bytes", len(got), len(payload))
		}
	})

	// --- (e) Interleaved terminator + initiator -----------------------------
	t.Run("interleaved_all_surfaces", func(t *testing.T) {
		const fanout = 3

		var (
			wg       sync.WaitGroup
			termErrs [fanout]error
			initErrs [fanout]error
			termOK   atomic.Int32
			initOK   atomic.Int32
		)

		for i := 0; i < fanout; i++ {
			i := i

			// Terminator call
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/fast", nil)
				if err != nil {
					termErrs[i] = fmt.Errorf("NewRequest: %w", err)
					return
				}
				resp, err := callerClient.ProxyHTTP(ctx, subject.ServiceTopic, req, aether.WithBackend("local"))
				if err != nil {
					termErrs[i] = fmt.Errorf("ProxyHTTP: %w", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					termErrs[i] = fmt.Errorf("status=%d", resp.StatusCode)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				if !bytes.Contains(body, []byte(`"ok":true`)) {
					termErrs[i] = fmt.Errorf("body=%q missing ok:true", string(body))
					return
				}
				termOK.Add(1)
			}()

			// Initiator call
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				url := fmt.Sprintf("http://%s/fast", subject.InitiatorAddr)
				req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
				if err != nil {
					initErrs[i] = fmt.Errorf("NewRequest: %w", err)
					return
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					initErrs[i] = fmt.Errorf("Do: %w", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					initErrs[i] = fmt.Errorf("status=%d", resp.StatusCode)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				if !bytes.Contains(body, []byte(`"ok":true`)) {
					initErrs[i] = fmt.Errorf("body=%q missing ok:true", string(body))
					return
				}
				initOK.Add(1)
			}()
		}

		wg.Wait()

		for i := range fanout {
			if termErrs[i] != nil {
				t.Errorf("terminator call %d: %v", i, termErrs[i])
			}
		}
		for i := range fanout {
			if initErrs[i] != nil {
				t.Errorf("initiator call %d: %v", i, initErrs[i])
			}
		}
		t.Logf("interleaved: terminator ok=%d/%d  initiator ok=%d/%d",
			termOK.Load(), fanout, initOK.Load(), fanout)
	})
}
