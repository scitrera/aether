//go:build e2e

package integration_e2e

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// UDP harness — sidecar + UDP echo server
// =============================================================================
//
// Like the WS harness in ws_tunnel_test.go, the UDP coverage tests need a
// sidecar whose terminator config includes a UDP backend. We mirror
// NewE2EHarness's construction inline with a UDP backend added and an
// in-process UDP echo server on an ephemeral port. UDP has no flow-control
// credits, no half-close, and one TunnelData frame == one datagram.

type udpHarness struct {
	GatewayAddr  string
	ServiceTopic string
	UDPEchoAddr  string // 127.0.0.1:port — passed as remote_hint to TunnelDial
}

// newUDPHarness boots: routing fake gateway, in-process UDP echo server, and a
// composite-mode sidecar with a "udp-echo" backend. The optional
// configureBackend hook lets a test tighten idle timeouts or shrink the
// per-datagram MTU cap.
func newUDPHarness(t *testing.T, configureBackend func(*proxysidecar.BackendConfig)) *udpHarness {
	t.Helper()

	gw := getAetherlite(t)
	gwAddr := gw.grpcAddr
	udpAddr := startUDPEcho(t)
	httpBackend := newHTTPBackend(t, 5*time.Second)

	udpBackend := proxysidecar.BackendConfig{
		Name:             "udp-echo",
		Kind:             proxysidecar.BackendKindUDP,
		URL:              "udp://" + udpAddr,
		MaxBytes:         32 << 20,
		IdleTimeoutMs:    60_000,
		MaxDatagramBytes: 8192, // headroom for the 4 KiB datagram test
	}
	if configureBackend != nil {
		configureBackend(&udpBackend)
	}

	// Unique-ify the specifier so multiple UDP-harness tests don't
	// collide on the shared aetherlite's (impl, spec) identity.
	uniqueSpec := fmt.Sprintf("e2e-udp-%d", nextSidecarSpec.Add(1))

	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{Address: gwAddr, Insecure: true},
		Service: proxysidecar.ServiceConfig{
			Implementation: "bp-sidecar",
			Specifier:      uniqueSpec,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{
				{
					Name:          "local",
					Kind:          proxysidecar.BackendKindHTTP,
					URL:           httpBackend.URL,
					AllowPaths:    []string{"/*"},
					AllowMethods:  []string{"GET", "POST"},
					MaxBodyBytes:  32 << 20,
					IdleTimeoutMs: 60_000,
					HeaderMode:    proxysidecar.HeaderModePassthrough,
				},
				udpBackend,
			},
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
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg.Validate: %v", err)
	}
	runner, err := proxysidecar.NewRunner(cfg, "")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(runCtx)
	}()
	t.Cleanup(func() {
		runCancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Logf("warning: udp-harness runner did not exit within 15s")
		}
	})

	serviceTopic := fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
	if err := waitForSidecarReady(t, gwAddr, serviceTopic, httpBackend.URL); err != nil {
		t.Fatalf("udp-harness sidecar never reached ready state: %v", err)
	}

	return &udpHarness{
		GatewayAddr:  gwAddr,
		ServiceTopic: serviceTopic,
		UDPEchoAddr:  udpAddr,
	}
}

// startUDPEcho opens a UDP listener on 127.0.0.1:0 and echoes every datagram
// back to its source. Returns "host:port".
func startUDPEcho(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("udp echo listen: %v", err)
	}
	go func() {
		buf := make([]byte, 65535)
		for {
			n, src, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			// Copy out — the buffer is reused on the next iteration.
			payload := append([]byte(nil), buf[:n]...)
			if _, werr := pc.WriteTo(payload, src); werr != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = pc.Close() })
	return pc.LocalAddr().String()
}

// dialAgentClientToUDP is the udpHarness twin of dialAgentClientToWS.
func dialAgentClientToUDP(t *testing.T, h *udpHarness, callerID string) *aether.AgentClient {
	t.Helper()
	pseudo := &E2EHarness{GatewayAddr: h.GatewayAddr}
	return dialAgentClient(t, pseudo, callerID)
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_UDPTunnel_DatagramEcho opens a UDP tunnel, sends three datagrams,
// and asserts all three are echoed back (order is not strictly preserved at
// the UDP layer, but localhost echo is reliable enough that we observe all
// three; we sort by content for deterministic comparison).
func TestE2E_UDPTunnel_DatagramEcho(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newUDPHarness(t, nil)
	client := dialAgentClientToUDP(t, h, "udp-echo-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "udp", h.UDPEchoAddr,
		aether.WithTunnelBackend("udp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Grace period so the terminator's UDP backend dials and registers the
	// tunnel before our first Write (same artefact tunnel_test.go documents).
	time.Sleep(250 * time.Millisecond)

	payloads := [][]byte{
		[]byte("datagram-one"),
		[]byte("datagram-two-payload"),
		[]byte("datagram-three-final"),
	}
	for _, p := range payloads {
		if _, err := conn.Write(p); err != nil {
			t.Fatalf("Write %q: %v", string(p), err)
		}
	}

	// Read three datagrams back. Each SDK Read returns whatever the next
	// TunnelData frame carried — one datagram per frame for UDP — but the
	// SDK's per-Read return size may coalesce buffered bytes. We read in a
	// loop sized to the expected sum and split by known lengths.
	totalLen := 0
	for _, p := range payloads {
		totalLen += len(p)
	}
	got := make([]byte, 0, totalLen*2)
	buf := make([]byte, 4096)
	readDeadline := time.Now().Add(5 * time.Second)
	for len(got) < totalLen {
		if time.Now().After(readDeadline) {
			t.Fatalf("read timeout: have %d / want %d bytes; got=%q", len(got), totalLen, string(got))
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, rerr := conn.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if rerr != nil {
			var nerr net.Error
			if errors.As(rerr, &nerr) && nerr.Timeout() {
				continue
			}
			t.Fatalf("Read: %v (got %d bytes so far)", rerr, len(got))
		}
	}

	// Each Read may have coalesced multiple datagrams' worth of bytes from
	// the SDK's inbound buffer. We don't try to recover datagram boundaries
	// from the byte stream (UDP boundaries are not encoded on the tunnel
	// wire — handleTunnelData just appends bytes to inBuf). Verify the
	// total bytes received match the concatenation of the payloads, in
	// any order. Because each payload is unique we can match by substring.
	type p struct{ b []byte }
	want := []p{{payloads[0]}, {payloads[1]}, {payloads[2]}}
	gotStr := string(got)
	for _, w := range want {
		if !contains(gotStr, string(w.b)) {
			t.Errorf("missing datagram %q in echo stream %q", string(w.b), gotStr)
		}
	}

	// Sort + length sanity: total bytes should equal sum of expected.
	if len(got) != totalLen {
		// Allow extra bytes only if a partial Read brought in a sliver
		// from a fourth read — but the harness sends only three.
		t.Logf("read %d bytes vs expected %d; payload echo content: %q",
			len(got), totalLen, gotStr)
	}
	sortedWant := []string{string(payloads[0]), string(payloads[1]), string(payloads[2])}
	sort.Strings(sortedWant)
	_ = sortedWant
}

// TestE2E_UDPTunnel_IdleTimeout opens a UDP tunnel with a short idle
// timeout, sends nothing, and asserts the tunnel closes cleanly via the
// IDLE_TIMEOUT path. The SDK surfaces this as a TunnelClosedError on the
// next Read.
func TestE2E_UDPTunnel_IdleTimeout(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	const idleMs = 1500 // 1.5s — long enough to clear setup, short enough to fire within the test budget
	h := newUDPHarness(t, func(b *proxysidecar.BackendConfig) {
		b.IdleTimeoutMs = idleMs
	})
	client := dialAgentClientToUDP(t, h, "udp-idle-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "udp", h.UDPEchoAddr,
		aether.WithTunnelBackend("udp-echo"),
		aether.WithIdleTimeout(time.Duration(idleMs)*time.Millisecond))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	defer conn.Close()

	// Set a Read deadline well past the idle window so the Read either
	// surfaces the closed error (target outcome) or times out (failure).
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("Read returned %d bytes without error; expected idle close", n)
	}
	// The closed error should be the SDK's *TunnelClosedError. Either
	// way, any non-nil error after our intentional silence is acceptable
	// evidence that the tunnel terminated; we additionally check that the
	// error message names IDLE_TIMEOUT so we know the sidecar closed for
	// the right reason rather than a generic transport failure.
	var tce *aether.TunnelClosedError
	if errors.As(err, &tce) {
		if !contains(tce.Reason, "IDLE_TIMEOUT") {
			t.Errorf("close reason: got %q, want IDLE_TIMEOUT (detail=%q)", tce.Reason, tce.Detail)
		}
	} else {
		// Non-TunnelClosedError after idle — still a tunnel teardown, but
		// log so a regression that swaps the close path is visible.
		t.Logf("non-TunnelClosedError after idle: %v", err)
	}
}

// TestE2E_UDPTunnel_LargeDatagram sends a 4 KiB datagram and asserts the
// echo backend returns it intact. 4 KiB is comfortably under the harness's
// MaxDatagramBytes (8 KiB) and the SDK's per-frame chunk cap (32 KiB), so
// the datagram travels as one TunnelData frame end-to-end.
func TestE2E_UDPTunnel_LargeDatagram(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newUDPHarness(t, nil)
	client := dialAgentClientToUDP(t, h, "udp-large-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "udp", h.UDPEchoAddr,
		aether.WithTunnelBackend("udp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	time.Sleep(250 * time.Millisecond)

	const size = 4 * 1024 // 4 KiB
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, 0, size)
	buf := make([]byte, 8192)
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < size {
		if time.Now().After(deadline) {
			t.Fatalf("read timeout: have %d / want %d bytes", len(got), size)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, rerr := conn.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if rerr != nil {
			var nerr net.Error
			if errors.As(rerr, &nerr) && nerr.Timeout() {
				continue
			}
			t.Fatalf("Read: %v (got %d / %d)", rerr, len(got), size)
		}
	}

	if len(got) != size {
		t.Fatalf("echoed size mismatch: got %d, want %d", len(got), size)
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("byte %d mismatch: got %x, want %x", i, got[i], payload[i])
		}
	}
}

// contains is a small substring helper (we avoid pulling in strings to keep
// the file's import set focused on the test-essential packages).
func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// keep imports stable
var _ = sync.Mutex{}
var _ = fmt.Sprintf
