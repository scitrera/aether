//go:build e2e

package integration_e2e

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Custom harness — WS/UDP backends registered on the sidecar
// =============================================================================
//
// The shared NewE2EHarness only registers HTTP+TCP terminator backends. The
// WS and UDP tunnel coverage tests need a sidecar whose terminator config
// includes a WS or UDP backend pointing at an in-process echo server, so we
// stand up the full stack inline here. The construction mirrors NewE2EHarness
// from harness.go but with extra Backends entries. We re-use harness.go's
// package-private helpers (getAetherlite, newHTTPBackend, waitForSidecarReady)
// so the routing semantics are identical to the shared harness.
//
// We also extend the existing dialAgentClient pattern by using its public
// signature unchanged — the AgentClient just needs the gateway address, and
// the real aetherlite forwards tunnel envelopes to whichever Service
// registers, so a custom harness "just works" with dialAgentClient.

// wsHarness bundles a sidecar+gateway stack whose terminator points its
// "ws-echo" backend at an in-process gorilla/websocket echo server.
type wsHarness struct {
	GatewayAddr  string
	ServiceTopic string
	WSEchoURL    string // ws://127.0.0.1:port — for the SDK to pass as remote_hint when needed
}

// newWSHarness boots: routing fake gateway, an in-process WS echo server
// (subprotocols configurable), and a composite-mode sidecar with a "ws-echo"
// backend. The optional configureBackend hook lets a test override e.g.
// MaxBytes or IdleTimeoutMs on the ws backend.
func newWSHarness(t *testing.T, subprotocols []string, configureBackend func(*proxysidecar.BackendConfig)) *wsHarness {
	t.Helper()

	gw := getAetherlite(t)
	gwAddr := gw.grpcAddr

	wsURL := startWSEchoServer(t, subprotocols)
	// HTTP backend exists only to satisfy the terminator's "at least one
	// HTTP backend" expectations for code paths that probe it — and to
	// mirror the production sidecar shape where multiple kinds coexist.
	httpBackend := newHTTPBackend(t, 5*time.Second)

	wsBackend := proxysidecar.BackendConfig{
		Name:          "ws-echo",
		Kind:          proxysidecar.BackendKindWS,
		URL:           wsURL,
		MaxBytes:      32 << 20,
		IdleTimeoutMs: 60_000,
	}
	if configureBackend != nil {
		configureBackend(&wsBackend)
	}

	// Unique-ify the specifier so multiple WS-harness tests don't
	// collide on the shared aetherlite's (impl, spec) identity.
	uniqueSpec := fmt.Sprintf("e2e-ws-%d", nextSidecarSpec.Add(1))

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
				wsBackend,
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
			t.Logf("warning: ws-harness runner did not exit within 15s")
		}
	})

	serviceTopic := fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
	if err := waitForSidecarReady(t, gwAddr, serviceTopic, httpBackend.URL); err != nil {
		t.Fatalf("ws-harness sidecar never reached ready state: %v", err)
	}

	return &wsHarness{
		GatewayAddr:  gwAddr,
		ServiceTopic: serviceTopic,
		WSEchoURL:    wsURL,
	}
}

// startWSEchoServer brings up a gorilla/websocket echo server that mirrors
// every received message back to the caller. The exposed URL uses ws://
// scheme so the sidecar's wsBackend accepts it.
func startWSEchoServer(t *testing.T, subprotocols []string) string {
	t.Helper()
	upgrader := websocket.Upgrader{
		Subprotocols:    subprotocols,
		ReadBufferSize:  256 * 1024,
		WriteBufferSize: 256 * 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("ws upgrade error: %v", err)
			return
		}
		defer c.Close()
		for {
			mt, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws://" + strings.TrimPrefix(srv.URL, "http://")
}

// dialAgentClientToWS adapts the streaming_test.go dialAgentClient helper to
// the wsHarness shape (which exposes the same GatewayAddr field but is a
// different type). Mirrors dialAgentClient's bootstrap exactly.
func dialAgentClientToWS(t *testing.T, h *wsHarness, callerID string) *aether.AgentClient {
	t.Helper()
	pseudo := &E2EHarness{GatewayAddr: h.GatewayAddr}
	return dialAgentClient(t, pseudo, callerID)
}

// =============================================================================
// WS framing helpers — match the wire format the sidecar emits
// =============================================================================
//
// The sidecar's wsTunnel encodes each WS message onto the byte stream as
// [4-byte big-endian length][payload]. In untagged (default) mode the
// length is just the payload length; in tagged mode the first byte of the
// payload is a kind tag (0x00 binary / 0x01 text / 0xFF negotiation
// preamble). See wsTunnel.handleData and wsTunnel.pumpToCaller for the
// definitive encoding. encodeWSPayload writes a single untagged frame.

// encodeWSPayload frames a single untagged WS message for the inbound byte
// stream the sidecar reassembles into one WS WriteMessage call.
func encodeWSPayload(payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(len(payload)))
	copy(out[4:], payload)
	return out
}

// readWSFrame reads one length-prefixed WS message from r. Returns the
// payload bytes (without the length prefix). io.EOF before any byte is
// read returns (nil, io.EOF); a short read mid-frame returns
// io.ErrUnexpectedEOF.
func readWSFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_WSTunnel_BasicRoundTrip opens a WS tunnel through the sidecar,
// writes one text message, and asserts it round-trips through the
// gorilla/websocket echo server.
func TestE2E_WSTunnel_BasicRoundTrip(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newWSHarness(t, nil, nil)
	client := dialAgentClientToWS(t, h, "ws-basic-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "ws", h.WSEchoURL,
		aether.WithTunnelBackend("ws-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Small grace so the runtime registers the tunnel with the terminator
	// before the first data frame arrives (in-process gateway is faster
	// than production; tunnel_test.go documents the same artefact).
	time.Sleep(250 * time.Millisecond)

	payload := []byte("hello-ws-world")
	if _, err := conn.Write(encodeWSPayload(payload)); err != nil {
		t.Fatalf("tunnel Write: %v", err)
	}

	got, err := readWSFrame(conn)
	if err != nil {
		t.Fatalf("readWSFrame: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("echo mismatch: got %q, want %q", string(got), string(payload))
	}
}

// TestE2E_WSTunnel_BinaryFrameLargePayload sends a 1 MiB binary frame and
// asserts the sidecar reassembles + echoes it back intact. A 1 MiB WS
// message necessarily fragments across multiple gRPC TunnelData frames
// (per-frame cap is 256 KiB on the sidecar's outbound and 32 KiB on the
// SDK's), so this exercises the reassembler on both sides.
func TestE2E_WSTunnel_BinaryFrameLargePayload(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newWSHarness(t, nil, nil)
	client := dialAgentClientToWS(t, h, "ws-large-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "ws", h.WSEchoURL,
		aether.WithTunnelBackend("ws-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	time.Sleep(250 * time.Millisecond)

	const payloadSize = 1 << 20 // 1 MiB
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	// Spawn the reader before the write so we don't deadlock on a small
	// SDK inbound buffer.
	type readResult struct {
		data []byte
		err  error
	}
	rch := make(chan readResult, 1)
	go func() {
		got, err := readWSFrame(conn)
		rch <- readResult{data: got, err: err}
	}()

	if _, err := conn.Write(encodeWSPayload(payload)); err != nil {
		t.Fatalf("tunnel Write: %v", err)
	}

	select {
	case res := <-rch:
		if res.err != nil {
			t.Fatalf("readWSFrame: %v", res.err)
		}
		if len(res.data) != payloadSize {
			t.Fatalf("echoed payload size: got %d, want %d", len(res.data), payloadSize)
		}
		for i := range payload {
			if res.data[i] != payload[i] {
				t.Fatalf("byte %d mismatch: got %x, want %x", i, res.data[i], payload[i])
			}
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for 1 MiB echo")
	}
}

// TestE2E_WSTunnel_SubProtocolNegotiation opens the tunnel with a
// "subprotocols" metadata entry and asserts the sidecar emits the
// negotiation preamble frame announcing the selected subprotocol before
// any echoed data. The preamble format is documented in tunnel_ws.go:
// a length-prefixed frame whose first byte is wsControlTagNegotiation
// (0xFF) followed by the selected subprotocol bytes.
func TestE2E_WSTunnel_SubProtocolNegotiation(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := newWSHarness(t, []string{"v2"}, nil)
	client := dialAgentClientToWS(t, h, "ws-subproto-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "ws", h.WSEchoURL,
		aether.WithTunnelBackend("ws-echo"),
		aether.WithMetadata(map[string]string{
			"subprotocols": "v1,v2",
		}))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	time.Sleep(250 * time.Millisecond)

	// Drive a single ping so the preamble is flushed alongside the echo.
	payload := []byte("ping")
	if _, err := conn.Write(encodeWSPayload(payload)); err != nil {
		t.Fatalf("tunnel Write: %v", err)
	}

	// The first frame must be the negotiation preamble. Its payload is
	// [0xFF][selected-subprotocol-bytes].
	preamble, err := readWSFrame(conn)
	if err != nil {
		t.Fatalf("readWSFrame preamble: %v", err)
	}
	if len(preamble) < 1 || preamble[0] != 0xFF {
		t.Fatalf("expected negotiation preamble tag 0xFF first; got tag=0x%02x payload=%q",
			func() byte {
				if len(preamble) > 0 {
					return preamble[0]
				}
				return 0
			}(), string(preamble))
	}
	if got := string(preamble[1:]); got != "v2" {
		t.Errorf("negotiated subprotocol: got %q, want %q", got, "v2")
	}

	// Then the echoed ping.
	got, err := readWSFrame(conn)
	if err != nil {
		t.Fatalf("readWSFrame echo: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("echo mismatch: got %q, want %q", string(got), string(payload))
	}
}

// keep imports stable across edits
var _ net.Conn = (*net.TCPConn)(nil)
var _ = fmt.Sprintf
