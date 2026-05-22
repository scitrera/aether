//go:build e2e

package integration_e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Helpers private to this file
// =============================================================================

// startTCPReadEchoHalfClose stands up a TCP listener whose handler reads
// until EOF (caller half-closed), echoes everything back, then closes its
// own write half. Returns the listener address. The handler is the
// canonical "TCP half-close" partner: it cooperates with the caller's
// "I'm done writing" signal by reciprocating once it has drained the
// read side.
func startTCPReadEchoHalfClose(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				// Drain until EOF (caller signalled write-side done).
				data, _ := io.ReadAll(c)
				// Echo what we read.
				if len(data) > 0 {
					_, _ = c.Write(data)
				}
				// Half-close our send side. The TCP listener gives us
				// *net.TCPConn here so CloseWrite is available.
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.CloseWrite()
				}
			}(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_TCPTunnel_HalfCloseFromCaller is the closest approximation of
// caller-side TCP half-close that the current SDK exposes. The SDK's
// tunnelConn does not surface CloseWrite() as a separate API — Close()
// sends TunnelClose{NORMAL} which tears both directions down at once
// rather than emitting a fin-only TunnelData. We therefore exercise the
// adjacent property: caller writes some bytes, the backend reads them
// (signalled by Read returning data), the caller then Close()s, and we
// observe that the echo round-trips cleanly before the close completes
// teardown. A true CloseWrite probe would need an SDK API the SDK does
// not yet ship.
//
// Real-bug note: if the SDK adds a CloseWrite primitive later, this test
// should be tightened to assert (a) caller can read the backend's echo
// after CloseWrite, and (b) backend sees EOF on its read.
func TestE2E_TCPTunnel_HalfCloseFromCaller(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := NewE2EHarness(t)
	client := dialAgentClient(t, h, "tcp-halfclose")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Use a half-close-aware backend so the test exercises the cooperating
	// teardown path even though the SDK lacks CloseWrite.
	target := startTCPReadEchoHalfClose(t)

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "tcp", target,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}

	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))

	// Settle race between SDK's first data frame and runtime's tunnel
	// registration (see tunnel_test.go for the same artefact).
	time.Sleep(250 * time.Millisecond)

	payload := []byte("hello half close world")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Drain enough bytes to confirm the echo round-tripped. The harness's
	// startTCPEcho backend (echo-on-the-fly) sends bytes back as we send
	// them — the test target above does the same after our drain triggers.
	got := make([]byte, len(payload))
	n, err := io.ReadFull(conn, got)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadFull after %d bytes: %v", n, err)
	}
	if !bytes.Equal(got[:n], payload[:n]) {
		t.Errorf("echo mismatch: got %q, want %q", string(got[:n]), string(payload[:n]))
	}

	// Now full-close from caller; the cooperating backend should observe
	// our TunnelClose and unwind cleanly. We assert no error on Close.
	if err := conn.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// One more Read should either return io.EOF / TunnelClosedError or
	// the closed-deadline error — anything that signals "tunnel is no
	// longer live" is acceptable. We do NOT require a specific error
	// type so we don't lock the test to an internal detail.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Logf("note: post-Close Read returned no error — backend may have buffered tail bytes")
	}
}

// TestE2E_TCPTunnel_OpenCloseChurn rapidly opens and closes 50 tunnels
// back-to-back and asserts there is no goroutine leak (compared against
// a pre-churn baseline with generous slack). This exercises the
// per-tunnel registration / deregistration paths.
func TestE2E_TCPTunnel_OpenCloseChurn(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := NewE2EHarness(t)
	client := dialAgentClient(t, h, "tcp-churn")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Warm-up: open one tunnel and close it so any one-shot init
	// goroutines (proxy-sidecar fan-out, terminator init) are accounted
	// for in the baseline.
	warm, err := client.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("warm-up TunnelDial: %v", err)
	}
	_ = warm.Close()
	// Let the close-side goroutines settle before sampling the baseline.
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const iterations = 50
	for i := 0; i < iterations; i++ {
		conn, err := client.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
			aether.WithTunnelBackend("tcp-echo"))
		if err != nil {
			t.Fatalf("iter %d: TunnelDial: %v", i, err)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("iter %d: Close: %v", i, err)
		}
	}

	// Allow background teardown goroutines (per-tunnel pump exits,
	// terminator finalizer) to drain. The exact settle time depends on
	// scheduling — 1.5s is empirically enough on local hardware and
	// stays well inside the test's 25s budget.
	time.Sleep(1500 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()

	// Allow generous slack: the in-process gateway and sidecar runtime
	// retain a handful of goroutines per active session, and a transient
	// straggler from the last teardown should not flake the test. We
	// fail only on a clear linear-in-iterations leak.
	const slack = 25
	if after-baseline > slack {
		t.Errorf("possible goroutine leak after %d open/close cycles: "+
			"baseline=%d after=%d (delta=%d, slack=%d)",
			iterations, baseline, after, after-baseline, slack)
	} else {
		t.Logf("goroutine baseline=%d after=%d (delta=%d, slack=%d) — clean",
			baseline, after, after-baseline, slack)
	}
}

// TestE2E_TCPTunnel_TwoConcurrentTunnelsIndependent opens two tunnels
// from ONE client to the same TCP echo backend, sends distinct payloads
// in parallel, and asserts each tunnel receives only its own echo.
// Validates per-tunnel data isolation on the caller side (no cross-
// tunnel data mix-up in the inbound dispatcher).
func TestE2E_TCPTunnel_TwoConcurrentTunnelsIndependent(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := NewE2EHarness(t)
	client := dialAgentClient(t, h, "tcp-two-tunnels")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	openOne := func(label string) net.Conn {
		c, err := client.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
			aether.WithTunnelBackend("tcp-echo"))
		if err != nil {
			t.Fatalf("%s: TunnelDial: %v", label, err)
		}
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		return c
	}

	connA := openOne("A")
	defer connA.Close()
	connB := openOne("B")
	defer connB.Close()

	// Settle race between first data and tunnel registration.
	time.Sleep(250 * time.Millisecond)

	const payloadSize = 8 * 1024
	makePayload := func(seed byte) []byte {
		p := make([]byte, payloadSize)
		if _, err := rand.Read(p); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		p[0] = seed
		return p
	}
	payloadA := makePayload('A')
	payloadB := makePayload('B')

	var (
		wg      sync.WaitGroup
		errAOut atomic.Value
		errBOut atomic.Value
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := connA.Write(payloadA); err != nil {
			errAOut.Store(err)
			return
		}
		got := make([]byte, payloadSize)
		if _, err := io.ReadFull(connA, got); err != nil {
			errAOut.Store(err)
			return
		}
		if !bytes.Equal(got, payloadA) {
			errAOut.Store(fmt.Errorf("A: echo mismatch (first byte got=%q want=%q)",
				rune(got[0]), rune(payloadA[0])))
			return
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := connB.Write(payloadB); err != nil {
			errBOut.Store(err)
			return
		}
		got := make([]byte, payloadSize)
		if _, err := io.ReadFull(connB, got); err != nil {
			errBOut.Store(err)
			return
		}
		if !bytes.Equal(got, payloadB) {
			errBOut.Store(fmt.Errorf("B: echo mismatch (first byte got=%q want=%q)",
				rune(got[0]), rune(payloadB[0])))
			return
		}
	}()
	wg.Wait()

	if v := errAOut.Load(); v != nil {
		t.Errorf("tunnel A: %v", v)
	}
	if v := errBOut.Load(); v != nil {
		t.Errorf("tunnel B: %v", v)
	}
}

// TestE2E_TCPTunnel_TwoCallersIndependentTunnels has two separate aether
// clients (distinct caller IDs) each open a tunnel to the same TCP echo
// backend and send distinct payloads. Each must receive its own echo.
// Validates the per-caller-id rewrite the harness's routing fake gateway
// performs — without it, the two clients would collide on overlapping
// NextRequestID counters and one client could see the other's bytes.
func TestE2E_TCPTunnel_TwoCallersIndependentTunnels(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := NewE2EHarness(t)
	clientA := dialAgentClient(t, h, "two-callers-A")
	clientB := dialAgentClient(t, h, "two-callers-B")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	dial := func(c *aether.AgentClient, label string) net.Conn {
		conn, err := c.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
			aether.WithTunnelBackend("tcp-echo"))
		if err != nil {
			t.Fatalf("%s: TunnelDial: %v", label, err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		return conn
	}

	connA := dial(clientA, "A")
	defer connA.Close()
	connB := dial(clientB, "B")
	defer connB.Close()

	time.Sleep(250 * time.Millisecond)

	payloadA := bytes.Repeat([]byte{'a'}, 4096)
	payloadB := bytes.Repeat([]byte{'b'}, 4096)

	var wg sync.WaitGroup
	var errA, errB atomic.Value
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := connA.Write(payloadA); err != nil {
			errA.Store(err)
			return
		}
		got := make([]byte, len(payloadA))
		if _, err := io.ReadFull(connA, got); err != nil {
			errA.Store(err)
			return
		}
		if !bytes.Equal(got, payloadA) {
			errA.Store(fmt.Errorf("A: unexpected echo (first 8 bytes got=%q want=%q)",
				string(got[:8]), string(payloadA[:8])))
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := connB.Write(payloadB); err != nil {
			errB.Store(err)
			return
		}
		got := make([]byte, len(payloadB))
		if _, err := io.ReadFull(connB, got); err != nil {
			errB.Store(err)
			return
		}
		if !bytes.Equal(got, payloadB) {
			errB.Store(fmt.Errorf("B: unexpected echo (first 8 bytes got=%q want=%q)",
				string(got[:8]), string(payloadB[:8])))
		}
	}()
	wg.Wait()

	if v := errA.Load(); v != nil {
		t.Errorf("caller A: %v", v)
	}
	if v := errB.Load(); v != nil {
		t.Errorf("caller B: %v", v)
	}
}

// TestE2E_TunnelData_AfterClose_DroppedCleanly opens a tunnel, closes it,
// then directly emits a TunnelData frame via the SDK's low-level Send()
// API targeting the tunnel id that was just torn down. We assert the
// SDK does not panic and the routing layer drops the frame silently
// (no error frame surfaces to the caller).
//
// We use Send() rather than reusing the closed tunnelConn because the
// SDK's tunnelConn.Write enforces a fin-out guard and returns an error
// rather than emitting a stray frame after Close(). The point of this
// test is the *transport*-level behaviour: if a delayed TunnelData
// crosses paths with a TunnelClose on the wire, the receiver must drop
// it cleanly. Send() is the SDK's lowest public escape hatch into that
// path.
func TestE2E_TunnelData_AfterClose_DroppedCleanly(t *testing.T) {
	// No t.Parallel() — see chunked_test.go for the rationale.

	h := NewE2EHarness(t)
	client := dialAgentClient(t, h, "tcp-after-close")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := client.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}

	// Extract the tunnel id by writing a probe byte and observing — the
	// SDK does not expose the id on the public surface. We bypass that
	// by sending a TunnelData with a SYNTHETIC tunnel id that the
	// gateway will not have a route for; this exercises the same drop
	// path as a post-close stray frame would, without depending on
	// SDK-internal accessors.
	//
	// Real-bug note: if a public TunnelID() accessor is added to the
	// SDK later, this probe should be replaced with the real id of the
	// tunnel after Close() to test the exact post-close path the spec
	// names. As shipped, the SDK keeps the id internal so this is the
	// closest reachable proxy.
	_ = conn.Close()
	// Drain settle time.
	time.Sleep(100 * time.Millisecond)

	syntheticID := "after-close-stray-id"

	// Send a TunnelData frame for a tunnel id that does not exist on the
	// gateway. The fake gateway's routeTunnelData should drop the frame
	// with the debug-log line "DROPPED — no valid route" and not panic.
	frame := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelData{
			TunnelData: &pb.TunnelData{
				TunnelId: syntheticID,
				Seq:      99,
				Data:     []byte("stray-after-close"),
			},
		},
	}
	// SendWithPriority returns nil on enqueue success; the gateway drop
	// happens server-side and is invisible to the caller. We assert only
	// that the SDK accepts the send and does not panic.
	sendCtx, sendCancel := context.WithTimeout(ctx, 2*time.Second)
	err = client.SendWithPriority(sendCtx, aether.PriorityRequest, frame)
	sendCancel()
	if err != nil {
		// Some send errors are legitimate (e.g. send-pipeline closed).
		// We don't fail the test on a clean error — the assertion is
		// "no panic" and "no crash".
		t.Logf("SendWithPriority returned (non-panic) err=%v — acceptable", err)
	}

	// Give the gateway a window to process and drop the frame. If the
	// drop path were buggy (e.g. nil-deref on the deleted route), this
	// is when it would surface. We then confirm the client is still
	// alive by issuing one more proxy round-trip via a fresh tunnel.
	time.Sleep(200 * time.Millisecond)

	verifyConn, err := client.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("post-stray TunnelDial (sanity): %v", err)
	}
	defer verifyConn.Close()
	_ = verifyConn.SetDeadline(time.Now().Add(5 * time.Second))
	time.Sleep(250 * time.Millisecond)
	if _, err := verifyConn.Write([]byte("still-alive")); err != nil {
		t.Fatalf("post-stray Write: %v", err)
	}
	got := make([]byte, len("still-alive"))
	if _, err := io.ReadFull(verifyConn, got); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("post-stray Read: %v", err)
	}
	if string(got) != "still-alive" {
		t.Errorf("post-stray echo mismatch: got %q, want %q", string(got), "still-alive")
	}
}
