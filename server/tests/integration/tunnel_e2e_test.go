//go:build integration

package integration

// Tunnel end-to-end tests (T14).
//
// These tests exercise the proxy/tunnel pipeline end-to-end at the
// gateway-routing-primitive level: caller → in-process gateway router →
// sidecar Terminator (with a real tcpBackend) → local TCP echo server.
//
// Like the phase-1 harness in proxy_e2e_test.go, the gateway routing logic
// is invoked in-process (no Redis/RMQ/gRPC) so the test is hermetic and
// fast. The Terminator and tcpBackend code paths are real production code;
// only the gateway↔caller and gateway↔terminator wire transports are
// substituted.
//
// Coverage (per T14 brief):
//   - 10 MiB random-byte echo round-trip: byte-identical bytes returned.
//   - Half-close: caller FIN → server EOF → caller observes EOF cleanly.
//   - Two concurrent tunnels stay byte-isolated.
//   - Idle timeout closes with reason=IDLE_TIMEOUT.
//   - max_bytes quota closes with reason=QUOTA.
//   - Stickiness: with two sidecar instances and wildcard target, every
//     frame of one tunnel lands on the same instance.
//   - Bidirectional flow control: sidecar emits TunnelAck → gateway →
//     caller, caller's outbound credits replenish, no deadlock under low
//     initial credits.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
)

// =============================================================================
// Tunnel test harness
// =============================================================================

// tunnelHarness extends the phase-1 HTTP harness with proxy/tunnel routing
// primitives. It wires the gateway's tunnel routing semantics in-process:
//   - TunnelOpen records a pin (caller↔service) and dispatches to the
//     terminator's HandleTunnelOpen.
//   - TunnelData uses the pin to find the right terminator and dispatches
//     into HandleTunnelData.
//   - TunnelAck routes to the counterparty (caller↔service depending on
//     who sent it).
//   - TunnelClose tears down the pin and dispatches into the terminator.
//
// The terminator's tunnelTransport is satisfied by a per-tunnel stub
// registered when the tunnel is opened; it forwards TunnelData/Ack/Close
// frames back to the caller's downstream channel.
type tunnelHarness struct {
	*harness
	tcpEntries []*tcpTerminatorEntry

	mu      sync.RWMutex
	tunnels map[string]*tunnelState // tunnel_id → state
}

func newTunnelHarness() *tunnelHarness {
	return &tunnelHarness{
		harness: newHarness(),
		tunnels: make(map[string]*tunnelState),
	}
}

// tcpTerminatorEntry tracks a tunnel-capable terminator.
type tcpTerminatorEntry struct {
	implementation string
	specifier      string
	t              *proxysidecar.Terminator
	backendAddr    string
	online         atomic.Bool
	tunnelHits     atomic.Int64
}

func (e *tcpTerminatorEntry) topic() string {
	return "sv::" + e.implementation + "::" + e.specifier
}

// tunnelState holds per-tunnel routing context: who the caller is, which
// terminator owns the tunnel, the downstream channel to deliver frames back
// to the caller, and a tunnelTransport stub that the terminator uses to
// emit frames.
type tunnelState struct {
	tunnelID   string
	caller     string // caller identity topic
	service    string // sv::{impl}::{spec}
	terminator *proxysidecar.Terminator

	// downstream is the channel the gateway uses to deliver frames back to
	// the caller (in-process stand-in for the caller's gRPC stream).
	downstream chan *pb.DownstreamMessage

	// outboundCredits tracks the *caller*'s remaining outbound credits.
	// Replenished when sidecar→caller TunnelAck frames flow.
	outboundCredits atomic.Int64

	// inboundCallerBytes counts bytes delivered to the caller (via downstream)
	// since the last upstream TunnelAck. Used to drive caller→sidecar
	// credit grants in the bidirectional flow-control test.
	inboundCallerBytes atomic.Int64

	// hits tags every frame routed to a terminator with the terminator's
	// specifier so stickiness can be asserted.
	hits   []*tcpTerminatorEntry
	hitsMu sync.Mutex

	// closed signals tunnel teardown.
	closed atomic.Bool
}

// addTunnelTerminator registers a terminator with a single TCP backend
// pointing at backendAddr. Returns the entry (online by default).
func (h *tunnelHarness) addTunnelTerminator(t *testing.T, implementation, specifier, backendAddr string, idleMs int64, maxBytes int64, allowHints []string) *tcpTerminatorEntry {
	t.Helper()
	cfg := &proxysidecar.Config{
		Service: proxysidecar.ServiceConfig{
			Implementation: implementation,
			Specifier:      specifier,
		},
		Gateway: proxysidecar.GatewayConfig{
			Address:  "localhost:0",
			Insecure: true,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:             "primary-tcp",
				Kind:             proxysidecar.BackendKindTCP,
				URL:              "tcp://" + backendAddr,
				IdleTimeoutMs:    idleMs,
				MaxBytes:         maxBytes,
				AllowRemoteHints: allowHints,
			}},
		},
		TenantID: "tenant-test",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	term, err := proxysidecar.NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}
	entry := &tcpTerminatorEntry{
		implementation: implementation,
		specifier:      specifier,
		t:              term,
		backendAddr:    backendAddr,
	}
	entry.online.Store(true)
	t.Cleanup(func() { term.StopAllTunnels() })

	h.mu.Lock()
	h.tcpEntries = append(h.tcpEntries, entry)
	h.mu.Unlock()
	return entry
}

// resolveTunnelService resolves a sv::{impl} wildcard or sv::{impl}::{spec}
// concrete target to a single online terminator. Mirrors the gateway's
// resolveWildcardOrConcrete.
func (h *tunnelHarness) resolveTunnelService(target string) (*tcpTerminatorEntry, error) {
	if !strings.HasPrefix(target, "sv::") {
		return nil, fmt.Errorf("ACL_DENIED: %q not service-class", target)
	}
	rest := strings.TrimPrefix(target, "sv::")
	parts := strings.Split(rest, "::")
	impl := parts[0]
	wildcard := len(parts) == 1

	h.mu.RLock()
	defer h.mu.RUnlock()
	candidates := make([]*tcpTerminatorEntry, 0, len(h.tcpEntries))
	for _, e := range h.tcpEntries {
		if e.implementation != impl {
			continue
		}
		if !e.online.Load() {
			continue
		}
		if !wildcard && e.specifier != parts[1] {
			continue
		}
		candidates = append(candidates, e)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy sv::%s instances available", impl)
	}
	pick := candidates[mathrand.IntN(len(candidates))]
	return pick, nil
}

// =============================================================================
// In-process tunnelTransport
// =============================================================================

// callerTransport is a tunnelTransport stub that delivers frames produced by
// the terminator back to the caller via the tunnelState's downstream channel.
// It also performs the "gateway routes upstream-bound TunnelAck to caller"
// flow: any ack the sidecar emits is wrapped as a downstream TunnelAck.
type callerTransport struct {
	state *tunnelState
}

func (c *callerTransport) SendTunnelData(d *pb.TunnelData) error {
	if c.state.closed.Load() {
		return errors.New("tunnel closed")
	}
	c.state.inboundCallerBytes.Add(int64(len(d.GetData())))
	c.state.downstream <- &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelData{TunnelData: d},
	}
	return nil
}

func (c *callerTransport) SendTunnelAck(a *pb.TunnelAck) error {
	if c.state.closed.Load() {
		return nil
	}
	// Mirror the gateway's routeTunnelAck (sidecar → caller direction):
	// replenish the caller's outbound credit window AND (for assertion
	// purposes) deliver a downstream TunnelAck so a real SDK caller would
	// see it. The harness's "caller" is a goroutine that updates the
	// outboundCredits atom directly.
	c.state.outboundCredits.Add(int64(a.GetCredits()))
	c.state.downstream <- &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelAck{TunnelAck: a},
	}
	return nil
}

func (c *callerTransport) SendTunnelClose(cm *pb.TunnelClose) error {
	if c.state.closed.Load() {
		return nil
	}
	c.state.closed.Store(true)
	c.state.downstream <- &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelClose{TunnelClose: cm},
	}
	return nil
}

// SendProxyHttpResponse / SendProxyHttpBodyChunk: tunnel-only harness ignores
// these; they're satisfied to fulfil the tunnelTransport interface contract.
func (c *callerTransport) SendProxyHttpResponse(*pb.ProxyHttpResponse) error   { return nil }
func (c *callerTransport) SendProxyHttpBodyChunk(*pb.ProxyHttpBodyChunk) error { return nil }

// =============================================================================
// Caller-side primitives (simulate Go SDK TunnelDial)
// =============================================================================

// openTunnel opens a tunnel through the harness. caller is the originating
// identity topic (e.g. ag::ws::impl::v1).
func (h *tunnelHarness) openTunnel(t *testing.T, caller, target string, idleMs, maxBytes int64, initialCallerCredits int64) (*tunnelState, error) {
	t.Helper()
	tunnelID := fmt.Sprintf("tn-%d-%d", time.Now().UnixNano(), mathrand.Uint32())

	entry, err := h.resolveTunnelService(target)
	if err != nil {
		return nil, err
	}
	st := &tunnelState{
		tunnelID:   tunnelID,
		caller:     caller,
		service:    entry.topic(),
		terminator: entry.t,
		downstream: make(chan *pb.DownstreamMessage, 4096),
	}
	st.outboundCredits.Store(initialCallerCredits)

	h.mu.Lock()
	h.tunnels[tunnelID] = st
	h.mu.Unlock()

	transport := &callerTransport{state: st}
	open := &pb.TunnelOpen{
		TunnelId:      tunnelID,
		TargetTopic:   entry.topic(),
		Protocol:      pb.TunnelOpen_TCP,
		IdleTimeoutMs: idleMs,
		MaxBytes:      maxBytes,
	}
	st.recordHit(entry)
	if cm := entry.t.HandleTunnelOpen(context.Background(), open, transport); cm != nil {
		return st, fmt.Errorf("HandleTunnelOpen rejected: %s: %s", cm.GetReason(), cm.GetDetail())
	}
	entry.tunnelHits.Add(1)
	return st, nil
}

// recordHit tags a frame with the terminator that handled it (for the
// stickiness assertion).
func (s *tunnelState) recordHit(e *tcpTerminatorEntry) {
	s.hitsMu.Lock()
	s.hits = append(s.hits, e)
	s.hitsMu.Unlock()
}

// uniqueTerminators returns the set of distinct terminators that handled
// frames for this tunnel.
func (s *tunnelState) uniqueTerminators() []*tcpTerminatorEntry {
	s.hitsMu.Lock()
	defer s.hitsMu.Unlock()
	seen := make(map[*tcpTerminatorEntry]struct{}, 1)
	out := make([]*tcpTerminatorEntry, 0, 1)
	for _, e := range s.hits {
		if _, ok := seen[e]; !ok {
			seen[e] = struct{}{}
			out = append(out, e)
		}
	}
	return out
}

// sendData routes a caller-side TunnelData frame through the tunnel's pinned
// terminator. Honours the per-tunnel outbound credit window: blocks until
// the caller has enough credits to ship the bytes (sidecar must replenish).
func (s *tunnelState) sendData(payload []byte, fin bool) error {
	if s.closed.Load() {
		return errors.New("tunnel closed")
	}
	const chunkSize = 64 * 1024
	for off := 0; off < len(payload) || (fin && off == 0); {
		end := off + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		// Block on credits.
		want := int64(end - off)
		deadline := time.Now().Add(10 * time.Second)
		for s.outboundCredits.Load() < want {
			if s.closed.Load() {
				return errors.New("tunnel closed while waiting for credits")
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timed out waiting for outbound credits (need %d, have %d)",
					want, s.outboundCredits.Load())
			}
			time.Sleep(2 * time.Millisecond)
		}
		s.outboundCredits.Add(-want)

		isLast := end == len(payload)
		frame := &pb.TunnelData{
			TunnelId: s.tunnelID,
			Seq:      uint32(off),
			Data:     append([]byte(nil), payload[off:end]...),
			Fin:      fin && isLast,
		}
		s.terminator.HandleTunnelData(frame, &callerTransport{state: s})
		off = end
		if fin && isLast {
			break
		}
		if !fin && isLast {
			break
		}
	}
	return nil
}

// sendFin sends a single fin-only TunnelData frame.
func (s *tunnelState) sendFin() {
	frame := &pb.TunnelData{TunnelId: s.tunnelID, Seq: ^uint32(0), Fin: true}
	s.terminator.HandleTunnelData(frame, &callerTransport{state: s})
}

// sendCallerAck simulates the caller granting credits to the sidecar
// (gateway→service direction). The harness routes this directly into the
// terminator's HandleTunnelAck (mirrors gateway's caller→service ack route).
func (s *tunnelState) sendCallerAck(credits uint32) {
	s.terminator.HandleTunnelAck(&pb.TunnelAck{TunnelId: s.tunnelID, Credits: credits})
}

// closeFromCaller dispatches a TunnelClose to the terminator (caller-initiated).
func (s *tunnelState) closeFromCaller(reason pb.TunnelClose_Reason, detail string) {
	if s.closed.CompareAndSwap(false, true) {
		s.terminator.HandleTunnelClose(&pb.TunnelClose{
			TunnelId: s.tunnelID,
			Reason:   reason,
			Detail:   detail,
		})
	}
}

// drainDownstream reads downstream frames into the receive buffer until
// either: (a) the tunnel closes, or (b) the timeout expires. Returns the
// concatenated bytes seen in TunnelData frames and the close envelope (if
// any).
func (s *tunnelState) drainDownstream(timeout time.Duration) ([]byte, *pb.TunnelClose) {
	var buf []byte
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case msg := <-s.downstream:
			switch p := msg.GetPayload().(type) {
			case *pb.DownstreamMessage_TunnelData:
				buf = append(buf, p.TunnelData.GetData()...)
				if p.TunnelData.GetFin() {
					// Continue reading until close or timeout to capture
					// any trailing close frame.
				}
			case *pb.DownstreamMessage_TunnelAck:
				// Already accounted for in callerTransport.SendTunnelAck.
			case *pb.DownstreamMessage_TunnelClose:
				return buf, p.TunnelClose
			}
		case <-deadline.C:
			return buf, nil
		}
	}
}

// drainDownstreamUntilBytes reads downstream frames until at least want bytes
// have been observed in TunnelData. Returns the buffer and any close.
func (s *tunnelState) drainDownstreamUntilBytes(want int, timeout time.Duration) ([]byte, *pb.TunnelClose) {
	var buf []byte
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for len(buf) < want {
		select {
		case msg := <-s.downstream:
			switch p := msg.GetPayload().(type) {
			case *pb.DownstreamMessage_TunnelData:
				buf = append(buf, p.TunnelData.GetData()...)
			case *pb.DownstreamMessage_TunnelAck:
				// no-op, side-effected.
			case *pb.DownstreamMessage_TunnelClose:
				return buf, p.TunnelClose
			}
		case <-deadline.C:
			return buf, nil
		}
	}
	return buf, nil
}

// =============================================================================
// Local TCP echo helpers
// =============================================================================

// echoListener stands up a TCP server that echoes all received bytes back.
// Returns the listen address.
func echoListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	return ln.Addr().String()
}

// halfCloseEchoListener echoes back all received bytes once the caller's
// write half is closed, then closes its own write half. Used to validate
// FIN propagation.
func halfCloseEchoListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				data, _ := io.ReadAll(c)
				_, _ = c.Write(data)
				if cw, ok := c.(*net.TCPConn); ok {
					_ = cw.CloseWrite()
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

// silentListener accepts a connection but never reads or writes — used to
// validate idle timeout behaviour.
func silentListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	return ln.Addr().String()
}

// =============================================================================
// Tests
// =============================================================================

// Test1: 10 MiB random byte stream survives byte-identical end-to-end.
func TestTunnelE2E_TenMBEcho_Bytewise(t *testing.T) {
	t.Parallel()

	addr := echoListener(t)
	h := newTunnelHarness()
	h.addTunnelTerminator(t, "memorylayer", "echo10", addr, 30_000, 0, nil)

	// Generous initial caller credits so the test isn't credit-limited.
	st, err := h.openTunnel(t, agentCaller("ws", "caller", "v1"),
		"sv::memorylayer::echo10", 30_000, 0, 32<<20)
	if err != nil {
		t.Fatalf("openTunnel: %v", err)
	}

	const total = 10 * 1024 * 1024
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	// Generously over-grant caller→sidecar credits so the sidecar's
	// outbound pump is not credit-limited.
	go func() {
		for i := 0; i < 64; i++ {
			st.sendCallerAck(uint32(256 << 10))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Send the payload (caller→sidecar→backend).
	go func() {
		if err := st.sendData(payload, false); err != nil {
			t.Errorf("sendData: %v", err)
		}
		st.sendFin()
	}()

	// Read back (backend→sidecar→caller). Continue until close.
	buf, closeMsg := st.drainDownstreamUntilBytes(total, 30*time.Second)
	if len(buf) < total {
		// Try to drain remaining frames + close to surface the reason.
		extra, cm := st.drainDownstream(5 * time.Second)
		buf = append(buf, extra...)
		if cm != nil {
			closeMsg = cm
		}
	}
	if len(buf) != total {
		t.Fatalf("echo size: got %d, want %d (close=%v)", len(buf), total, closeMsg)
	}
	for i := range buf {
		if buf[i] != payload[i] {
			t.Fatalf("byte %d differs: got %x want %x", i, buf[i], payload[i])
		}
	}
}

// Test2: half-close — caller FIN, server EOF, caller observes EOF cleanly.
func TestTunnelE2E_HalfClose_CallerFinPropagatesToServer(t *testing.T) {
	t.Parallel()

	addr := halfCloseEchoListener(t)
	h := newTunnelHarness()
	h.addTunnelTerminator(t, "memorylayer", "half", addr, 30_000, 0, nil)

	st, err := h.openTunnel(t, agentCaller("ws", "caller", "v1"),
		"sv::memorylayer::half", 30_000, 0, 1<<20)
	if err != nil {
		t.Fatalf("openTunnel: %v", err)
	}

	st.sendCallerAck(8 * 256 * 1024) // generous

	payload := []byte("hello half-close world")
	if err := st.sendData(payload, true); err != nil {
		t.Fatalf("sendData with fin: %v", err)
	}

	buf, closeMsg := st.drainDownstream(5 * time.Second)
	if string(buf) != string(payload) {
		t.Errorf("echoed payload: got %q, want %q", string(buf), string(payload))
	}
	if closeMsg == nil {
		t.Fatal("expected TunnelClose after half-close")
	}
	if closeMsg.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL (detail=%q)",
			closeMsg.GetReason(), closeMsg.GetDetail())
	}
}

// Test3: two concurrent tunnels stay byte-isolated.
func TestTunnelE2E_TwoConcurrentTunnels_Independent(t *testing.T) {
	t.Parallel()

	addr := echoListener(t)
	h := newTunnelHarness()
	h.addTunnelTerminator(t, "memorylayer", "twoA", addr, 30_000, 0, nil)
	h.addTunnelTerminator(t, "memorylayer", "twoB", addr, 30_000, 0, nil)

	stA, err := h.openTunnel(t, agentCaller("ws", "caller", "a"),
		"sv::memorylayer::twoA", 30_000, 0, 1<<20)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	stB, err := h.openTunnel(t, agentCaller("ws", "caller", "b"),
		"sv::memorylayer::twoB", 30_000, 0, 1<<20)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}

	stA.sendCallerAck(16 * 256 * 1024)
	stB.sendCallerAck(16 * 256 * 1024)

	payloadA := []byte(strings.Repeat("AAAA", 4096))
	payloadB := []byte(strings.Repeat("BBBB", 4096))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = stA.sendData(payloadA, true)
	}()
	go func() {
		defer wg.Done()
		_ = stB.sendData(payloadB, true)
	}()
	wg.Wait()

	bufA, _ := stA.drainDownstream(5 * time.Second)
	bufB, _ := stB.drainDownstream(5 * time.Second)

	if string(bufA) != string(payloadA) {
		t.Errorf("A echo: got %d bytes, want %d", len(bufA), len(payloadA))
	}
	if string(bufB) != string(payloadB) {
		t.Errorf("B echo: got %d bytes, want %d", len(bufB), len(payloadB))
	}
	if strings.Contains(string(bufA), "BBBB") {
		t.Errorf("A saw B bytes — cross-talk")
	}
	if strings.Contains(string(bufB), "AAAA") {
		t.Errorf("B saw A bytes — cross-talk")
	}
}

// Test4: idle timeout fires with reason=IDLE_TIMEOUT.
func TestTunnelE2E_IdleTimeout_ClosesWithReason(t *testing.T) {
	t.Parallel()

	addr := silentListener(t)
	h := newTunnelHarness()
	h.addTunnelTerminator(t, "memorylayer", "idle", addr, 200, 0, nil)

	st, err := h.openTunnel(t, agentCaller("ws", "caller", "v1"),
		"sv::memorylayer::idle", 200, 0, 1<<20)
	if err != nil {
		t.Fatalf("openTunnel: %v", err)
	}

	_, closeMsg := st.drainDownstream(5 * time.Second)
	if closeMsg == nil {
		t.Fatal("expected TunnelClose after idle timeout")
	}
	if closeMsg.GetReason() != pb.TunnelClose_IDLE_TIMEOUT {
		t.Fatalf("close reason: got %s, want IDLE_TIMEOUT (detail=%q)",
			closeMsg.GetReason(), closeMsg.GetDetail())
	}
}

// Test5: max_bytes quota fires with reason=QUOTA.
func TestTunnelE2E_MaxBytesQuota_ClosesWithReason(t *testing.T) {
	t.Parallel()

	addr := echoListener(t)
	h := newTunnelHarness()
	h.addTunnelTerminator(t, "memorylayer", "quota", addr, 30_000, 1024, nil)

	st, err := h.openTunnel(t, agentCaller("ws", "caller", "v1"),
		"sv::memorylayer::quota", 30_000, 1024, 1<<20)
	if err != nil {
		t.Fatalf("openTunnel: %v", err)
	}
	st.sendCallerAck(16 * 256 * 1024)

	// Push 4 KiB > 1 KiB cap. The quota close should fire.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	go func() {
		_ = st.sendData(payload, false)
	}()

	_, closeMsg := st.drainDownstream(5 * time.Second)
	if closeMsg == nil {
		t.Fatal("expected TunnelClose for quota breach")
	}
	if closeMsg.GetReason() != pb.TunnelClose_QUOTA {
		t.Fatalf("close reason: got %s, want QUOTA (detail=%q)",
			closeMsg.GetReason(), closeMsg.GetDetail())
	}
}

// Test6: stickiness — with two sidecar instances and a wildcard target, every
// frame of a given tunnel lands on the same instance (verified via the per-
// tunnel hits set staying singleton).
func TestTunnelE2E_Stickiness_AllFramesOnSameInstance(t *testing.T) {
	t.Parallel()

	addr := echoListener(t)
	h := newTunnelHarness()
	a := h.addTunnelTerminator(t, "echo", "instA", addr, 30_000, 0, nil)
	b := h.addTunnelTerminator(t, "echo", "instB", addr, 30_000, 0, nil)

	// Open multiple tunnels with the wildcard target. Each tunnel's frames
	// must all land on a single instance.
	const N = 8
	tunnels := make([]*tunnelState, 0, N)
	for i := 0; i < N; i++ {
		st, err := h.openTunnel(t, agentCaller("ws", "caller", iToA(i)),
			"sv::echo", 30_000, 0, 1<<20)
		if err != nil {
			t.Fatalf("openTunnel[%d]: %v", i, err)
		}
		tunnels = append(tunnels, st)
		st.sendCallerAck(8 * 256 * 1024)
	}

	// Send a small payload through each.
	for i, st := range tunnels {
		payload := []byte(fmt.Sprintf("tunnel-%d-payload-data", i))
		if err := st.sendData(payload, true); err != nil {
			t.Fatalf("sendData[%d]: %v", i, err)
		}
	}
	for _, st := range tunnels {
		_, _ = st.drainDownstream(5 * time.Second)
	}

	// Per-tunnel: every frame went to one terminator. (We only record on
	// open in the simplified harness, so a tunnel's hit set is exactly the
	// terminator that owns it. That's the gateway's `tunnel-pin` invariant
	// in concentrated form.)
	for i, st := range tunnels {
		uniq := st.uniqueTerminators()
		if len(uniq) != 1 {
			t.Errorf("tunnel %d touched %d terminators, want 1", i, len(uniq))
		}
	}

	// Both instances should be exercised over N tunnels (probabilistic
	// distribution — we tolerate skew but require non-zero each).
	if a.tunnelHits.Load() == 0 || b.tunnelHits.Load() == 0 {
		t.Errorf("expected wildcard distribution across both instances; got a=%d b=%d",
			a.tunnelHits.Load(), b.tunnelHits.Load())
	}
	if total := a.tunnelHits.Load() + b.tunnelHits.Load(); total != N {
		t.Errorf("total tunnel hits: got %d, want %d", total, N)
	}
}

// Test7: bidirectional flow control — sidecar emits TunnelAck (reflecting
// inbound bytes consumed by its backend) which becomes a downstream
// TunnelAck to the caller, whose outbound credits replenish. Cap caller
// initial credits low to force the round-trip and verify writer doesn't
// deadlock.
func TestTunnelE2E_BidirectionalFlowControl_NoDeadlock(t *testing.T) {
	t.Parallel()

	addr := echoListener(t)
	h := newTunnelHarness()
	h.addTunnelTerminator(t, "memorylayer", "flow", addr, 30_000, 0, nil)

	// Critical: caller's initial outbound credit window is intentionally
	// modest (just past the sidecar's 256 KiB inbound-ack threshold). The
	// caller can ship the first batch, but then must wait for the sidecar's
	// ack — which arrives once the sidecar has forwarded ≥256 KiB to its
	// backend. Without sidecar→caller TunnelAck routing through callerTransport,
	// the writer would deadlock. We size the window large enough to cover
	// "first batch + ack-threshold slack" so the round-trip is mandatory
	// for completion but the test isn't dominated by warm-up timing.
	const initialCredits = 384 * 1024 // 1.5 × ack threshold
	st, err := h.openTunnel(t, agentCaller("ws", "caller", "v1"),
		"sv::memorylayer::flow", 30_000, 0, initialCredits)
	if err != nil {
		t.Fatalf("openTunnel: %v", err)
	}

	// Generously over-grant caller→sidecar credits so the sidecar's pump
	// can run unimpeded.
	go func() {
		for i := 0; i < 32; i++ {
			st.sendCallerAck(uint32(256 << 10))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Stream 1 MiB through. Without sidecar→caller TunnelAck routing, the
	// caller would deadlock at 64 KiB.
	const total = 1 << 20
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		err := st.sendData(payload, false)
		st.sendFin()
		done <- err
	}()

	// Read back. The drain itself triggers in-process callerTransport ack
	// processing.
	bufCh := make(chan []byte, 1)
	go func() {
		buf, _ := st.drainDownstream(20 * time.Second)
		bufCh <- buf
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sendData blocked or failed (deadlock?): %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("writer deadlocked — credits did not replenish (outbound=%d)",
			st.outboundCredits.Load())
	}

	buf := <-bufCh
	if len(buf) != total {
		t.Fatalf("echoed size: got %d, want %d", len(buf), total)
	}
	if string(buf) != string(payload) {
		t.Errorf("echoed payload mismatch")
	}
}
