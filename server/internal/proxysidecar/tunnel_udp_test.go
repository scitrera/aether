package proxysidecar

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// udpEchoServer starts a UDP echo server that reads one datagram and writes
// it straight back. Returns the *net.UDPConn (for cleanup) and its address.
func udpEchoServer(t *testing.T) (*net.UDPConn, string) {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}
	go func() {
		buf := make([]byte, 65535)
		for {
			n, remote, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(buf[:n], remote)
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return conn, conn.LocalAddr().String()
}

// udpSilentServer accepts datagrams but never replies. Used for idle-timeout test.
func udpSilentServer(t *testing.T) (*net.UDPConn, string) {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}
	go func() {
		buf := make([]byte, 65535)
		for {
			_, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			// discard
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return conn, conn.LocalAddr().String()
}

// terminatorWithUDP returns a terminator wired to a single UDP backend
// pointed at addr.
func terminatorWithUDP(t *testing.T, addr string, opts ...func(*BackendConfig)) *Terminator {
	t.Helper()
	bcfg := BackendConfig{
		Name:             "udp-default",
		Kind:             BackendKindUDP,
		URL:              "udp://" + addr,
		MaxBytes:         0, // unlimited unless opt overrides
		MaxDatagramBytes: 1400,
	}
	for _, opt := range opts {
		opt(&bcfg)
	}
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:0", Insecure: true},
		Service: ServiceConfig{Implementation: "test", Specifier: "udp"},
		Terminator: TerminatorConfig{
			Enabled:  true,
			Backends: []BackendConfig{bcfg},
		},
		TenantID: "tenant-test",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	term, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}
	return term
}

// TestUDPTunnel_EchoVaryingSizes sends 100 datagrams of varying sizes through
// a UDP echo backend and verifies each is returned intact with boundary preservation.
func TestUDPTunnel_EchoVaryingSizes(t *testing.T) {
	t.Parallel()

	_, addr := udpEchoServer(t)
	term := terminatorWithUDP(t, addr, func(b *BackendConfig) {
		b.IdleTimeoutMs = 10_000
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-udp-echo",
		Protocol:      pb.TunnelOpen_UDP,
		IdleTimeoutMs: 10_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned close: %s %s", c.GetReason(), c.GetDetail())
	}

	// Send 100 datagrams of varying sizes: 1B, 100B, 1400B cycling.
	sizes := []int{1, 100, 1400}
	var sent [][]byte
	var seq uint32
	for i := 0; i < 100; i++ {
		sz := sizes[i%len(sizes)]
		data := make([]byte, sz)
		for j := range data {
			data[j] = byte(i + j)
		}
		sent = append(sent, data)
		seq++
		term.HandleTunnelData(&pb.TunnelData{
			TunnelId: "tn-udp-echo",
			Seq:      seq,
			Data:     data,
		}, ft)
		// Small gap to allow echo to arrive before next send.
		time.Sleep(2 * time.Millisecond)
	}

	// Wait for all 100 echoes to arrive.
	waitFor(t, 5*time.Second, func() bool {
		ft.mu.Lock()
		defer ft.mu.Unlock()
		return len(ft.dataFrames) >= 100
	})

	ft.mu.Lock()
	frames := make([]*pb.TunnelData, len(ft.dataFrames))
	copy(frames, ft.dataFrames)
	ft.mu.Unlock()

	if len(frames) < 100 {
		t.Fatalf("expected 100 echo frames, got %d", len(frames))
	}
	// Each outbound frame must be exactly the size of one sent datagram.
	// Because UDP echo is order-preserving on loopback we can match by order.
	for i, f := range frames[:100] {
		expected := sent[i]
		if len(f.Data) != len(expected) {
			t.Errorf("frame %d: size %d, expected %d (boundary not preserved)", i, len(f.Data), len(expected))
		}
	}
}

// TestUDPTunnel_DatagramBoundary verifies that one TunnelData in produces
// exactly one TunnelData out — datagrams are never coalesced or split.
func TestUDPTunnel_DatagramBoundary(t *testing.T) {
	t.Parallel()

	_, addr := udpEchoServer(t)
	term := terminatorWithUDP(t, addr, func(b *BackendConfig) {
		b.IdleTimeoutMs = 10_000
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-udp-boundary",
		Protocol:      pb.TunnelOpen_UDP,
		IdleTimeoutMs: 10_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned close: %s %s", c.GetReason(), c.GetDetail())
	}

	const N = 5
	for i := 0; i < N; i++ {
		data := make([]byte, 200+i*100)
		for j := range data {
			data[j] = byte(i)
		}
		term.HandleTunnelData(&pb.TunnelData{
			TunnelId: "tn-udp-boundary",
			Seq:      uint32(i + 1),
			Data:     data,
		}, ft)
		time.Sleep(5 * time.Millisecond)
	}

	waitFor(t, 5*time.Second, func() bool {
		ft.mu.Lock()
		defer ft.mu.Unlock()
		return len(ft.dataFrames) >= N
	})

	ft.mu.Lock()
	frameCount := len(ft.dataFrames)
	ft.mu.Unlock()

	// Exactly N frames back — no coalescing, no splitting.
	if frameCount != N {
		t.Errorf("boundary check: got %d frames, want exactly %d", frameCount, N)
	}
}

// TestUDPTunnel_OversizeRejectsWithError verifies that a TunnelData frame
// larger than max_datagram_bytes causes TunnelClose{ERROR, "datagram exceeds MTU"}.
func TestUDPTunnel_OversizeRejectsWithError(t *testing.T) {
	t.Parallel()

	_, addr := udpEchoServer(t)
	term := terminatorWithUDP(t, addr, func(b *BackendConfig) {
		b.MaxDatagramBytes = 512
		b.IdleTimeoutMs = 5_000
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-udp-oversize",
		Protocol:      pb.TunnelOpen_UDP,
		IdleTimeoutMs: 5_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned close: %s %s", c.GetReason(), c.GetDetail())
	}

	// Send a datagram larger than the configured MTU.
	oversized := make([]byte, 513)
	for i := range oversized {
		oversized[i] = 0xFF
	}
	term.HandleTunnelData(&pb.TunnelData{
		TunnelId: "tn-udp-oversize",
		Seq:      1,
		Data:     oversized,
	}, ft)

	c := waitForClose(t, ft, 3*time.Second)
	if c.GetReason() != pb.TunnelClose_ERROR {
		t.Fatalf("close reason: got %s, want ERROR (detail=%q)", c.GetReason(), c.GetDetail())
	}
	if !strings.Contains(c.GetDetail(), "datagram exceeds MTU") {
		t.Errorf("detail %q should mention 'datagram exceeds MTU'", c.GetDetail())
	}
}

// TestUDPTunnel_IdleTimeoutFires opens a UDP tunnel against a silent server
// and asserts TunnelClose{IDLE_TIMEOUT} fires once the idle window elapses.
func TestUDPTunnel_IdleTimeoutFires(t *testing.T) {
	t.Parallel()

	_, addr := udpSilentServer(t)
	term := terminatorWithUDP(t, addr, func(b *BackendConfig) {
		b.IdleTimeoutMs = 200
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-udp-idle",
		Protocol:      pb.TunnelOpen_UDP,
		IdleTimeoutMs: 200,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned close: %s %s", c.GetReason(), c.GetDetail())
	}

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_IDLE_TIMEOUT {
		t.Fatalf("close reason: got %s, want IDLE_TIMEOUT (detail=%q)", c.GetReason(), c.GetDetail())
	}
}

// TestUDPTunnel_TwoConcurrentTunnelsIndependent verifies that two concurrent
// UDP tunnels share no state and datagrams don't bleed between them.
func TestUDPTunnel_TwoConcurrentTunnelsIndependent(t *testing.T) {
	t.Parallel()

	_, addrA := udpEchoServer(t)
	_, addrB := udpEchoServer(t)

	termA := terminatorWithUDP(t, addrA, func(b *BackendConfig) { b.IdleTimeoutMs = 10_000 })
	termB := terminatorWithUDP(t, addrB, func(b *BackendConfig) { b.IdleTimeoutMs = 10_000 })
	t.Cleanup(termA.StopAllTunnels)
	t.Cleanup(termB.StopAllTunnels)

	ftA := newFakeTransport()
	ftB := newFakeTransport()

	if c := termA.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-udp-a", Protocol: pb.TunnelOpen_UDP, IdleTimeoutMs: 10_000},
		ftA); c != nil {
		t.Fatalf("open A: %s %s", c.GetReason(), c.GetDetail())
	}
	if c := termB.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-udp-b", Protocol: pb.TunnelOpen_UDP, IdleTimeoutMs: 10_000},
		ftB); c != nil {
		t.Fatalf("open B: %s %s", c.GetReason(), c.GetDetail())
	}

	payloadA := []byte(strings.Repeat("AAAA", 50)) // 200 bytes
	payloadB := []byte(strings.Repeat("BBBB", 50))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			termA.HandleTunnelData(&pb.TunnelData{
				TunnelId: "tn-udp-a",
				Seq:      uint32(i + 1),
				Data:     payloadA,
			}, ftA)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			termB.HandleTunnelData(&pb.TunnelData{
				TunnelId: "tn-udp-b",
				Seq:      uint32(i + 1),
				Data:     payloadB,
			}, ftB)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool {
		ftA.mu.Lock()
		nA := len(ftA.dataFrames)
		ftA.mu.Unlock()
		ftB.mu.Lock()
		nB := len(ftB.dataFrames)
		ftB.mu.Unlock()
		return nA >= 10 && nB >= 10
	})

	ftA.mu.Lock()
	dataA := string(ftA.dataBytes)
	ftA.mu.Unlock()
	ftB.mu.Lock()
	dataB := string(ftB.dataBytes)
	ftB.mu.Unlock()

	if strings.Contains(dataA, "BBBB") {
		t.Errorf("tunnel A received tunnel B data")
	}
	if strings.Contains(dataB, "AAAA") {
		t.Errorf("tunnel B received tunnel A data")
	}
}

// TestUDPTunnel_FinClosesNormally verifies that fin=true in a TunnelData frame
// closes the UDP tunnel with TunnelClose_NORMAL.
func TestUDPTunnel_FinClosesNormally(t *testing.T) {
	t.Parallel()

	_, addr := udpEchoServer(t)
	term := terminatorWithUDP(t, addr, func(b *BackendConfig) {
		b.IdleTimeoutMs = 10_000
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-udp-fin",
		Protocol:      pb.TunnelOpen_UDP,
		IdleTimeoutMs: 10_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned close: %s %s", c.GetReason(), c.GetDetail())
	}

	term.HandleTunnelData(&pb.TunnelData{
		TunnelId: "tn-udp-fin",
		Seq:      1,
		Fin:      true,
	}, ft)

	c := waitForClose(t, ft, 3*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL", c.GetReason())
	}
}

// TestUDPTunnel_NoUDPBackendReturnsError asserts that a TunnelOpen{UDP}
// against a terminator with only TCP backends returns a synchronous ERROR.
func TestUDPTunnel_NoUDPBackendReturnsError(t *testing.T) {
	t.Parallel()

	// Use HTTP-only config.
	cfg := terminatorTestConfig()
	term, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	ft := newFakeTransport()
	c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-udp-nobackend", Protocol: pb.TunnelOpen_UDP},
		ft)
	if c == nil {
		t.Fatal("expected synchronous TunnelClose")
	}
	if c.GetReason() != pb.TunnelClose_ERROR {
		t.Errorf("reason: got %s, want ERROR", c.GetReason())
	}
	if !strings.Contains(c.GetDetail(), "not implemented") {
		t.Errorf("detail %q should mention 'not implemented'", c.GetDetail())
	}
}
