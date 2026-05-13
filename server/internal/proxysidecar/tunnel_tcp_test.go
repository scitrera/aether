package proxysidecar

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// fakeTransport captures every frame the tunnel produces so tests can
// inspect ordering, totals, and final close reasons. It also exposes a
// gateAcks switch that simulates a slow consumer for the flow-control test.
type fakeTransport struct {
	mu         sync.Mutex
	dataFrames []*pb.TunnelData
	dataBytes  []byte
	ackFrames  []*pb.TunnelAck
	closeMsg   *pb.TunnelClose
	closed     atomic.Bool
	httpResps  []*pb.ProxyHttpResponse
	httpChunks []*pb.ProxyHttpBodyChunk

	// blockData causes SendTunnelData to block until unblockData is called,
	// simulating a slow caller for flow-control validation.
	blockData chan struct{}
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{}
}

func (f *fakeTransport) SendTunnelData(d *pb.TunnelData) error {
	if ch := f.blockData; ch != nil {
		<-ch
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dataFrames = append(f.dataFrames, d)
	f.dataBytes = append(f.dataBytes, d.GetData()...)
	return nil
}

func (f *fakeTransport) SendTunnelAck(a *pb.TunnelAck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackFrames = append(f.ackFrames, a)
	return nil
}

func (f *fakeTransport) SendTunnelClose(c *pb.TunnelClose) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closeMsg == nil {
		f.closeMsg = c
	}
	f.closed.Store(true)
	return nil
}

func (f *fakeTransport) SendProxyHttpResponse(r *pb.ProxyHttpResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.httpResps = append(f.httpResps, r)
	return nil
}

func (f *fakeTransport) SendProxyHttpBodyChunk(c *pb.ProxyHttpBodyChunk) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.httpChunks = append(f.httpChunks, c)
	return nil
}

func (f *fakeTransport) snapshot() (frames int, total int, lastFin bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	frames = len(f.dataFrames)
	total = len(f.dataBytes)
	if frames > 0 {
		lastFin = f.dataFrames[frames-1].GetFin()
	}
	return
}

// echoServer is a tiny TCP listener that reads bytes off accepted connections
// and writes them straight back. It returns the listener, an explicit close
// helper, and the address to dial.
func echoServer(t *testing.T) (net.Listener, string) {
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
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln, ln.Addr().String()
}

// halfCloseEchoServer reads everything the caller sends, then echoes the
// concatenated bytes back exactly once and shuts down its write half. Used
// to validate FIN propagation from socket EOF back to the caller.
func halfCloseEchoServer(t *testing.T) (net.Listener, string) {
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
				data, _ := io.ReadAll(c)
				_, _ = c.Write(data)
				if cw, ok := c.(*net.TCPConn); ok {
					_ = cw.CloseWrite()
				}
			}(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln, ln.Addr().String()
}

// silentServer accepts a connection but never reads or writes. Used to
// validate idle-timeout enforcement.
func silentServer(t *testing.T) (net.Listener, string) {
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
			// Hold the conn open without doing IO; close on test exit.
			t.Cleanup(func() { c.Close() })
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln, ln.Addr().String()
}

// firehoseServer accepts a connection and writes a never-ending stream of
// 0xAA bytes until the conn is closed. Used to validate that the sidecar
// stops reading from the socket when outbound credits are exhausted instead
// of buffering unbounded data in memory.
func firehoseServer(t *testing.T) (net.Listener, string) {
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
				buf := make([]byte, 4096)
				for i := range buf {
					buf[i] = 0xAA
				}
				for {
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln, ln.Addr().String()
}

// terminatorWithTCP returns a terminator wired to a single TCP backend
// pointed at addr.
func terminatorWithTCP(t *testing.T, addr string, opts ...func(*BackendConfig)) *Terminator {
	t.Helper()
	bcfg := BackendConfig{
		Name:     "tcp-default",
		Kind:     BackendKindTCP,
		URL:      "tcp://" + addr,
		MaxBytes: 1 << 30, // disable the cap unless an opt overrides
	}
	for _, opt := range opts {
		opt(&bcfg)
	}
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:0", Insecure: true},
		Service: ServiceConfig{Implementation: "memorylayer", Specifier: "test"},
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

// pumpInbound delivers data to the tunnel as a sequence of TunnelData frames.
// chunkSize controls inbound framing on the simulated caller side.
func pumpInbound(term *Terminator, transport tunnelTransport, tunnelID string, payload []byte, chunkSize int, fin bool) {
	if chunkSize <= 0 {
		chunkSize = tcpFrameMaxBytes
	}
	var seq uint32
	for off := 0; off < len(payload); off += chunkSize {
		end := off + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		seq++
		term.HandleTunnelData(&pb.TunnelData{
			TunnelId: tunnelID,
			Seq:      seq,
			Data:     payload[off:end],
		}, transport)
	}
	if fin {
		seq++
		term.HandleTunnelData(&pb.TunnelData{TunnelId: tunnelID, Seq: seq, Fin: true}, transport)
	}
}

// waitForClose polls the fake transport until it observes a TunnelClose.
func waitForClose(t *testing.T, ft *fakeTransport, timeout time.Duration) *pb.TunnelClose {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ft.closed.Load() {
			ft.mu.Lock()
			c := ft.closeMsg
			ft.mu.Unlock()
			return c
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for TunnelClose")
	return nil
}

// waitFor polls until cond returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// TestTCPTunnel_EchoTenMB pushes 10 MiB of random bytes through an echo
// backend and validates the bytes returned to the caller are byte-identical.
// Also exercises ack emission and the multi-frame outbound path.
func TestTCPTunnel_EchoTenMB(t *testing.T) {
	t.Parallel()

	_, addr := echoServer(t)
	term := terminatorWithTCP(t, addr)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-echo",
		Protocol:      pb.TunnelOpen_TCP,
		IdleTimeoutMs: 30_000,
		MaxBytes:      0,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned synchronous close: %s %s", c.GetReason(), c.GetDetail())
	}

	const total = 10 * 1024 * 1024
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	// Generously over-grant credits so flow-control isn't the limiter here.
	go func() {
		for i := 0; i < 64; i++ {
			term.HandleTunnelAck(&pb.TunnelAck{
				TunnelId: "tn-echo",
				Credits:  uint32(tcpFrameMaxBytes),
			})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	pumpInbound(term, ft, "tn-echo", payload, 64*1024, true)

	// Expect a TunnelClose{NORMAL} once both halves drain.
	c := waitForClose(t, ft, 30*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.dataBytes) != total {
		t.Fatalf("echoed bytes: got %d, want %d", len(ft.dataBytes), total)
	}
	for i := range payload {
		if ft.dataBytes[i] != payload[i] {
			t.Fatalf("byte %d differs: got %x want %x", i, ft.dataBytes[i], payload[i])
		}
	}
	// Several frames should have arrived since payload >> 256 KiB.
	if len(ft.dataFrames) < 4 {
		t.Errorf("expected multi-frame outbound; got %d frames", len(ft.dataFrames))
	}
	// At least one ack should have fired.
	if len(ft.ackFrames) == 0 {
		t.Errorf("expected at least one TunnelAck for inbound bytes")
	}
}

// TestTCPTunnel_HalfCloseFromCaller asserts that TunnelData{fin:true} closes
// the TCP write half cleanly: the half-close-echo backend writes back the
// concatenated input then closes its write side, the sidecar emits a fin
// frame, and a TunnelClose{NORMAL} is observed.
func TestTCPTunnel_HalfCloseFromCaller(t *testing.T) {
	t.Parallel()

	_, addr := halfCloseEchoServer(t)
	term := terminatorWithTCP(t, addr)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-half",
		Protocol:      pb.TunnelOpen_TCP,
		IdleTimeoutMs: 30_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen returned close: %s %s", c.GetReason(), c.GetDetail())
	}

	// Liberal credits so flow control doesn't gate this test.
	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-half", Credits: 16 * tcpFrameMaxBytes})

	payload := []byte("hello half-close world")
	pumpInbound(term, ft, "tn-half", payload, 4, true)

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if string(ft.dataBytes) != string(payload) {
		t.Fatalf("echoed payload: got %q, want %q", string(ft.dataBytes), string(payload))
	}
	// The last data frame should carry fin=true so the caller knows our
	// outbound half is closed.
	if n := len(ft.dataFrames); n == 0 || !ft.dataFrames[n-1].GetFin() {
		t.Errorf("expected last TunnelData frame to carry fin=true, frames=%d", n)
	}
}

// TestTCPTunnel_TwoConcurrentTunnelsIndependent asserts that data written
// into one tunnel does not bleed into another. Each tunnel runs its own pump
// against its own echo backend.
func TestTCPTunnel_TwoConcurrentTunnelsIndependent(t *testing.T) {
	t.Parallel()

	_, addr := echoServer(t)
	term := terminatorWithTCP(t, addr)
	t.Cleanup(term.StopAllTunnels)

	ftA := newFakeTransport()
	ftB := newFakeTransport()

	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-a", Protocol: pb.TunnelOpen_TCP, IdleTimeoutMs: 30_000},
		ftA); c != nil {
		t.Fatalf("open A: %s %s", c.GetReason(), c.GetDetail())
	}
	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-b", Protocol: pb.TunnelOpen_TCP, IdleTimeoutMs: 30_000},
		ftB); c != nil {
		t.Fatalf("open B: %s %s", c.GetReason(), c.GetDetail())
	}

	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-a", Credits: 16 * tcpFrameMaxBytes})
	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-b", Credits: 16 * tcpFrameMaxBytes})

	payloadA := []byte(strings.Repeat("AAAA", 4096)) // ~16 KiB
	payloadB := []byte(strings.Repeat("BBBB", 4096))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		pumpInbound(term, ftA, "tn-a", payloadA, 1024, true)
	}()
	go func() {
		defer wg.Done()
		pumpInbound(term, ftB, "tn-b", payloadB, 1024, true)
	}()
	wg.Wait()

	cA := waitForClose(t, ftA, 5*time.Second)
	cB := waitForClose(t, ftB, 5*time.Second)
	if cA.GetReason() != pb.TunnelClose_NORMAL {
		t.Errorf("A close: got %s", cA.GetReason())
	}
	if cB.GetReason() != pb.TunnelClose_NORMAL {
		t.Errorf("B close: got %s", cB.GetReason())
	}

	ftA.mu.Lock()
	dataA := append([]byte(nil), ftA.dataBytes...)
	ftA.mu.Unlock()
	ftB.mu.Lock()
	dataB := append([]byte(nil), ftB.dataBytes...)
	ftB.mu.Unlock()

	if string(dataA) != string(payloadA) {
		t.Errorf("tunnel A echoed wrong bytes (len=%d, expected %d)", len(dataA), len(payloadA))
	}
	if string(dataB) != string(payloadB) {
		t.Errorf("tunnel B echoed wrong bytes (len=%d, expected %d)", len(dataB), len(payloadB))
	}
	// Cross-talk check: neither side should ever observe the other's bytes.
	if strings.Contains(string(dataA), "BBBB") {
		t.Errorf("tunnel A saw tunnel B bytes")
	}
	if strings.Contains(string(dataB), "AAAA") {
		t.Errorf("tunnel B saw tunnel A bytes")
	}
}

// TestTCPTunnel_IdleTimeoutFires opens a tunnel against a silent backend
// (no traffic either direction) and asserts TunnelClose{IDLE_TIMEOUT}
// fires promptly once the idle window elapses.
func TestTCPTunnel_IdleTimeoutFires(t *testing.T) {
	t.Parallel()

	_, addr := silentServer(t)
	term := terminatorWithTCP(t, addr, func(b *BackendConfig) {
		b.IdleTimeoutMs = 200 // 200ms
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-idle", Protocol: pb.TunnelOpen_TCP, IdleTimeoutMs: 200},
		ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_IDLE_TIMEOUT {
		t.Fatalf("close reason: got %s, want IDLE_TIMEOUT (detail=%q)", c.GetReason(), c.GetDetail())
	}
}

// TestTCPTunnel_MaxBytesFiresQuotaClose verifies that pushing data over
// max_bytes triggers a TunnelClose{QUOTA}.
func TestTCPTunnel_MaxBytesFiresQuotaClose(t *testing.T) {
	t.Parallel()

	_, addr := echoServer(t)
	term := terminatorWithTCP(t, addr, func(b *BackendConfig) {
		b.MaxBytes = 1024 // 1 KiB cap
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-quota", Protocol: pb.TunnelOpen_TCP, IdleTimeoutMs: 30_000},
		ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-quota", Credits: 16 * tcpFrameMaxBytes})

	// 4 KiB > 1 KiB cap. We don't FIN here — the quota close should fire.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	pumpInbound(term, ft, "tn-quota", payload, 256, false)

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_QUOTA {
		t.Fatalf("close reason: got %s, want QUOTA (detail=%q)", c.GetReason(), c.GetDetail())
	}
}

// TestTCPTunnel_FlowControlGatesSocketReads simulates a slow caller by
// granting only a modest credit window against a firehose backend, then
// asserts the sidecar respects the credit limit (it does not race ahead and
// flood the transport with frames). Outbound bytes shipped should not
// materially exceed the granted credit window plus initial credits.
func TestTCPTunnel_FlowControlGatesSocketReads(t *testing.T) {
	t.Parallel()

	_, addr := firehoseServer(t)
	term := terminatorWithTCP(t, addr)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-fc", Protocol: pb.TunnelOpen_TCP, IdleTimeoutMs: 30_000},
		ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	// The default initial credit window is tcpInitialOutboundCredits (1 MiB).
	// We grant no further credits and let the firehose run for a while; the
	// sidecar should plateau at roughly the initial window and stop reading.
	deadline := time.Now().Add(750 * time.Millisecond)
	var lastBytes int
	for time.Now().Before(deadline) {
		_, total, _ := ft.snapshot()
		lastBytes = total
		time.Sleep(50 * time.Millisecond)
	}

	maxAllowed := tcpInitialOutboundCredits + 4*tcpFrameMaxBytes // generous slack
	if lastBytes > maxAllowed {
		t.Fatalf("flow control breached: shipped %d bytes, expected ≤ %d", lastBytes, maxAllowed)
	}
	if lastBytes == 0 {
		t.Fatalf("expected some bytes to ship under initial credit window, got 0")
	}

	// Now release more credit and confirm progress resumes.
	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-fc", Credits: 512 * 1024})
	waitFor(t, 2*time.Second, func() bool {
		_, total, _ := ft.snapshot()
		return total > lastBytes
	})

	// Cleanly close to release the firehose conn.
	term.HandleTunnelClose(&pb.TunnelClose{TunnelId: "tn-fc", Reason: pb.TunnelClose_NORMAL})
	waitForClose(t, ft, 5*time.Second)
}

// TestTCPTunnel_NoTCPBackendReturnsError asserts that a TunnelOpen against a
// terminator with only HTTP backends returns ERROR with a "not implemented"
// detail.
func TestTCPTunnel_NoTCPBackendReturnsError(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig() // HTTP-only
	term, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	ft := newFakeTransport()
	c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-no-backend", Protocol: pb.TunnelOpen_TCP},
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

// TestTCPTunnel_DialFailureClosesWithError asserts that a backend that
// refuses connections produces a synchronous TunnelClose{ERROR}.
func TestTCPTunnel_DialFailureClosesWithError(t *testing.T) {
	t.Parallel()

	// Bind and immediately close to obtain a guaranteed-closed port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	term := terminatorWithTCP(t, addr)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-dial", Protocol: pb.TunnelOpen_TCP},
		ft)
	if c == nil {
		t.Fatal("expected synchronous TunnelClose for dial failure")
	}
	if c.GetReason() != pb.TunnelClose_ERROR {
		t.Errorf("reason: got %s, want ERROR", c.GetReason())
	}
}

// TestResolveTCPAddress_AllowList exercises remote_hint matching paths.
func TestResolveTCPAddress_AllowList(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		cfg     BackendConfig
		hint    string
		want    string
		wantErr bool
	}{
		{
			name: "default url no hint",
			cfg:  BackendConfig{URL: "tcp://127.0.0.1:5901"},
			hint: "",
			want: "127.0.0.1:5901",
		},
		{
			name: "hint with no allow list falls back to url",
			cfg:  BackendConfig{URL: "tcp://127.0.0.1:5901"},
			hint: "any:thing",
			want: "127.0.0.1:5901",
		},
		{
			name: "allow list permits hint",
			cfg:  BackendConfig{URL: "tcp://127.0.0.1:5901", AllowRemoteHints: []string{"vnc:*"}},
			hint: "vnc:1.2.3.4:5901",
			want: "vnc:1.2.3.4:5901",
		},
		{
			name:    "allow list rejects hint",
			cfg:     BackendConfig{URL: "tcp://127.0.0.1:5901", AllowRemoteHints: []string{"vnc:*"}},
			hint:    "ssh:1.2.3.4:22",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveTCPAddress(tc.cfg, tc.hint)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
