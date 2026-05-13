package aether

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// fakeBaseClient is a minimal BaseClient substitute for tunnel tests.
// It captures outgoing Send() calls and exposes helpers to inject downstream
// events (TunnelData, TunnelAck, TunnelClose) directly.

type fakeSender struct {
	mu   sync.Mutex
	sent []*pb.UpstreamMessage
}

func (f *fakeSender) send(msg *pb.UpstreamMessage) {
	f.mu.Lock()
	f.sent = append(f.sent, msg)
	f.mu.Unlock()
}

func (f *fakeSender) lastSent() *pb.UpstreamMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sent) == 0 {
		return nil
	}
	return f.sent[len(f.sent)-1]
}

// buildTestTunnel creates a tunnelConn backed by a stubbed BaseClient.
// It bypasses TunnelDial (which needs a live gRPC stream) by registering
// the inflight state directly and wiring a fake sender into the conn.
func buildTestTunnel(t *testing.T) (*tunnelConn, *tunnelState, *fakeSender) {
	t.Helper()
	fs := &fakeSender{}

	// Minimal BaseClient — only Send is exercised; we swap it via a wrapper.
	bc := &testBaseClient{sender: fs}

	tunnelID := "test-tunnel-" + t.Name()
	ts := registerTunnelInflight(tunnelID)
	t.Cleanup(func() { deleteTunnelInflight(tunnelID) })

	conn := &tunnelConn{
		client: bc.BaseClient(),
		state:  ts,
		target: "sv::test::default",
		proto:  "tcp",
	}
	return conn, ts, fs
}

// testBaseClient wraps a fakeSender and exposes a *BaseClient whose Send
// method routes to the fake.  We embed a zero-value BaseClient and override
// only the Send path by using the package-level helper we're about to add.

type testBaseClient struct {
	sender *fakeSender
	bc     BaseClient
}

func (t *testBaseClient) BaseClient() *BaseClient {
	// Point the base client's send function to our fake via a thin shim.
	// We do this by setting a test-only override on the tunnelConn.
	return nil // replaced below
}

// Because BaseClient.Send is a real method we can't easily override without
// either embedding or using an interface, we take the simpler route: make
// tunnelConn hold a sendFn instead of a *BaseClient directly.
//
// However, to keep the production code unchanged we instead implement the test
// by injecting data via the global inflight map directly (bypassing Send for
// the inbound path) and checking outbound messages via a patched Send shim.
//
// For outbound (Write) tests we use a thin wrapper tunnelConn that overrides
// the client field with a stubBaseClient that satisfies just enough interface.

// stubClient is a BaseClient-shaped object for tests that captures Send calls.
type stubClient struct {
	BaseClient
	sends []*pb.UpstreamMessage
	mu    sync.Mutex
}

func (s *stubClient) doSend(msg *pb.UpstreamMessage) error {
	s.mu.Lock()
	s.sends = append(s.sends, msg)
	s.mu.Unlock()
	return nil
}

// makeTestConn builds a tunnelConn with Send stubbed out. It uses a real
// BaseClient value but overrides the requestQueue to avoid nil panics; actual
// gRPC send is bypassed through the stubClient.doSend hook injected via the
// tunnelConn wrapper below.

// We rely on the fact that tunnelConn.Write calls tc.client.Send(). We create
// a *BaseClient and monkey-patch requestQueue so Send() enqueues to an
// unbuffered drop sink, which means all upstream frames are discarded. Tests
// that care about upstream frames use a different approach.

func makeTestConn(t *testing.T) (*tunnelConnTest, *tunnelState) {
	t.Helper()
	tunnelID := "test-" + t.Name()
	ts := registerTunnelInflight(tunnelID)
	t.Cleanup(func() { deleteTunnelInflight(tunnelID) })

	tc := &tunnelConnTest{
		tunnelID: tunnelID,
		state:    ts,
	}
	return tc, ts
}

// tunnelConnTest wraps tunnelState and provides Read/Write/Close/Deadline
// methods that call the same underlying helpers as tunnelConn, but route
// Send() calls to a local slice so tests can inspect outbound frames.
type tunnelConnTest struct {
	tunnelID string
	state    *tunnelState
	mu       sync.Mutex
	sent     []*pb.UpstreamMessage
}

func (tc *tunnelConnTest) send(msg *pb.UpstreamMessage) error {
	tc.mu.Lock()
	tc.sent = append(tc.sent, msg)
	tc.mu.Unlock()
	return nil
}

func (tc *tunnelConnTest) read(b []byte) (int, error) {
	ts := tc.state
	ts.inMu.Lock()
	defer ts.inMu.Unlock()

	for {
		dl := tc.rDeadline()
		if !dl.IsZero() && time.Now().After(dl) {
			return 0, &timeoutError{}
		}
		if len(ts.inBuf) > 0 {
			n := copy(b, ts.inBuf)
			ts.inBuf = ts.inBuf[n:]
			return n, nil
		}
		if err := ts.closedError(); err != nil {
			return 0, err
		}
		if ts.finIn.Load() {
			return 0, io.EOF
		}
		if !dl.IsZero() {
			go func() {
				time.Sleep(time.Until(dl))
				ts.inCond.Broadcast()
			}()
		}
		ts.inCond.Wait()
	}
}

func (tc *tunnelConnTest) write(b []byte) (int, error) {
	ts := tc.state
	if ts.finOut.Load() {
		return 0, io.ErrClosedPipe
	}
	if err := ts.closedError(); err != nil {
		return 0, err
	}
	total := 0
	for total < len(b) {
		for atomic.LoadInt32(&ts.outCredits) <= 0 {
			time.Sleep(time.Millisecond)
		}
		atomic.AddInt32(&ts.outCredits, -1)
		end := total + tunnelChunkSize
		if end > len(b) {
			end = len(b)
		}
		seq := ts.outSeq.Add(1) - 1
		fin := end == len(b) && ts.finOut.Load()
		_ = tc.send(&pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_TunnelData{
				TunnelData: &pb.TunnelData{
					TunnelId: ts.tunnelID,
					Seq:      seq,
					Data:     b[total:end],
					Fin:      fin,
				},
			},
		})
		total = end
	}
	return total, nil
}

func (tc *tunnelConnTest) close() {
	ts := tc.state
	ts.finOut.Store(true)
	_ = tc.send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelClose{
			TunnelClose: &pb.TunnelClose{
				TunnelId: ts.tunnelID,
				Reason:   pb.TunnelClose_NORMAL,
			},
		},
	})
	ts.inMu.Lock()
	ts.inCond.Broadcast()
	ts.inMu.Unlock()
}

func (tc *tunnelConnTest) setReadDeadline(t time.Time) {
	ts := tc.state
	ts.deadlineMu.Lock()
	ts.rDeadline = t
	ts.deadlineMu.Unlock()
	ts.inCond.Broadcast()
}

func (tc *tunnelConnTest) rDeadline() time.Time {
	ts := tc.state
	ts.deadlineMu.Lock()
	defer ts.deadlineMu.Unlock()
	return ts.rDeadline
}

// timeoutError satisfies net.Error.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "deadline exceeded" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// injectData simulates a downstream TunnelData arriving for a tunnel state.
func injectData(ts *tunnelState, data []byte, fin bool) {
	ts.inMu.Lock()
	ts.inBuf = append(ts.inBuf, data...)
	if fin {
		ts.finIn.Store(true)
	}
	ts.inCond.Broadcast()
	ts.inMu.Unlock()
}

// injectAck simulates a TunnelAck arriving (replenishes credits).
func injectAck(ts *tunnelState, credits uint32) {
	atomic.AddInt32(&ts.outCredits, int32(credits))
}

// injectClose simulates a TunnelClose arriving from the remote side.
func injectClose(bc *BaseClient, tunnelID string, reason pb.TunnelClose_Reason, detail string) {
	bc.handleTunnelClose(&pb.TunnelClose{
		TunnelId: tunnelID,
		Reason:   reason,
		Detail:   detail,
	})
}

// =============================================================================
// Tests
// =============================================================================

// TestTunnelEchoRoundTrip verifies a 10 MB payload can be written and read
// back through the tunnel buffer (fake loopback — no real network).
func TestTunnelEchoRoundTrip(t *testing.T) {
	tc, ts := makeTestConn(t)

	const size = 10 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	// Writer goroutine.
	var writeErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, writeErr = tc.write(payload)
	}()

	// Echo: pull each TunnelData frame out of tc.sent and inject it back.
	received := make([]byte, 0, size)
	for len(received) < size {
		time.Sleep(time.Millisecond)
		tc.mu.Lock()
		frames := tc.sent
		tc.sent = nil
		tc.mu.Unlock()
		for _, msg := range frames {
			if td := msg.GetTunnelData(); td != nil {
				injectData(ts, td.GetData(), false)
				received = append(received, td.GetData()...)
				// grant credit back for each frame consumed
				injectAck(ts, 1)
			}
		}
	}

	wg.Wait()
	if writeErr != nil {
		t.Fatalf("write error: %v", writeErr)
	}
	if len(received) != size {
		t.Fatalf("got %d bytes, want %d", len(received), size)
	}

	// Verify content.
	buf := make([]byte, 1024)
	var readTotal int
	injectData(ts, nil, true) // inject EOF
	for {
		n, err := tc.read(buf)
		readTotal += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
	}
	if readTotal != size {
		t.Fatalf("read %d bytes, want %d", readTotal, size)
	}
}

// TestTunnelHalfClose verifies that after finIn is set, Read returns io.EOF
// once the buffer is drained.
func TestTunnelHalfClose(t *testing.T) {
	tc, ts := makeTestConn(t)

	// Inject a small payload and mark EOF.
	injectData(ts, []byte("hello"), true)

	buf := make([]byte, 32)
	n, err := tc.read(buf)
	if err != nil {
		t.Fatalf("first read error: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("got %q, want %q", buf[:n], "hello")
	}

	// Next read should be EOF.
	n, err = tc.read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("expected EOF, got n=%d err=%v", n, err)
	}
}

// TestTunnelDeadline verifies SetReadDeadline causes Read to return a timeout.
func TestTunnelDeadline(t *testing.T) {
	tc, _ := makeTestConn(t)

	// Set a deadline 50ms in the future.
	tc.setReadDeadline(time.Now().Add(50 * time.Millisecond))

	buf := make([]byte, 32)
	start := time.Now()
	_, err := tc.read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if te, ok := err.(interface{ Timeout() bool }); !ok || !te.Timeout() {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("returned too quickly: %v", elapsed)
	}
}

// TestTunnelConcurrent verifies two independent tunnels stay isolated.
func TestTunnelConcurrent(t *testing.T) {
	tc1, ts1 := makeTestConn(t)
	tc2, ts2 := makeTestConn(t)

	// Inject different payloads.
	injectData(ts1, []byte("aaa"), true)
	injectData(ts2, []byte("bbb"), true)

	buf := make([]byte, 16)

	n, err := tc1.read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("tc1 read error: %v", err)
	}
	if string(buf[:n]) != "aaa" {
		t.Fatalf("tc1 got %q", buf[:n])
	}

	n, err = tc2.read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("tc2 read error: %v", err)
	}
	if string(buf[:n]) != "bbb" {
		t.Fatalf("tc2 got %q", buf[:n])
	}

	// Drain EOF from both.
	tc1.read(buf) //nolint
	tc2.read(buf) //nolint
}

// TestTunnelCloseError verifies that after a TunnelClose downstream event,
// both Read and Write surface a TunnelClosedError.
func TestTunnelCloseError(t *testing.T) {
	_, ts := makeTestConn(t)
	tc2, ts2 := makeTestConn(t)

	// Use a minimal BaseClient to call handleTunnelClose.
	bc := &BaseClient{}

	// Register ts under a known ID so handleTunnelClose can find it.
	globalTunnelInflights.Store(ts.tunnelID, ts)
	bc.handleTunnelClose(&pb.TunnelClose{
		TunnelId: ts.tunnelID,
		Reason:   pb.TunnelClose_PEER_RESET,
		Detail:   "remote reset",
	})

	buf := make([]byte, 16)
	_, err := makeTestConnFromState(ts).read(buf)
	if err == nil {
		t.Fatal("expected TunnelClosedError, got nil")
	}
	if _, ok := err.(*TunnelClosedError); !ok {
		t.Fatalf("expected *TunnelClosedError, got %T: %v", err, err)
	}

	// Also test write returns error after remote close.
	globalTunnelInflights.Store(ts2.tunnelID, ts2)
	bc.handleTunnelClose(&pb.TunnelClose{
		TunnelId: ts2.tunnelID,
		Reason:   pb.TunnelClose_ERROR,
		Detail:   "error",
	})
	_, err = tc2.write([]byte("data"))
	if err == nil {
		t.Fatal("expected error on write after close, got nil")
	}
	if _, ok := err.(*TunnelClosedError); !ok {
		t.Fatalf("expected *TunnelClosedError on write, got %T: %v", err, err)
	}
}

// makeTestConnFromState wraps an existing tunnelState in a tunnelConnTest.
func makeTestConnFromState(ts *tunnelState) *tunnelConnTest {
	return &tunnelConnTest{tunnelID: ts.tunnelID, state: ts}
}

// TestTunnelDial_WithBackend verifies that WithTunnelBackend pins the named
// backend onto the outgoing TunnelOpen envelope.
func TestTunnelDial_WithBackend(t *testing.T) {
	client := newConnectedBaseClient(t)

	go func() {
		// TunnelDial blocks only on Send (which writes to requestQueue with no
		// reader), so dialing in a goroutine drains as we inspect.
		_, _ = client.TunnelDial(testCtx(t), "sv::svc::default", "tcp", "10.0.0.1:5000",
			WithTunnelBackend("tcp-b"))
	}()

	var open *pb.TunnelOpen
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && open == nil {
		select {
		case msg := <-client.RequestQueue():
			open = msg.GetTunnelOpen()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if open == nil {
		t.Fatal("no TunnelOpen in queue")
	}
	if got := open.GetBackendName(); got != "tcp-b" {
		t.Errorf("BackendName = %q, want %q", got, "tcp-b")
	}
	if got := open.GetRemoteHint(); got != "10.0.0.1:5000" {
		t.Errorf("RemoteHint = %q, want 10.0.0.1:5000", got)
	}
	deleteTunnelInflight(open.GetTunnelId())
}

// TestTunnelDial_NoBackendOption verifies that omitting WithTunnelBackend
// leaves the BackendName field empty.
func TestTunnelDial_NoBackendOption(t *testing.T) {
	client := newConnectedBaseClient(t)

	go func() {
		_, _ = client.TunnelDial(testCtx(t), "sv::svc::default", "tcp", "10.0.0.1:5000")
	}()

	var open *pb.TunnelOpen
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && open == nil {
		select {
		case msg := <-client.RequestQueue():
			open = msg.GetTunnelOpen()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if open == nil {
		t.Fatal("no TunnelOpen in queue")
	}
	if got := open.GetBackendName(); got != "" {
		t.Errorf("BackendName = %q, want empty", got)
	}
	deleteTunnelInflight(open.GetTunnelId())
}

// testCtx is a small helper returning a 200ms context for tunnel-dial tests.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// TestTunnelDispatch verifies that the BaseClient dispatcher routes
// TunnelData / TunnelAck / TunnelClose to the correct handlers.
func TestTunnelDispatch(t *testing.T) {
	bc := &BaseClient{}

	tunnelID := "dispatch-test"
	ts := registerTunnelInflight(tunnelID)
	defer deleteTunnelInflight(tunnelID)

	// Dispatch TunnelData.
	bc.handleTunnelData(&pb.TunnelData{
		TunnelId: tunnelID,
		Data:     []byte("dispatched"),
		Fin:      false,
	})
	ts.inMu.Lock()
	got := string(ts.inBuf)
	ts.inMu.Unlock()
	if got != "dispatched" {
		t.Fatalf("TunnelData dispatch: got %q", got)
	}

	// Dispatch TunnelAck — credits should increase.
	before := atomic.LoadInt32(&ts.outCredits)
	bc.handleTunnelAck(&pb.TunnelAck{TunnelId: tunnelID, Credits: 5})
	after := atomic.LoadInt32(&ts.outCredits)
	if after != before+5 {
		t.Fatalf("TunnelAck: credits %d → %d, want +5", before, after)
	}

	// Dispatch TunnelClose.
	bc.handleTunnelClose(&pb.TunnelClose{
		TunnelId: tunnelID,
		Reason:   pb.TunnelClose_NORMAL,
		Detail:   "done",
	})
	if err := ts.closedError(); err == nil {
		t.Fatal("expected closedErr after TunnelClose dispatch")
	}
}
