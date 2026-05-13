package proxysidecar

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	pb "github.com/scitrera/aether/api/proto"
)

// wsEchoServer stands up an httptest server that upgrades to WebSocket and
// echoes every received message back to the caller. Returns the listener URL
// (ws://...) plus the close cleanup hook.
func wsEchoServer(t *testing.T, subprotocols []string) string {
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
			t.Logf("ws echo upgrade: %v", err)
			return
		}
		defer c.Close()
		for {
			mt, payload, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(mt, payload); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws://" + strings.TrimPrefix(srv.URL, "http://")
}

// wsHeaderEchoServer captures the headers it received so the test can verify
// caller-supplied headers propagate. The first message back is the JSON-ish
// header dump (key=value;...).
func wsHeaderEchoServer(t *testing.T, captured *http.Header) string {
	t.Helper()
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Snapshot the headers before the upgrade tears them down.
		if captured != nil {
			h := http.Header{}
			for k, vs := range r.Header {
				for _, v := range vs {
					h.Add(k, v)
				}
			}
			*captured = h
		}
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.WriteMessage(websocket.BinaryMessage, []byte("hello-from-server"))
		for {
			mt, payload, err := c.ReadMessage()
			if err != nil {
				return
			}
			_ = c.WriteMessage(mt, payload)
		}
	}))
	t.Cleanup(srv.Close)
	return "ws://" + strings.TrimPrefix(srv.URL, "http://")
}

// terminatorWithWS returns a terminator wired to a single WS backend pointed
// at urlStr.
func terminatorWithWS(t *testing.T, urlStr string, opts ...func(*BackendConfig)) *Terminator {
	t.Helper()
	bcfg := BackendConfig{
		Name:     "ws-default",
		Kind:     BackendKindWS,
		URL:      urlStr,
		MaxBytes: 1 << 30,
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

// encodeWSMessage frames a single WS payload for the inbound TunnelData byte
// stream. tagged controls whether the 1-byte kind tag is included.
func encodeWSMessage(payload []byte, tagged bool, kind byte) []byte {
	if tagged {
		out := make([]byte, 4+1+len(payload))
		binary.BigEndian.PutUint32(out[:4], uint32(1+len(payload)))
		out[4] = kind
		copy(out[5:], payload)
		return out
	}
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(len(payload)))
	copy(out[4:], payload)
	return out
}

// decodeWSStream walks the byte stream emitted by the sidecar and returns
// the sequence of decoded messages. tagged controls how kind bytes are
// interpreted on the data messages. expectPreamble tells the decoder to
// peel off a leading negotiation frame; otherwise the very first byte of a
// random binary payload could collide with the negotiation marker.
type decodedMsg struct {
	tag     byte // 0xFF for negotiation, 0x00/0x01 for tagged data, 0xFE for "untagged" entries
	payload []byte
}

func decodeWSStream(t *testing.T, stream []byte, tagged bool, expectPreamble bool) []decodedMsg {
	t.Helper()
	out := []decodedMsg{}
	first := true
	for len(stream) > 0 {
		if len(stream) < 4 {
			t.Fatalf("truncated length prefix: %d bytes left", len(stream))
		}
		ln := binary.BigEndian.Uint32(stream[:4])
		stream = stream[4:]
		if int(ln) > len(stream) {
			t.Fatalf("truncated payload: need %d, have %d", ln, len(stream))
		}
		body := stream[:ln]
		stream = stream[ln:]
		if first && expectPreamble {
			first = false
			if len(body) < 1 || body[0] != wsControlTagNegotiation {
				t.Fatalf("first frame not a negotiation preamble (tag=0x%02x)", body[0])
			}
			out = append(out, decodedMsg{tag: wsControlTagNegotiation, payload: append([]byte(nil), body[1:]...)})
			continue
		}
		first = false
		if tagged {
			if len(body) < 1 {
				t.Fatalf("tagged frame missing tag byte")
			}
			out = append(out, decodedMsg{tag: body[0], payload: append([]byte(nil), body[1:]...)})
		} else {
			out = append(out, decodedMsg{tag: 0xFE, payload: append([]byte(nil), body...)})
		}
	}
	return out
}

// TestWSTunnel_FullDuplexBinaryEcho writes 10 binary messages through an
// echo backend and asserts the sidecar returns them intact.
func TestWSTunnel_FullDuplexBinaryEcho(t *testing.T) {
	t.Parallel()

	wsURL := wsEchoServer(t, nil)
	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-ws-bin",
		Protocol:      pb.TunnelOpen_WEBSOCKET,
		IdleTimeoutMs: 30_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-ws-bin", Credits: 16 * wsFrameMaxBytes})

	const n = 10
	payloads := make([][]byte, n)
	stream := []byte{}
	for i := 0; i < n; i++ {
		p := make([]byte, 1024+i*128)
		if _, err := rand.Read(p); err != nil {
			t.Fatalf("rand: %v", err)
		}
		payloads[i] = p
		stream = append(stream, encodeWSMessage(p, false, 0)...)
	}
	// Single multi-frame inbound delivery: chunk the stream so multiple
	// TunnelData frames are required.
	pumpInbound(term, ft, "tn-ws-bin", stream, 4096, true)

	c := waitForClose(t, ft, 10*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	got := decodeWSStream(t, ft.dataBytes, false, false)
	if len(got) != n {
		t.Fatalf("decoded messages: got %d, want %d", len(got), n)
	}
	for i := range payloads {
		if string(got[i].payload) != string(payloads[i]) {
			t.Errorf("msg %d differs: got %d bytes, want %d", i, len(got[i].payload), len(payloads[i]))
		}
	}
	if len(ft.ackFrames) == 0 {
		t.Errorf("expected at least one TunnelAck for inbound bytes")
	}
}

// TestWSTunnel_LargeMessageReassembled pushes a single >256 KiB WS message
// and asserts the sidecar splits it across multiple TunnelData frames and
// the echo backend reassembles it correctly.
func TestWSTunnel_LargeMessageReassembled(t *testing.T) {
	t.Parallel()

	wsURL := wsEchoServer(t, nil)
	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-ws-large",
		Protocol:      pb.TunnelOpen_WEBSOCKET,
		IdleTimeoutMs: 30_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	go func() {
		// Generously over-grant to avoid flow-control gating this test.
		for i := 0; i < 64; i++ {
			term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-ws-large", Credits: uint32(wsFrameMaxBytes)})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	const total = 1024 * 1024 // 1 MiB single WS message
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	stream := encodeWSMessage(payload, false, 0)
	pumpInbound(term, ft, "tn-ws-large", stream, 32*1024, true)

	c := waitForClose(t, ft, 30*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	got := decodeWSStream(t, ft.dataBytes, false, false)
	if len(got) != 1 {
		t.Fatalf("decoded messages: got %d, want 1", len(got))
	}
	if len(got[0].payload) != total {
		t.Fatalf("payload size: got %d, want %d", len(got[0].payload), total)
	}
	for i := range payload {
		if got[0].payload[i] != payload[i] {
			t.Fatalf("byte %d differs: got %x want %x", i, got[0].payload[i], payload[i])
		}
	}
	// Multi-frame outbound expected since payload >> 256 KiB.
	if len(ft.dataFrames) < 4 {
		t.Errorf("expected multi-frame outbound; got %d frames", len(ft.dataFrames))
	}
}

// TestWSTunnel_SubprotocolNegotiation requests two subprotocols from the
// backend, has the backend select one, and asserts the selected value
// propagates back via SelectedSubprotocol AND via the negotiation preamble
// frame embedded in the outbound stream.
func TestWSTunnel_SubprotocolNegotiation(t *testing.T) {
	t.Parallel()

	wsURL := wsEchoServer(t, []string{"v2"})
	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-ws-subproto",
		Protocol:      pb.TunnelOpen_WEBSOCKET,
		IdleTimeoutMs: 30_000,
		Metadata: map[string]string{
			wsMetaSubprotocols: "v1,v2",
		},
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	tun := term.tunnels.get("tn-ws-subproto")
	if tun == nil {
		t.Fatal("tunnel not registered")
	}
	wsTun, ok := tun.(*wsTunnel)
	if !ok {
		t.Fatalf("expected *wsTunnel, got %T", tun)
	}
	if got := wsTun.SelectedSubprotocol(); got != "v2" {
		t.Errorf("SelectedSubprotocol(): got %q, want %q", got, "v2")
	}

	// Drive a single round-trip so the preamble is flushed onto the stream.
	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-ws-subproto", Credits: 16 * wsFrameMaxBytes})
	stream := encodeWSMessage([]byte("ping"), false, 0)
	pumpInbound(term, ft, "tn-ws-subproto", stream, 4096, true)

	c := waitForClose(t, ft, 10*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close: got %s (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	got := decodeWSStream(t, ft.dataBytes, false, true)
	if len(got) < 1 {
		t.Fatalf("expected at least the negotiation preamble, got 0 frames")
	}
	if got[0].tag != wsControlTagNegotiation {
		t.Fatalf("first frame tag: got 0x%02x, want 0x%02x", got[0].tag, wsControlTagNegotiation)
	}
	if string(got[0].payload) != "v2" {
		t.Errorf("preamble subprotocol: got %q, want %q", string(got[0].payload), "v2")
	}
}

// TestWSTunnel_TaggedFramingMixed exercises tagged framing where the same
// tunnel mixes binary and text WS messages.
func TestWSTunnel_TaggedFramingMixed(t *testing.T) {
	t.Parallel()

	wsURL := wsEchoServer(t, nil)
	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-ws-tagged",
		Protocol:      pb.TunnelOpen_WEBSOCKET,
		IdleTimeoutMs: 30_000,
		Metadata: map[string]string{
			wsMetaFraming: "tagged",
		},
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-ws-tagged", Credits: 16 * wsFrameMaxBytes})

	binMsg := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	txtMsg := []byte("hello text")
	stream := append([]byte{}, encodeWSMessage(binMsg, true, wsControlTagBinary)...)
	stream = append(stream, encodeWSMessage(txtMsg, true, wsControlTagText)...)
	pumpInbound(term, ft, "tn-ws-tagged", stream, 4096, true)

	c := waitForClose(t, ft, 10*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close: got %s (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	got := decodeWSStream(t, ft.dataBytes, true, false)
	if len(got) != 2 {
		t.Fatalf("decoded messages: got %d, want 2", len(got))
	}
	if got[0].tag != wsControlTagBinary || string(got[0].payload) != string(binMsg) {
		t.Errorf("first echoed frame: tag=0x%02x payload=%x; want bin=%x", got[0].tag, got[0].payload, binMsg)
	}
	if got[1].tag != wsControlTagText || string(got[1].payload) != string(txtMsg) {
		t.Errorf("second echoed frame: tag=0x%02x payload=%q; want text=%q", got[1].tag, string(got[1].payload), string(txtMsg))
	}
}

// TestWSTunnel_DialFailureClosesWithError asserts that dialing a backend
// that doesn't speak WebSocket produces a synchronous TunnelClose{ERROR}.
func TestWSTunnel_DialFailureClosesWithError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no ws here", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws://" + strings.TrimPrefix(srv.URL, "http://")

	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-ws-dial", Protocol: pb.TunnelOpen_WEBSOCKET},
		ft)
	if c == nil {
		t.Fatal("expected synchronous TunnelClose for dial failure")
	}
	if c.GetReason() != pb.TunnelClose_ERROR {
		t.Errorf("reason: got %s, want ERROR", c.GetReason())
	}
}

// TestWSTunnel_AllowListRejects asserts a remote_hint outside the configured
// allow_remote_hints list synchronously closes with ERROR.
func TestWSTunnel_AllowListRejects(t *testing.T) {
	t.Parallel()

	wsURL := wsEchoServer(t, nil)
	term := terminatorWithWS(t, wsURL, func(b *BackendConfig) {
		b.AllowRemoteHints = []string{"ws://approved.example/*"}
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{
			TunnelId:   "tn-ws-deny",
			Protocol:   pb.TunnelOpen_WEBSOCKET,
			RemoteHint: "ws://forbidden.example/socket",
		},
		ft)
	if c == nil {
		t.Fatal("expected synchronous TunnelClose for disallowed hint")
	}
	if c.GetReason() != pb.TunnelClose_ERROR {
		t.Errorf("reason: got %s, want ERROR", c.GetReason())
	}
	if !strings.Contains(c.GetDetail(), "ACL_DENIED") {
		t.Errorf("detail %q should mention ACL_DENIED", c.GetDetail())
	}
}

// TestWSTunnel_NoWSBackendReturnsError asserts that a TunnelOpen{WEBSOCKET}
// against a terminator with no WS backends synchronously closes with ERROR.
func TestWSTunnel_NoWSBackendReturnsError(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig() // HTTP-only
	term, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	ft := newFakeTransport()
	c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-ws-no-backend", Protocol: pb.TunnelOpen_WEBSOCKET},
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

// TestWSTunnel_IdleTimeoutFires opens a tunnel against a backend that never
// sends or receives, asserting TunnelClose{IDLE_TIMEOUT} fires once the
// idle window elapses.
func TestWSTunnel_IdleTimeoutFires(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Hold the conn open without doing IO; close on test exit.
		t.Cleanup(func() { c.Close() })
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws://" + strings.TrimPrefix(srv.URL, "http://")

	term := terminatorWithWS(t, wsURL, func(b *BackendConfig) {
		b.IdleTimeoutMs = 200
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-ws-idle", Protocol: pb.TunnelOpen_WEBSOCKET, IdleTimeoutMs: 200},
		ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_IDLE_TIMEOUT {
		t.Fatalf("close reason: got %s, want IDLE_TIMEOUT (detail=%q)", c.GetReason(), c.GetDetail())
	}
}

// TestWSTunnel_MaxBytesFiresQuotaClose verifies that pushing data over
// max_bytes triggers a TunnelClose{QUOTA}.
func TestWSTunnel_MaxBytesFiresQuotaClose(t *testing.T) {
	t.Parallel()

	wsURL := wsEchoServer(t, nil)
	term := terminatorWithWS(t, wsURL, func(b *BackendConfig) {
		b.MaxBytes = 1024
	})
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	if c := term.HandleTunnelOpen(context.Background(),
		&pb.TunnelOpen{TunnelId: "tn-ws-quota", Protocol: pb.TunnelOpen_WEBSOCKET, IdleTimeoutMs: 30_000},
		ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-ws-quota", Credits: 16 * wsFrameMaxBytes})

	// Push a 4 KiB message into a 1 KiB cap. Expect QUOTA close.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}
	stream := encodeWSMessage(payload, false, 0)
	pumpInbound(term, ft, "tn-ws-quota", stream, 256, false)

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_QUOTA {
		t.Fatalf("close reason: got %s, want QUOTA (detail=%q)", c.GetReason(), c.GetDetail())
	}
}

// TestWSTunnel_HeadersPropagate asserts caller-supplied headers from
// metadata reach the backend's HTTP upgrade request.
func TestWSTunnel_HeadersPropagate(t *testing.T) {
	t.Parallel()

	var captured http.Header
	wsURL := wsHeaderEchoServer(t, &captured)

	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-ws-hdr",
		Protocol:      pb.TunnelOpen_WEBSOCKET,
		IdleTimeoutMs: 30_000,
		Metadata: map[string]string{
			wsMetaHeaders: "X-Custom-Header=propagated;X-Another=value/2",
		},
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}

	if got := captured.Get("X-Custom-Header"); got != "propagated" {
		t.Errorf("X-Custom-Header: got %q, want %q", got, "propagated")
	}
	if got := captured.Get("X-Another"); got != "value/2" {
		t.Errorf("X-Another: got %q, want %q", got, "value/2")
	}

	term.HandleTunnelClose(&pb.TunnelClose{TunnelId: "tn-ws-hdr", Reason: pb.TunnelClose_NORMAL})
	waitForClose(t, ft, 5*time.Second)
}

// TestWSTunnel_HalfCloseFromCaller asserts that TunnelData{fin:true} after
// the backend has stopped reading produces a clean TunnelClose{NORMAL}.
func TestWSTunnel_HalfCloseFromCaller(t *testing.T) {
	t.Parallel()

	// Server reads one message, echoes it, then closes.
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		mt, payload, err := c.ReadMessage()
		if err != nil {
			return
		}
		_ = c.WriteMessage(mt, payload)
		// Initiate a normal close.
		_ = c.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws://" + strings.TrimPrefix(srv.URL, "http://")

	term := terminatorWithWS(t, wsURL)
	t.Cleanup(term.StopAllTunnels)

	ft := newFakeTransport()
	open := &pb.TunnelOpen{
		TunnelId:      "tn-ws-half",
		Protocol:      pb.TunnelOpen_WEBSOCKET,
		IdleTimeoutMs: 30_000,
	}
	if c := term.HandleTunnelOpen(context.Background(), open, ft); c != nil {
		t.Fatalf("HandleTunnelOpen: %s %s", c.GetReason(), c.GetDetail())
	}
	term.HandleTunnelAck(&pb.TunnelAck{TunnelId: "tn-ws-half", Credits: 16 * wsFrameMaxBytes})

	stream := encodeWSMessage([]byte("hello"), false, 0)
	pumpInbound(term, ft, "tn-ws-half", stream, 4096, true)

	c := waitForClose(t, ft, 5*time.Second)
	if c.GetReason() != pb.TunnelClose_NORMAL {
		t.Fatalf("close reason: got %s, want NORMAL (detail=%q)", c.GetReason(), c.GetDetail())
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	got := decodeWSStream(t, ft.dataBytes, false, false)
	if len(got) != 1 {
		t.Fatalf("decoded messages: got %d, want 1", len(got))
	}
	if string(got[0].payload) != "hello" {
		t.Errorf("payload: got %q, want %q", string(got[0].payload), "hello")
	}
	// The last data frame should carry fin=true so the caller knows our
	// outbound half is closed.
	if n := len(ft.dataFrames); n == 0 || !ft.dataFrames[n-1].GetFin() {
		t.Errorf("expected last TunnelData frame to carry fin=true, frames=%d", n)
	}
}

// TestResolveWSAddress_AllowList exercises ws remote_hint matching paths.
func TestResolveWSAddress_AllowList(t *testing.T) {
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
			cfg:  BackendConfig{URL: "ws://127.0.0.1:8080/socket"},
			hint: "",
			want: "ws://127.0.0.1:8080/socket",
		},
		{
			name: "hint with no allow list falls back to url",
			cfg:  BackendConfig{URL: "ws://127.0.0.1:8080/socket"},
			hint: "ws://other.example/x",
			want: "ws://127.0.0.1:8080/socket",
		},
		{
			name: "allow list permits hint",
			cfg:  BackendConfig{URL: "ws://127.0.0.1:8080/socket", AllowRemoteHints: []string{"ws://*.example/*"}},
			hint: "ws://api.example/socket",
			want: "ws://api.example/socket",
		},
		{
			name:    "allow list rejects hint",
			cfg:     BackendConfig{URL: "ws://127.0.0.1:8080/socket", AllowRemoteHints: []string{"ws://*.example/*"}},
			hint:    "ws://forbidden.invalid/x",
			wantErr: true,
		},
		{
			name:    "non-ws scheme rejected",
			cfg:     BackendConfig{URL: "tcp://127.0.0.1:8080"},
			hint:    "",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveWSAddress(tc.cfg, tc.hint)
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
