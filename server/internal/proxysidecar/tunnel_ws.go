package proxysidecar

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// WebSocket tunnel framing constants. Mirror the TCP backend so callers see
// uniform credit accounting across protocols.
const (
	// wsFrameMaxBytes caps a single outbound TunnelData frame at 256 KiB,
	// matching the TCP backend so flow-control accounting is uniform.
	wsFrameMaxBytes = 256 * 1024

	// wsInitialOutboundCredits primes the outbound flow-control window so
	// the sidecar can begin pumping bytes before the first TunnelAck arrives.
	wsInitialOutboundCredits = 4 * wsFrameMaxBytes // 1 MiB

	// wsInboundAckThreshold is the consumed-byte threshold at which the
	// sidecar emits a TunnelAck back to the caller granting more credits.
	wsInboundAckThreshold = wsFrameMaxBytes

	// wsMaxMessageBytes caps a single reassembled WS message at 16 MiB.
	// Larger messages would defeat the credit window's purpose.
	wsMaxMessageBytes = 16 * 1024 * 1024

	// wsControlTagBinary marks a binary message in tagged-framing mode.
	wsControlTagBinary byte = 0x00
	// wsControlTagText marks a UTF-8 text message in tagged-framing mode.
	wsControlTagText byte = 0x01
	// wsControlTagNegotiation marks a sidecar-emitted negotiation preamble
	// (carries the selected subprotocol). Always sent first when the caller
	// requested subprotocol negotiation.
	wsControlTagNegotiation byte = 0xFF
)

// Metadata keys recognised on TunnelOpen for WebSocket tunnels. The wire
// format is "key=value;key=value" for the headers map and a comma-separated
// list for subprotocols.
const (
	wsMetaSubprotocols = "subprotocols"
	wsMetaHeaders      = "headers"

	// wsMetaMessageKind declares the per-tunnel default message kind:
	// "binary" (default) or "text". When set to a single kind, every WS
	// message in either direction uses that kind and the byte stream omits
	// per-message tag bytes.
	wsMetaMessageKind = "ws_message_kind"

	// wsMetaFraming opts the tunnel into tagged framing where each WS
	// message carries a 1-byte kind prefix (0=binary, 1=text). Use this for
	// callers that need to mix text and binary messages on the same tunnel.
	// Recognised value: "tagged".
	wsMetaFraming = "ws_framing"
)

// wsDialer abstracts the websocket Dial so tests can inject a synthetic
// backend without standing up a real listener.
type wsDialer func(ctx context.Context, urlStr string, header http.Header, subprotocols []string) (*websocket.Conn, *http.Response, error)

// defaultWSDialer dials a WebSocket backend with a 10s handshake timeout.
func defaultWSDialer(ctx context.Context, urlStr string, header http.Header, subprotocols []string) (*websocket.Conn, *http.Response, error) {
	d := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Subprotocols:     subprotocols,
		ReadBufferSize:   wsFrameMaxBytes,
		WriteBufferSize:  wsFrameMaxBytes,
	}
	return d.DialContext(ctx, urlStr, header)
}

// wsBackend evaluates a TunnelOpen against config and stands up a per-tunnel
// pump connecting the local WebSocket service to the caller via tunnelTransport.
type wsBackend struct {
	cfg            BackendConfig
	dialer         wsDialer
	targetResolver tunnelTargetResolver
}

// newWSBackend constructs a WS backend from cfg. dialer may be nil to use
// the default websocket dialer.
func newWSBackend(cfg BackendConfig, dialer wsDialer) *wsBackend {
	if dialer == nil {
		dialer = defaultWSDialer
	}
	return &wsBackend{cfg: cfg, dialer: dialer}
}

// open dials the backend WebSocket and starts the bidirectional pump
// goroutines for the tunnel described by the open frame.
func (b *wsBackend) open(ctx context.Context, open *pb.TunnelOpen, transport tunnelTransport) (*wsTunnel, error) {
	target, err := resolveWSAddress(b.cfg, open.GetRemoteHint())
	if err != nil {
		return nil, err
	}

	if err := enforceTunnelTargetScope(b.targetResolver, open, b.cfg.Name, identityheaders.TunnelProtocolWS, open.GetRemoteHint()); err != nil {
		return nil, err
	}

	subprotocols := parseSubprotocols(open.GetMetadata()[wsMetaSubprotocols])
	headers := parseHeaderMap(open.GetMetadata()[wsMetaHeaders])

	conn, resp, err := b.dialer(ctx, target, headers, subprotocols)
	if err != nil {
		return nil, fmt.Errorf("ws dial %s: %w", target, err)
	}
	selected := conn.Subprotocol()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	maxBytes := b.cfg.MaxBytes
	if open.GetMaxBytes() > 0 && (maxBytes == 0 || open.GetMaxBytes() < maxBytes) {
		maxBytes = open.GetMaxBytes()
	}
	idle := time.Duration(b.cfg.IdleTimeoutMs) * time.Millisecond
	if open.GetIdleTimeoutMs() > 0 {
		callerIdle := time.Duration(open.GetIdleTimeoutMs()) * time.Millisecond
		if callerIdle < idle {
			idle = callerIdle
		}
	}
	if idle <= 0 {
		idle = 5 * time.Minute
	}

	defaultKind := websocket.BinaryMessage
	switch strings.ToLower(strings.TrimSpace(open.GetMetadata()[wsMetaMessageKind])) {
	case "", "binary":
		defaultKind = websocket.BinaryMessage
	case "text":
		defaultKind = websocket.TextMessage
	}
	tagged := strings.EqualFold(strings.TrimSpace(open.GetMetadata()[wsMetaFraming]), "tagged")

	tunCtx, cancel := context.WithCancel(ctx)
	t := &wsTunnel{
		tunnelID:           open.GetTunnelId(),
		conn:               conn,
		transport:          transport,
		ctx:                tunCtx,
		cancel:             cancel,
		idle:               idle,
		maxBytes:           maxBytes,
		defaultKind:        defaultKind,
		tagged:             tagged,
		selectedSubproto:   selected,
		negotiationEmitted: len(subprotocols) == 0, // skip preamble unless caller requested negotiation
		creditsCh:          make(chan struct{}, 1),
		writeCh:            make(chan wsOutboundFrame, 16),
	}
	t.credits.Store(int64(wsInitialOutboundCredits))
	t.lastSeenNs.Store(time.Now().UnixNano())

	t.wg.Add(3)
	go t.pumpSocketToCaller()
	go t.pumpCallerToSocket()
	go t.idleWatcher()
	return t, nil
}

// resolveWSAddress picks the dial target for a WS tunnel. Mirrors
// resolveTCPAddress but operates on ws:// / wss:// URLs.
func resolveWSAddress(cfg BackendConfig, hint string) (string, error) {
	defaultURL := normaliseWSURL(cfg.URL)
	if hint == "" {
		if defaultURL == "" {
			return "", errors.New("no remote_hint and ws backend has no default url")
		}
		return defaultURL, nil
	}
	if len(cfg.AllowRemoteHints) == 0 {
		if defaultURL == "" {
			return "", errors.New("ws backend has no default url and remote_hint is not allow-listed")
		}
		return defaultURL, nil
	}
	for _, pattern := range cfg.AllowRemoteHints {
		if matchHint(pattern, hint) {
			normalised := normaliseWSURL(hint)
			if normalised == "" {
				return "", fmt.Errorf("remote_hint %q is not a valid ws/wss url", hint)
			}
			return normalised, nil
		}
	}
	return "", fmt.Errorf("remote_hint %q does not match any allow_remote_hints pattern", hint)
}

// normaliseWSURL accepts ws://host/path, wss://host/path, or a bare host:port
// (in which case ws:// is assumed) and returns a URL suitable for the dialer.
// Returns "" on syntactic failure.
func normaliseWSURL(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if !strings.Contains(s, "://") {
		s = "ws://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return ""
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return ""
	}
	if u.Host == "" {
		return ""
	}
	return u.String()
}

// parseSubprotocols turns "v1,v2 ,v3" into ["v1","v2","v3"]. Empty inputs
// produce a nil slice so the dialer omits the Sec-WebSocket-Protocol header.
func parseSubprotocols(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// parseHeaderMap turns "Host=x.example;Cookie=foo=bar" into an http.Header.
// Semicolon-delimited so values may contain '='. Repeated keys append.
func parseHeaderMap(s string) http.Header {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	h := http.Header{}
	for _, pair := range strings.Split(s, ";") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		eq := strings.IndexByte(pair, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eq])
		val := pair[eq+1:]
		if key == "" {
			continue
		}
		h.Add(key, val)
	}
	if len(h) == 0 {
		return nil
	}
	return h
}

// wsOutboundFrame is the queue entry the writer goroutine pulls from. kind
// is the gorilla/websocket message type; data is the payload to send. The
// special kind wsOutboundFinish requests a close frame after the queue
// drains, signalling the half-close path back to the writer.
type wsOutboundFrame struct {
	kind int
	data []byte
}

// wsOutboundFinish is a sentinel message kind: when the writer goroutine
// pulls it from writeCh, it has already drained every pending data write,
// so it is safe to issue the closing handshake without racing the queue.
const wsOutboundFinish = -1

// wsTunnel is the per-tunnel state for an active WebSocket backend bridge.
// One instance is registered with the terminator's tunnel manager keyed by
// tunnel_id; inbound/outbound dispatch routes through its handle methods.
type wsTunnel struct {
	tunnelID  string
	conn      *websocket.Conn
	transport tunnelTransport

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	idle     time.Duration
	maxBytes int64

	// credits tracks remaining outbound bytes the caller has authorised.
	credits   atomic.Int64
	creditsCh chan struct{}

	// inboundBytes counts WS payload bytes written to the backend (caller →
	// backend). outboundBytes counts payload bytes read from the backend
	// and shipped to the caller. Both feed quota enforcement.
	inboundBytes  atomic.Int64
	outboundBytes atomic.Int64

	pendingAckBytes atomic.Int64

	inboundSeq  atomic.Uint32
	outboundSeq atomic.Uint32

	lastSeenNs atomic.Int64

	// defaultKind is the WS message type used for unframed payloads.
	defaultKind int
	// tagged enables 1-byte kind prefixing per message.
	tagged bool

	// selectedSubproto is the WS subprotocol the backend chose during
	// handshake. Exposed to tests via SelectedSubprotocol so the negotiation
	// outcome is observable without parsing the preamble frame.
	selectedSubproto string
	// negotiationEmitted guards the subprotocol-ack preamble so it is sent
	// at most once and only when the caller asked for negotiation.
	negotiationEmitted bool

	// inboundBuf accumulates bytes from caller-side TunnelData frames so
	// length-prefixed WS messages can span multiple TunnelData frames.
	inboundBuf []byte

	// writeCh serialises writes onto the WS conn from one goroutine, since
	// gorilla/websocket forbids concurrent writers.
	writeCh chan wsOutboundFrame

	closeOnce      sync.Once
	closeReason    pb.TunnelClose_Reason
	closeDetail    string
	mu             sync.Mutex
	callerSentFin  bool
	socketEOF      bool
	finishedNormal bool
	closed         bool
}

// id returns the tunnel identifier; satisfies activeTunnel.
func (t *wsTunnel) id() string { return t.tunnelID }

// storeInboundSeq records the latest inbound sequence number observed.
func (t *wsTunnel) storeInboundSeq(seq uint32) { t.inboundSeq.Store(seq) }

// SelectedSubprotocol exposes the WS subprotocol the backend chose at
// handshake time. Returns "" when the caller did not advertise any.
func (t *wsTunnel) SelectedSubprotocol() string { return t.selectedSubproto }

// pumpSocketToCaller reads WS messages from the backend, length-prefix-frames
// them onto the byte stream, splits the stream into TunnelData chunks, and
// ships them via the transport. Honours credit-based flow control.
func (t *wsTunnel) pumpSocketToCaller() {
	defer t.wg.Done()
	defer t.markSocketEOF()

	// Emit the negotiation preamble first when the caller asked for one.
	// Always shipped as a length-prefixed control message tagged with
	// wsControlTagNegotiation so the caller can observe the selected
	// subprotocol without parsing per-tunnel ad-hoc framing.
	if !t.negotiationEmitted {
		t.negotiationEmitted = true
		preamble := buildNegotiationPreamble(t.selectedSubproto)
		if !t.shipBytes(preamble) {
			return
		}
	}

	for {
		if t.ctx.Err() != nil {
			return
		}
		// Read one full WS message from the backend. We accept arbitrary
		// fragmentation inside the WS layer; the lib reassembles for us.
		t.conn.SetReadLimit(int64(wsMaxMessageBytes))
		mt, payload, err := t.conn.ReadMessage()
		if err != nil {
			if !isWSExpectedClose(err) {
				log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("ws tunnel: backend read error")
			}
			// Emit a fin frame so the caller knows our send half is closed.
			seq := t.outboundSeq.Add(1)
			finFrame := &pb.TunnelData{TunnelId: t.tunnelID, Seq: seq, Fin: true}
			_ = t.transport.SendTunnelData(finFrame)
			return
		}
		t.lastSeenNs.Store(time.Now().UnixNano())

		framed := t.encodeOutboundMessage(mt, payload)
		if framed == nil {
			// Dropped a control frame (ping/pong) or an unknown type — skip.
			continue
		}
		if !t.shipBytes(framed) {
			return
		}
	}
}

// shipBytes splits a fully-framed message buffer into TunnelData chunks of
// at most wsFrameMaxBytes apiece, blocking on credits as necessary, and
// ships them through the transport. Returns false on cancellation or send
// failure (caller should exit the read loop).
func (t *wsTunnel) shipBytes(buf []byte) bool {
	for off := 0; off < len(buf); {
		if t.ctx.Err() != nil {
			return false
		}
		if !t.waitForCredits() {
			return false
		}
		chunk := wsFrameMaxBytes
		if c := t.credits.Load(); c < int64(chunk) {
			chunk = int(c)
		}
		if chunk <= 0 {
			continue
		}
		if off+chunk > len(buf) {
			chunk = len(buf) - off
		}
		piece := buf[off : off+chunk]
		t.credits.Add(-int64(chunk))
		t.outboundBytes.Add(int64(chunk))

		seq := t.outboundSeq.Add(1)
		frame := &pb.TunnelData{
			TunnelId: t.tunnelID,
			Seq:      seq,
			Data:     append([]byte(nil), piece...),
		}
		if err := t.transport.SendTunnelData(frame); err != nil {
			log.Warn().Err(err).Str("tunnel_id", t.tunnelID).Msg("ws tunnel: send TunnelData failed")
			t.finalize(pb.TunnelClose_ERROR, "send failed: "+err.Error())
			return false
		}
		if t.maxBytes > 0 && t.outboundBytes.Load()+t.inboundBytes.Load() > t.maxBytes {
			t.finalize(pb.TunnelClose_QUOTA, fmt.Sprintf("tunnel byte cap %d exceeded", t.maxBytes))
			return false
		}
		off += chunk
	}
	return true
}

// pumpCallerToSocket serialises writes onto the underlying WS conn. Reading
// from a single channel guarantees gorilla/websocket's "one writer at a
// time" invariant even if handleData is invoked concurrently.
func (t *wsTunnel) pumpCallerToSocket() {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg, ok := <-t.writeCh:
			if !ok {
				return
			}
			if msg.kind == wsOutboundFinish {
				// Caller-half is fully drained — send a normal close to
				// the backend so its read loop terminates and our pump
				// observes EOF, allowing maybeFinishNormal to fire.
				_ = t.conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(2*time.Second))
				return
			}
			if err := t.conn.WriteMessage(msg.kind, msg.data); err != nil {
				log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("ws tunnel: backend write error")
				t.finalize(pb.TunnelClose_ERROR, "ws write: "+err.Error())
				return
			}
			t.lastSeenNs.Store(time.Now().UnixNano())
		}
	}
}

// waitForCredits blocks until at least one byte of outbound credit is
// available or the tunnel is cancelled. Returns false on cancellation.
func (t *wsTunnel) waitForCredits() bool {
	if t.credits.Load() > 0 {
		return true
	}
	for {
		select {
		case <-t.ctx.Done():
			return false
		case <-t.creditsCh:
			if t.credits.Load() > 0 {
				return true
			}
		}
	}
}

// idleWatcher fires TunnelClose{IDLE_TIMEOUT} if no traffic flows in either
// direction for the configured idle interval.
func (t *wsTunnel) idleWatcher() {
	defer t.wg.Done()
	if t.idle <= 0 {
		return
	}
	tick := t.idle / 4
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Duration(time.Now().UnixNano() - t.lastSeenNs.Load())
			if elapsed >= t.idle {
				t.finalize(pb.TunnelClose_IDLE_TIMEOUT,
					fmt.Sprintf("no activity for %s", elapsed.Round(time.Millisecond)))
				return
			}
		}
	}
}

// handleData consumes a TunnelData frame from the caller, appends it to the
// inbound byte buffer, decodes any complete length-prefixed WS messages it
// contains, and writes them to the backend via writeCh.
func (t *wsTunnel) handleData(frame *pb.TunnelData) {
	if frame == nil {
		return
	}
	t.lastSeenNs.Store(time.Now().UnixNano())
	if len(frame.Data) > 0 {
		consumed := int64(len(frame.Data))
		t.inboundBytes.Add(consumed)
		t.pendingAckBytes.Add(consumed)
		if t.maxBytes > 0 && t.inboundBytes.Load()+t.outboundBytes.Load() > t.maxBytes {
			t.finalize(pb.TunnelClose_QUOTA, fmt.Sprintf("tunnel byte cap %d exceeded", t.maxBytes))
			return
		}
		t.inboundBuf = append(t.inboundBuf, frame.Data...)
		if err := t.drainInboundMessages(); err != nil {
			t.finalize(pb.TunnelClose_ERROR, err.Error())
			return
		}
		if t.pendingAckBytes.Load() >= int64(wsInboundAckThreshold) {
			t.flushAck()
		}
	}
	if frame.GetFin() {
		t.handleCallerFin()
	}
}

// drainInboundMessages decodes as many complete length-prefixed WS messages
// from inboundBuf as possible and forwards them to the backend writer. The
// buffer is rewritten in place to drop consumed bytes.
func (t *wsTunnel) drainInboundMessages() error {
	for {
		if len(t.inboundBuf) < 4 {
			return nil
		}
		msgLen := binary.BigEndian.Uint32(t.inboundBuf[:4])
		if msgLen > wsMaxMessageBytes {
			return fmt.Errorf("ws message length %d exceeds cap %d", msgLen, wsMaxMessageBytes)
		}
		need := 4 + int(msgLen)
		if len(t.inboundBuf) < need {
			return nil
		}
		body := t.inboundBuf[4:need]

		kind := t.defaultKind
		var payload []byte
		if t.tagged {
			if len(body) < 1 {
				return errors.New("tagged ws frame missing kind byte")
			}
			tag := body[0]
			payload = body[1:]
			switch tag {
			case wsControlTagBinary:
				kind = websocket.BinaryMessage
			case wsControlTagText:
				kind = websocket.TextMessage
			default:
				return fmt.Errorf("unknown ws kind tag 0x%02x", tag)
			}
		} else {
			payload = body
		}

		// Copy out so the writer cannot race with future buffer truncations.
		payloadCopy := append([]byte(nil), payload...)
		select {
		case <-t.ctx.Done():
			return nil
		case t.writeCh <- wsOutboundFrame{kind: kind, data: payloadCopy}:
		}
		// Truncate the consumed prefix.
		remaining := len(t.inboundBuf) - need
		copy(t.inboundBuf, t.inboundBuf[need:])
		t.inboundBuf = t.inboundBuf[:remaining]
	}
}

// flushAck sends a TunnelAck granting fresh credits equal to the inbound
// bytes consumed since the previous ack. Resets the pending counter.
func (t *wsTunnel) flushAck() {
	pending := t.pendingAckBytes.Swap(0)
	if pending <= 0 {
		return
	}
	ack := &pb.TunnelAck{
		TunnelId: t.tunnelID,
		AckSeq:   t.inboundSeq.Load(),
		Credits:  uint32(pending),
	}
	if err := t.transport.SendTunnelAck(ack); err != nil {
		log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("ws tunnel: send TunnelAck failed")
		t.pendingAckBytes.Add(pending)
	}
}

// handleAck applies an inbound TunnelAck from the caller, releasing more
// outbound credits to the pump.
func (t *wsTunnel) handleAck(ack *pb.TunnelAck) {
	if ack == nil {
		return
	}
	if ack.GetCredits() == 0 {
		return
	}
	t.credits.Add(int64(ack.GetCredits()))
	select {
	case t.creditsCh <- struct{}{}:
	default:
	}
}

// handleClose terminates the tunnel locally. Always idempotent.
func (t *wsTunnel) handleClose(closeMsg *pb.TunnelClose) {
	reason := pb.TunnelClose_NORMAL
	detail := ""
	if closeMsg != nil {
		reason = closeMsg.GetReason()
		detail = closeMsg.GetDetail()
	}
	t.finalize(reason, detail)
}

// handleCallerFin processes TunnelData{fin:true} from the caller by queuing
// a sentinel onto the writer channel. The writer pump drains all pending
// data writes first, then sends a normal-closure WS close frame. WebSocket
// has no true half-close, so emitting the close frame is what causes the
// backend to terminate its read loop; the resulting read error in the
// socket pump flips socketEOF and maybeFinishNormal emits TunnelClose{NORMAL}.
func (t *wsTunnel) handleCallerFin() {
	t.mu.Lock()
	already := t.callerSentFin
	t.callerSentFin = true
	t.mu.Unlock()
	if !already {
		select {
		case <-t.ctx.Done():
		case t.writeCh <- wsOutboundFrame{kind: wsOutboundFinish}:
		}
	}
	t.maybeFinishNormal()
}

// markSocketEOF flags that the backend read half is done.
func (t *wsTunnel) markSocketEOF() {
	t.mu.Lock()
	t.socketEOF = true
	t.mu.Unlock()
	t.maybeFinishNormal()
}

// maybeFinishNormal emits a TunnelClose{NORMAL} when both halves are closed
// and no other terminating reason has been recorded.
func (t *wsTunnel) maybeFinishNormal() {
	t.mu.Lock()
	ready := t.callerSentFin && t.socketEOF && !t.finishedNormal && !t.closed
	if ready {
		t.finishedNormal = true
	}
	t.mu.Unlock()
	if ready {
		t.flushAck()
		t.finalize(pb.TunnelClose_NORMAL, "")
	}
}

// finalize cancels the tunnel context, closes the WS connection with the
// matching close code, and emits a TunnelClose if one has not already been
// sent. Safe to call repeatedly.
func (t *wsTunnel) finalize(reason pb.TunnelClose_Reason, detail string) {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closeReason = reason
		t.closeDetail = detail
		t.closed = true
		t.mu.Unlock()

		// Send a WS close frame matching the Aether reason before tearing
		// down the conn. Best-effort: ignore errors.
		closeCode := wsCloseCodeForReason(reason)
		closeMsg := websocket.FormatCloseMessage(closeCode, detail)
		_ = t.conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(2*time.Second))

		t.cancel()
		_ = t.conn.Close()

		closeFrame := &pb.TunnelClose{
			TunnelId: t.tunnelID,
			Reason:   reason,
			Detail:   detail,
		}
		if err := t.transport.SendTunnelClose(closeFrame); err != nil {
			log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("ws tunnel: send TunnelClose failed")
		}
	})
}

// stop forces the tunnel down. Used by the terminator on shutdown.
func (t *wsTunnel) stop() {
	t.finalize(pb.TunnelClose_NORMAL, "terminator shutdown")
	t.wg.Wait()
}

// encodeOutboundMessage serialises a backend-originated WS message into the
// length-prefixed format used on the TunnelData byte stream. Returns nil for
// message types that are not data (control frames are handled by the lib).
func (t *wsTunnel) encodeOutboundMessage(messageType int, payload []byte) []byte {
	switch messageType {
	case websocket.BinaryMessage, websocket.TextMessage:
	default:
		return nil
	}
	if t.tagged {
		body := make([]byte, 4+1+len(payload))
		binary.BigEndian.PutUint32(body[:4], uint32(1+len(payload)))
		switch messageType {
		case websocket.TextMessage:
			body[4] = wsControlTagText
		default:
			body[4] = wsControlTagBinary
		}
		copy(body[5:], payload)
		return body
	}
	body := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(body[:4], uint32(len(payload)))
	copy(body[4:], payload)
	return body
}

// buildNegotiationPreamble emits a length-prefixed control message carrying
// the selected WS subprotocol. The body is `[wsControlTagNegotiation][utf8]`
// so callers can distinguish it from regular tagged messages (which use 0x00
// or 0x01 tags).
func buildNegotiationPreamble(subproto string) []byte {
	body := []byte(subproto)
	out := make([]byte, 4+1+len(body))
	binary.BigEndian.PutUint32(out[:4], uint32(1+len(body)))
	out[4] = wsControlTagNegotiation
	copy(out[5:], body)
	return out
}

// wsCloseCodeForReason maps an Aether close reason to a WebSocket close
// code per RFC 6455. NORMAL → 1000, ERROR → 1011, IDLE_TIMEOUT/QUOTA → 1000
// with a descriptive reason in detail, PEER_RESET → 1001 (going away).
func wsCloseCodeForReason(reason pb.TunnelClose_Reason) int {
	switch reason {
	case pb.TunnelClose_NORMAL:
		return websocket.CloseNormalClosure
	case pb.TunnelClose_PEER_RESET:
		return websocket.CloseGoingAway
	case pb.TunnelClose_ERROR:
		return websocket.CloseInternalServerErr
	case pb.TunnelClose_IDLE_TIMEOUT, pb.TunnelClose_QUOTA:
		return websocket.CloseNormalClosure
	default:
		return websocket.CloseInternalServerErr
	}
}

// isWSExpectedClose reports whether err is a normal/going-away close that
// should not be logged as an error.
func isWSExpectedClose(err error) bool {
	if err == nil {
		return false
	}
	if websocket.IsCloseError(err,
		websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseNoStatusReceived) {
		return true
	}
	return false
}
