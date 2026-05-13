package proxysidecar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// tunnelTargetResolver returns the per-grant tunnel_target patterns for a
// given TunnelOpen. Implementations resolve the OBO authority on the open
// envelope and surface the resulting ResourceScope[tunnel_target] entries.
// A nil return (or empty slice) signals "no scope configured" — the
// matcher then defers to blanket allow.
type tunnelTargetResolver func(*pb.TunnelOpen) []string

// errTunnelTargetScopeDenied is the sentinel returned when a backend's open
// path resolves a tunnel_target scope but the (backend, protocol,
// remote_hint) triple fails the matcher. The terminator translates this
// into TunnelClose{ERROR, "tunnel_target_scope_denied"}.
var errTunnelTargetScopeDenied = errors.New("tunnel_target_scope_denied")

// enforceTunnelTargetScope evaluates the per-grant tunnel_target patterns
// returned by resolver against the chosen backend, protocol, and resolved
// remote_hint. Returns errTunnelTargetScopeDenied when a non-empty scope
// excludes the triple. Empty scope or nil resolver → allow.
func enforceTunnelTargetScope(resolver tunnelTargetResolver, open *pb.TunnelOpen, backendName, protocol, remoteHint string) error {
	if resolver == nil {
		return nil
	}
	patterns := resolver(open)
	if len(patterns) == 0 {
		return nil
	}
	if !identityheaders.MatchTunnelTarget(patterns, backendName, protocol, remoteHint) {
		return errTunnelTargetScopeDenied
	}
	return nil
}

// Tunnel framing constants.
const (
	// tcpFrameMaxBytes caps a single outbound TunnelData frame. Sized at
	// 256 KiB per the Aether tunnel spec — large enough to amortise per-frame
	// overhead, small enough to keep credit accounting fine-grained.
	tcpFrameMaxBytes = 256 * 1024

	// tcpInitialOutboundCredits primes the outbound flow-control window so
	// the sidecar can begin pumping bytes before the first TunnelAck arrives.
	// The caller is expected to refresh credits at least every 256 KiB.
	tcpInitialOutboundCredits = 4 * tcpFrameMaxBytes // 1 MiB

	// tcpInboundAckThreshold is the consumed-byte threshold at which the
	// sidecar emits a TunnelAck back to the caller granting more credits.
	tcpInboundAckThreshold = tcpFrameMaxBytes
)

// tunnelTransport sends frames produced by a tunnel back toward the caller.
// In production this is wired to the gateway-connected ServiceClient; tests
// inject a fake to assert frame sequencing and to inject TunnelAck signals.
type tunnelTransport interface {
	// SendTunnelData ships a TunnelData frame upstream toward the caller.
	SendTunnelData(*pb.TunnelData) error
	// SendTunnelClose ships a TunnelClose upstream and tears down state.
	SendTunnelClose(*pb.TunnelClose) error
	// SendTunnelAck ships a TunnelAck back to the caller. In v1 the upstream
	// proto cannot carry TunnelAck; tests use this to validate the sidecar's
	// internal accounting and production wiring is responsible for choosing
	// an appropriate carrier (e.g., piggy-backed on TunnelData seq=ack-only).
	SendTunnelAck(*pb.TunnelAck) error
	// SendProxyHttpResponse ships a ProxyHttpResponse upstream toward the
	// originating caller via the gateway's request-pin.
	SendProxyHttpResponse(*pb.ProxyHttpResponse) error
	// SendProxyHttpBodyChunk ships a ProxyHttpBodyChunk upstream. Used for
	// chunked-response delivery (is_request=false).
	SendProxyHttpBodyChunk(*pb.ProxyHttpBodyChunk) error
}

// tcpDialer abstracts net.Dial so tests can inject a synthetic backend.
type tcpDialer func(ctx context.Context, address string) (net.Conn, error)

// defaultTCPDialer dials the address with a 10s timeout.
func defaultTCPDialer(ctx context.Context, address string) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	return d.DialContext(ctx, "tcp", address)
}

// tcpBackend evaluates a TunnelOpen against config and stands up a per-tunnel
// pump connecting the local TCP service to the caller via tunnelTransport.
type tcpBackend struct {
	cfg            BackendConfig
	dialer         tcpDialer
	targetResolver tunnelTargetResolver
}

// newTCPBackend constructs a TCP backend from cfg. dialer may be nil to use
// the default net dialer.
func newTCPBackend(cfg BackendConfig, dialer tcpDialer) *tcpBackend {
	if dialer == nil {
		dialer = defaultTCPDialer
	}
	return &tcpBackend{cfg: cfg, dialer: dialer}
}

// open dials the backend and starts the bidirectional pump goroutines for
// the tunnel described by the open frame. The returned tcpTunnel exposes
// inbound/outbound frame entry points that the caller wires into the
// terminator's downstream message dispatcher.
func (b *tcpBackend) open(ctx context.Context, open *pb.TunnelOpen, transport tunnelTransport) (*tcpTunnel, error) {
	address, err := resolveTCPAddress(b.cfg, open.GetRemoteHint())
	if err != nil {
		return nil, err
	}

	if err := enforceTunnelTargetScope(b.targetResolver, open, b.cfg.Name, identityheaders.TunnelProtocolTCP, open.GetRemoteHint()); err != nil {
		return nil, err
	}

	conn, err := b.dialer(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", address, err)
	}

	maxBytes := b.cfg.MaxBytes
	if open.GetMaxBytes() > 0 && (maxBytes == 0 || open.GetMaxBytes() < maxBytes) {
		// Caller may request a smaller cap, never a larger one.
		maxBytes = open.GetMaxBytes()
	}

	idle := time.Duration(b.cfg.IdleTimeoutMs) * time.Millisecond
	if open.GetIdleTimeoutMs() > 0 {
		// Same rule: caller may shorten but not extend.
		callerIdle := time.Duration(open.GetIdleTimeoutMs()) * time.Millisecond
		if callerIdle < idle {
			idle = callerIdle
		}
	}
	if idle <= 0 {
		idle = 5 * time.Minute
	}

	tunCtx, cancel := context.WithCancel(ctx)
	t := &tcpTunnel{
		tunnelID:    open.GetTunnelId(),
		conn:        conn,
		transport:   transport,
		ctx:         tunCtx,
		cancel:      cancel,
		idle:        idle,
		maxBytes:    maxBytes,
		credits:     atomic.Int64{},
		creditsCh:   make(chan struct{}, 1),
		lastSeenNs:  atomic.Int64{},
		inboundSeq:  atomic.Uint32{},
		outboundSeq: atomic.Uint32{},
	}
	t.credits.Store(int64(tcpInitialOutboundCredits))
	t.lastSeenNs.Store(time.Now().UnixNano())

	t.wg.Add(2)
	go t.pumpSocketToCaller()
	go t.idleWatcher()
	return t, nil
}

// resolveTCPAddress picks the dial target for a tunnel. When the backend's
// allow_remote_hints is empty, the configured URL is the only legal target
// and the caller's remote_hint is ignored. When the list is populated,
// remote_hint must match at least one pattern and overrides the backend URL.
func resolveTCPAddress(cfg BackendConfig, hint string) (string, error) {
	defaultAddr := stripTCPScheme(cfg.URL)
	if hint == "" {
		if defaultAddr == "" {
			return "", errors.New("no remote_hint and backend has no default url")
		}
		return defaultAddr, nil
	}
	if len(cfg.AllowRemoteHints) == 0 {
		// Hint provided but backend forbids redirection — fall through to
		// the configured URL so we don't silently honour an unauthorised hint.
		if defaultAddr == "" {
			return "", errors.New("backend has no default url and remote_hint is not allow-listed")
		}
		return defaultAddr, nil
	}
	for _, pattern := range cfg.AllowRemoteHints {
		if matchHint(pattern, hint) {
			return stripTCPScheme(hint), nil
		}
	}
	return "", fmt.Errorf("remote_hint %q does not match any allow_remote_hints pattern", hint)
}

// matchHint matches a pattern against a remote_hint using path.Match glob
// semantics, with a fallback to exact equality for plain strings.
func matchHint(pattern, hint string) bool {
	if pattern == hint {
		return true
	}
	if matched, err := path.Match(pattern, hint); err == nil && matched {
		return true
	}
	// Allow "prefix:*" style entries to match as a prefix when path.Match
	// would refuse (e.g., colons in TCP-style hints).
	if strings.HasSuffix(pattern, ":*") {
		prefix := strings.TrimSuffix(pattern, ":*")
		if strings.HasPrefix(hint, prefix+":") || hint == prefix {
			return true
		}
	}
	return false
}

// stripTCPScheme normalises tcp://host:port to host:port. Bare host:port is
// passed through unchanged.
func stripTCPScheme(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "tcp://") {
		return strings.TrimPrefix(s, "tcp://")
	}
	return s
}

// tcpTunnel is the per-tunnel state for an active TCP backend bridge. One
// instance is registered with the terminator's tunnel manager keyed by
// tunnel_id; inbound/outbound dispatch routes through its handle methods.
type tcpTunnel struct {
	tunnelID  string
	conn      net.Conn
	transport tunnelTransport

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	idle     time.Duration
	maxBytes int64

	// credits tracks remaining outbound bytes the caller has authorised.
	// Refreshed by handleAck. The pumpSocketToCaller goroutine waits on
	// creditsCh whenever it observes a non-positive value.
	credits   atomic.Int64
	creditsCh chan struct{}

	// inboundBytes counts bytes written into the TCP socket from the caller.
	// Used to drive ack threshold and max_bytes enforcement.
	inboundBytes  atomic.Int64
	outboundBytes atomic.Int64

	// pendingAckBytes is the number of inbound bytes consumed since the most
	// recent TunnelAck was sent.
	pendingAckBytes atomic.Int64

	inboundSeq  atomic.Uint32
	outboundSeq atomic.Uint32

	// lastSeenNs is the most recent activity timestamp (UnixNano) used by
	// the idle watcher. Updated on every inbound/outbound byte.
	lastSeenNs atomic.Int64

	// closeOnce guards finalize from running twice; closeReason carries the
	// reason recorded for the eventual TunnelClose.
	closeOnce   sync.Once
	closeReason pb.TunnelClose_Reason
	closeDetail string

	// halfClose tracks the FIN state so we can emit TunnelClose{NORMAL} once
	// both directions are done.
	mu             sync.Mutex
	callerSentFin  bool // caller -> sidecar half closed (FIN observed inbound)
	socketEOF      bool // sidecar -> caller half closed (socket read EOF)
	finishedNormal bool
	closed         bool
}

// pumpSocketToCaller reads from the TCP socket, splits into TunnelData
// frames, and ships them via the transport. Respects credit-based flow
// control: when credits are exhausted, the goroutine blocks on creditsCh
// instead of consuming more bytes from the socket. This keeps the kernel
// recv buffer as the back-pressure store rather than letting the sidecar
// queue arbitrary amounts in memory.
func (t *tcpTunnel) pumpSocketToCaller() {
	defer t.wg.Done()
	defer func() {
		// On any exit (EOF, error, cancel) signal the half-close path.
		t.markSocketEOF()
	}()

	buf := make([]byte, tcpFrameMaxBytes)
	for {
		if t.ctx.Err() != nil {
			return
		}
		// Wait for outbound credit before pulling more bytes from the TCP
		// stack. We hold reads off until at least one byte is permitted.
		if !t.waitForCredits() {
			return
		}

		// Limit the chunk to the smaller of the per-frame cap and the
		// currently-available credits. Cap reads to the credit window so
		// we never consume more bytes than we are permitted to forward.
		chunk := tcpFrameMaxBytes
		if c := t.credits.Load(); c < int64(chunk) {
			chunk = int(c)
		}
		if chunk <= 0 {
			continue
		}

		n, readErr := t.conn.Read(buf[:chunk])
		if n > 0 {
			t.lastSeenNs.Store(time.Now().UnixNano())
			t.outboundBytes.Add(int64(n))
			t.credits.Add(-int64(n))

			seq := t.outboundSeq.Add(1)
			frame := &pb.TunnelData{
				TunnelId: t.tunnelID,
				Seq:      seq,
				Data:     append([]byte(nil), buf[:n]...),
			}
			if err := t.transport.SendTunnelData(frame); err != nil {
				log.Warn().Err(err).Str("tunnel_id", t.tunnelID).Msg("tcp tunnel: send TunnelData failed")
				t.finalize(pb.TunnelClose_ERROR, "send failed: "+err.Error())
				return
			}
			if t.maxBytes > 0 && t.outboundBytes.Load()+t.inboundBytes.Load() > t.maxBytes {
				t.finalize(pb.TunnelClose_QUOTA, fmt.Sprintf("tunnel byte cap %d exceeded", t.maxBytes))
				return
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !isClosedConnErr(readErr) {
				log.Debug().Err(readErr).Str("tunnel_id", t.tunnelID).Msg("tcp tunnel: socket read error")
			}
			// Emit a fin frame so the caller knows our send half is closed.
			seq := t.outboundSeq.Add(1)
			finFrame := &pb.TunnelData{TunnelId: t.tunnelID, Seq: seq, Fin: true}
			_ = t.transport.SendTunnelData(finFrame)
			return
		}
	}
}

// waitForCredits blocks until at least one byte of outbound credit is
// available or the tunnel is cancelled. Returns false on cancellation.
func (t *tcpTunnel) waitForCredits() bool {
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
// direction for the configured idle interval. It runs at one quarter of the
// idle period to bound jitter.
func (t *tcpTunnel) idleWatcher() {
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

// handleData consumes a TunnelData frame inbound from the caller, writes its
// payload to the TCP socket, and emits TunnelAcks at the configured cadence.
// fin=true triggers a half-close of the TCP write side.
func (t *tcpTunnel) handleData(frame *pb.TunnelData) {
	if frame == nil {
		return
	}
	t.lastSeenNs.Store(time.Now().UnixNano())
	if len(frame.Data) > 0 {
		if _, err := t.conn.Write(frame.Data); err != nil {
			t.finalize(pb.TunnelClose_ERROR, "socket write: "+err.Error())
			return
		}
		consumed := int64(len(frame.Data))
		t.inboundBytes.Add(consumed)
		t.pendingAckBytes.Add(consumed)
		if t.maxBytes > 0 && t.inboundBytes.Load()+t.outboundBytes.Load() > t.maxBytes {
			t.finalize(pb.TunnelClose_QUOTA, fmt.Sprintf("tunnel byte cap %d exceeded", t.maxBytes))
			return
		}
		if t.pendingAckBytes.Load() >= int64(tcpInboundAckThreshold) {
			t.flushAck()
		}
	}
	if frame.GetFin() {
		t.handleCallerFin()
	}
}

// flushAck sends a TunnelAck granting fresh credits equal to the inbound
// bytes consumed since the previous ack. Resets the pending counter.
func (t *tcpTunnel) flushAck() {
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
		log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("tcp tunnel: send TunnelAck failed")
		// Restore the pending bytes so a future flush retries the grant.
		t.pendingAckBytes.Add(pending)
	}
}

// handleAck applies an inbound TunnelAck from the caller, releasing more
// outbound credits to the socket pump.
func (t *tcpTunnel) handleAck(ack *pb.TunnelAck) {
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
func (t *tcpTunnel) handleClose(closeMsg *pb.TunnelClose) {
	reason := pb.TunnelClose_NORMAL
	detail := ""
	if closeMsg != nil {
		reason = closeMsg.GetReason()
		detail = closeMsg.GetDetail()
	}
	t.finalize(reason, detail)
}

// handleCallerFin processes TunnelData{fin:true} from the caller by
// half-closing the TCP write side. If both halves are now closed we can emit
// TunnelClose{NORMAL}.
func (t *tcpTunnel) handleCallerFin() {
	t.mu.Lock()
	t.callerSentFin = true
	closeWriter := !t.closed
	t.mu.Unlock()
	if closeWriter {
		if cw, ok := t.conn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}
	t.maybeFinishNormal()
}

// markSocketEOF flags that the socket read half is done; once both halves
// have completed we may emit TunnelClose{NORMAL}.
func (t *tcpTunnel) markSocketEOF() {
	t.mu.Lock()
	t.socketEOF = true
	t.mu.Unlock()
	t.maybeFinishNormal()
}

// maybeFinishNormal emits a TunnelClose{NORMAL} when both halves are closed
// and no other terminating reason has been recorded.
func (t *tcpTunnel) maybeFinishNormal() {
	t.mu.Lock()
	ready := t.callerSentFin && t.socketEOF && !t.finishedNormal && !t.closed
	if ready {
		t.finishedNormal = true
	}
	t.mu.Unlock()
	if ready {
		// Drain any remaining inbound credits before announcing close so the
		// caller has accurate accounting.
		t.flushAck()
		t.finalize(pb.TunnelClose_NORMAL, "")
	}
}

// finalize cancels the tunnel context, closes the socket, and emits a
// TunnelClose if one has not already been sent. Safe to call repeatedly.
func (t *tcpTunnel) finalize(reason pb.TunnelClose_Reason, detail string) {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closeReason = reason
		t.closeDetail = detail
		t.closed = true
		t.mu.Unlock()

		t.cancel()
		_ = t.conn.Close()

		closeMsg := &pb.TunnelClose{
			TunnelId: t.tunnelID,
			Reason:   reason,
			Detail:   detail,
		}
		if err := t.transport.SendTunnelClose(closeMsg); err != nil {
			log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("tcp tunnel: send TunnelClose failed")
		}
	})
}

// stop forces the tunnel down. Used by the terminator on shutdown.
func (t *tcpTunnel) stop() {
	t.finalize(pb.TunnelClose_NORMAL, "terminator shutdown")
	t.wg.Wait()
}

// isClosedConnErr reports whether err indicates a closed network connection.
// net.ErrClosed surfaced after Close returns instead of io.EOF.
func isClosedConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}
