package proxysidecar

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/identityheaders"
)

const (
	// udpDefaultMaxDatagramBytes is the default MTU cap for a single UDP
	// TunnelData frame. Sized conservatively to fit within typical Ethernet MTU
	// minus IP/UDP headers.
	udpDefaultMaxDatagramBytes = 1400
)

// udpDialer abstracts net.DialUDP so tests can inject a synthetic backend.
type udpDialer func(ctx context.Context, address string) (*net.UDPConn, error)

// defaultUDPDialer resolves and dials the address as a connected UDP socket.
func defaultUDPDialer(_ context.Context, address string) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve UDP %s: %w", address, err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial UDP %s: %w", address, err)
	}
	return conn, nil
}

// udpBackend evaluates a TunnelOpen against config and stands up a per-tunnel
// datagram bridge connecting a local UDP service to the caller via tunnelTransport.
type udpBackend struct {
	cfg            BackendConfig
	dialer         udpDialer
	targetResolver tunnelTargetResolver
}

// newUDPBackend constructs a UDP backend from cfg. dialer may be nil to use
// the default net.DialUDP dialer.
func newUDPBackend(cfg BackendConfig, dialer udpDialer) *udpBackend {
	if dialer == nil {
		dialer = defaultUDPDialer
	}
	return &udpBackend{cfg: cfg, dialer: dialer}
}

// open dials the backend and starts the receive pump goroutine for the tunnel
// described by the open frame. The returned udpTunnel exposes handleData for
// dispatching inbound TunnelData frames from the caller.
func (b *udpBackend) open(ctx context.Context, open *pb.TunnelOpen, transport tunnelTransport) (*udpTunnel, error) {
	address, err := resolveUDPAddress(b.cfg, open.GetRemoteHint())
	if err != nil {
		return nil, err
	}

	if err := enforceTunnelTargetScope(b.targetResolver, open, b.cfg.Name, identityheaders.TunnelProtocolUDP, open.GetRemoteHint()); err != nil {
		return nil, err
	}

	conn, err := b.dialer(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("dial UDP %s: %w", address, err)
	}

	maxBytes := b.cfg.MaxBytes
	if open.GetMaxBytes() > 0 && (maxBytes == 0 || open.GetMaxBytes() < maxBytes) {
		maxBytes = open.GetMaxBytes()
	}

	idle := time.Duration(b.cfg.IdleTimeoutMs) * time.Millisecond
	if open.GetIdleTimeoutMs() > 0 {
		callerIdle := time.Duration(open.GetIdleTimeoutMs()) * time.Millisecond
		if callerIdle < idle || idle <= 0 {
			idle = callerIdle
		}
	}
	if idle <= 0 {
		idle = time.Minute
	}

	maxDatagram := b.cfg.MaxDatagramBytes
	if maxDatagram <= 0 {
		maxDatagram = udpDefaultMaxDatagramBytes
	}

	tunCtx, cancel := context.WithCancel(ctx)
	t := &udpTunnel{
		tunnelID:    open.GetTunnelId(),
		conn:        conn,
		transport:   transport,
		ctx:         tunCtx,
		cancel:      cancel,
		idle:        idle,
		maxBytes:    maxBytes,
		maxDatagram: maxDatagram,
		lastSeenNs:  atomic.Int64{},
	}
	t.lastSeenNs.Store(time.Now().UnixNano())

	t.wg.Add(2)
	go t.pumpSocketToCaller()
	go t.idleWatcher()
	return t, nil
}

// resolveUDPAddress picks the dial target for a UDP tunnel. Mirrors
// resolveTCPAddress but strips udp:// scheme.
func resolveUDPAddress(cfg BackendConfig, hint string) (string, error) {
	defaultAddr := stripUDPScheme(cfg.URL)
	if hint == "" {
		if defaultAddr == "" {
			return "", fmt.Errorf("no remote_hint and backend has no default url")
		}
		return defaultAddr, nil
	}
	if len(cfg.AllowRemoteHints) == 0 {
		if defaultAddr == "" {
			return "", fmt.Errorf("backend has no default url and remote_hint is not allow-listed")
		}
		return defaultAddr, nil
	}
	for _, pattern := range cfg.AllowRemoteHints {
		if matchHint(pattern, hint) {
			return stripUDPScheme(hint), nil
		}
	}
	return "", fmt.Errorf("remote_hint %q does not match any allow_remote_hints pattern", hint)
}

// stripUDPScheme normalises udp://host:port to host:port.
func stripUDPScheme(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "udp://") {
		return strings.TrimPrefix(s, "udp://")
	}
	return s
}

// udpTunnel is the per-tunnel state for an active UDP backend bridge. Unlike
// TCP tunnels there is no stream model: each TunnelData frame is exactly one
// UDP datagram. No flow control credits are tracked for UDP.
type udpTunnel struct {
	tunnelID  string
	conn      *net.UDPConn
	transport tunnelTransport

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	idle        time.Duration
	maxBytes    int64
	maxDatagram int

	inboundBytes  atomic.Int64
	outboundBytes atomic.Int64

	lastSeenNs atomic.Int64

	closeOnce   sync.Once
	closeReason pb.TunnelClose_Reason
	closeDetail string

	mu     sync.Mutex
	closed bool
}

// pumpSocketToCaller reads datagrams from the UDP socket and ships each one
// as a separate TunnelData frame. Each Read call returns exactly one datagram
// (net.UDPConn semantics), so boundary preservation is implicit.
func (t *udpTunnel) pumpSocketToCaller() {
	defer t.wg.Done()

	// Buffer sized to the maximum datagram we'd ever accept from the backend.
	// Any backend datagram larger than this would be truncated by the OS — we
	// size the buffer at 64 KiB (max UDP payload) so nothing is silently lost.
	buf := make([]byte, 65535)
	for {
		if t.ctx.Err() != nil {
			return
		}

		// Apply a read deadline so we can check cancellation periodically.
		_ = t.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := t.conn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if isClosedConnErr(err) {
				return
			}
			log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("udp tunnel: socket read error")
			t.finalize(pb.TunnelClose_ERROR, "socket read: "+err.Error())
			return
		}
		if n == 0 {
			continue
		}

		t.lastSeenNs.Store(time.Now().UnixNano())
		t.outboundBytes.Add(int64(n))

		frame := &pb.TunnelData{
			TunnelId: t.tunnelID,
			Data:     append([]byte(nil), buf[:n]...),
		}
		if err := t.transport.SendTunnelData(frame); err != nil {
			log.Warn().Err(err).Str("tunnel_id", t.tunnelID).Msg("udp tunnel: send TunnelData failed")
			t.finalize(pb.TunnelClose_ERROR, "send failed: "+err.Error())
			return
		}

		if t.maxBytes > 0 && t.outboundBytes.Load()+t.inboundBytes.Load() > t.maxBytes {
			t.finalize(pb.TunnelClose_QUOTA, fmt.Sprintf("tunnel byte cap %d exceeded", t.maxBytes))
			return
		}
	}
}

// idleWatcher fires TunnelClose{IDLE_TIMEOUT} if no traffic flows for the
// configured idle interval.
func (t *udpTunnel) idleWatcher() {
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

// handleData consumes a TunnelData frame from the caller and sends it as a
// single UDP datagram. Frames larger than maxDatagram are rejected with
// TunnelClose{ERROR}. fin=true closes the tunnel immediately (UDP has no
// half-close concept).
func (t *udpTunnel) handleData(frame *pb.TunnelData) {
	if frame == nil {
		return
	}
	if frame.GetFin() {
		t.finalize(pb.TunnelClose_NORMAL, "caller sent fin")
		return
	}
	if len(frame.Data) == 0 {
		return
	}
	if len(frame.Data) > t.maxDatagram {
		t.finalize(pb.TunnelClose_ERROR,
			fmt.Sprintf("datagram exceeds MTU: %d > %d", len(frame.Data), t.maxDatagram))
		return
	}

	t.lastSeenNs.Store(time.Now().UnixNano())

	if _, err := t.conn.Write(frame.Data); err != nil {
		t.finalize(pb.TunnelClose_ERROR, "socket write: "+err.Error())
		return
	}
	consumed := int64(len(frame.Data))
	t.inboundBytes.Add(consumed)

	if t.maxBytes > 0 && t.inboundBytes.Load()+t.outboundBytes.Load() > t.maxBytes {
		t.finalize(pb.TunnelClose_QUOTA, fmt.Sprintf("tunnel byte cap %d exceeded", t.maxBytes))
	}
}

// handleClose terminates the tunnel locally. Always idempotent.
func (t *udpTunnel) handleClose(closeMsg *pb.TunnelClose) {
	reason := pb.TunnelClose_NORMAL
	detail := ""
	if closeMsg != nil {
		reason = closeMsg.GetReason()
		detail = closeMsg.GetDetail()
	}
	t.finalize(reason, detail)
}

// handleAck is a no-op for UDP: UDP has no stream flow control.
func (t *udpTunnel) handleAck(_ *pb.TunnelAck) {}

// finalize cancels the tunnel context, closes the socket, and emits a
// TunnelClose if one has not already been sent. Safe to call repeatedly.
func (t *udpTunnel) finalize(reason pb.TunnelClose_Reason, detail string) {
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
			log.Debug().Err(err).Str("tunnel_id", t.tunnelID).Msg("udp tunnel: send TunnelClose failed")
		}
	})
}

// stop forces the tunnel down. Used by the terminator on shutdown.
func (t *udpTunnel) stop() {
	t.finalize(pb.TunnelClose_NORMAL, "terminator shutdown")
	t.wg.Wait()
}

// id returns the tunnel's identifier; satisfies activeTunnel.
func (t *udpTunnel) id() string { return t.tunnelID }

// storeInboundSeq is a no-op for UDP since UDP datagrams have no sequence
// ordering guarantee; satisfies activeTunnel.
func (t *udpTunnel) storeInboundSeq(_ uint32) {}
