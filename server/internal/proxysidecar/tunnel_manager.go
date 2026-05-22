package proxysidecar

import (
	"sync"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
)

// pendingTunnelMaxFrames bounds the number of pre-dial frames buffered
// per tunnel. Sized to roughly 32 MiB at the SDK's 256 KiB max frame
// size — enough to cover a worst-case 10s dial of an SDK-side caller
// that immediately starts pushing data, but capped so a stalled dial
// can't consume unbounded memory.
const pendingTunnelMaxFrames = 128

// pendingFrame is a single buffered envelope held by pendingTunnel
// during the dial-in-flight window. Exactly one payload field is set.
type pendingFrame struct {
	data     *pb.TunnelData
	ack      *pb.TunnelAck
	closeMsg *pb.TunnelClose
}

// pendingTunnel is a placeholder activeTunnel inserted into the
// tunnelManager at the moment a TunnelOpen header is received on the
// receive loop, before the backend dial completes on its own goroutine.
//
// It satisfies activeTunnel so that the existing downstreamRouter /
// HandleTunnelData / HandleTunnelAck / HandleTunnelClose paths route
// pre-dial frames into the placeholder unchanged. Frames are buffered
// in arrival order; activate flushes them into the real tunnel once
// dialed.
//
// Without this placeholder, an in-process or low-latency runtime where
// the SDK's first TunnelData frame arrives nanoseconds after the
// TunnelOpen could race the dial goroutine and drop data with
// "no matching tunnel" — the bug exposed by the e2e harness when the
// fake gateway forwards frames back-to-back.
type pendingTunnel struct {
	tunnelID string

	mu      sync.Mutex
	real    activeTunnel   // nil until activate
	failed  bool           // true after stop / pre-activate Close — buffered frames discarded
	pending []pendingFrame // buffered in arrival order
	capped  bool           // true after first overflow, so we warn once
}

func (p *pendingTunnel) id() string { return p.tunnelID }

func (p *pendingTunnel) handleData(d *pb.TunnelData) {
	p.mu.Lock()
	if p.real != nil {
		real := p.real
		p.mu.Unlock()
		real.handleData(d)
		return
	}
	if p.failed {
		p.mu.Unlock()
		return
	}
	if len(p.pending) >= pendingTunnelMaxFrames {
		warn := !p.capped
		p.capped = true
		p.mu.Unlock()
		if warn {
			log.Warn().Str("tunnel_id", p.tunnelID).Int("limit", pendingTunnelMaxFrames).Msg("pending tunnel buffer full, dropping further pre-dial data frames")
		}
		return
	}
	p.pending = append(p.pending, pendingFrame{data: d})
	p.mu.Unlock()
}

func (p *pendingTunnel) handleAck(a *pb.TunnelAck) {
	p.mu.Lock()
	if p.real != nil {
		real := p.real
		p.mu.Unlock()
		real.handleAck(a)
		return
	}
	if p.failed {
		p.mu.Unlock()
		return
	}
	if len(p.pending) >= pendingTunnelMaxFrames {
		p.mu.Unlock()
		return
	}
	p.pending = append(p.pending, pendingFrame{ack: a})
	p.mu.Unlock()
}

func (p *pendingTunnel) handleClose(c *pb.TunnelClose) {
	p.mu.Lock()
	if p.real != nil {
		real := p.real
		p.mu.Unlock()
		real.handleClose(c)
		return
	}
	// Close arrived before activate. Mark failed so activate stops the
	// dialed tunnel immediately and buffered frames don't replay into
	// a connection the caller already cancelled.
	p.failed = true
	p.pending = nil
	p.mu.Unlock()
}

func (p *pendingTunnel) storeInboundSeq(seq uint32) {
	p.mu.Lock()
	if p.real != nil {
		real := p.real
		p.mu.Unlock()
		real.storeInboundSeq(seq)
		return
	}
	// Pre-activate seq tracking is a no-op; seq is monotonically
	// derived from buffered data frames and the real tunnel will
	// re-derive it as those frames are flushed.
	p.mu.Unlock()
}

func (p *pendingTunnel) stop() {
	p.mu.Lock()
	real := p.real
	p.failed = true
	p.pending = nil
	p.mu.Unlock()
	if real != nil {
		real.stop()
	}
}

// activate swaps the real tunnel in and flushes buffered frames in
// arrival order. Returns true on success; returns false when the
// placeholder was already marked failed (via stop or a pre-activate
// TunnelClose) — in that case the caller is responsible for stopping
// the dialed tunnel.
func (p *pendingTunnel) activate(real activeTunnel) bool {
	p.mu.Lock()
	if p.failed {
		p.mu.Unlock()
		return false
	}
	p.real = real
	buffered := p.pending
	p.pending = nil
	p.mu.Unlock()
	for _, f := range buffered {
		switch {
		case f.data != nil:
			real.handleData(f.data)
		case f.ack != nil:
			real.handleAck(f.ack)
		case f.closeMsg != nil:
			real.handleClose(f.closeMsg)
		}
	}
	return true
}

// activeTunnel is the minimal contract a per-tunnel pump must satisfy to be
// addressable through the tunnelManager. Both tcpTunnel and wsTunnel
// implement it; the manager dispatches inbound TunnelData/TunnelAck/
// TunnelClose envelopes through this interface so additional backend kinds
// can be slotted in without changing the routing layer.
type activeTunnel interface {
	id() string
	handleData(*pb.TunnelData)
	handleAck(*pb.TunnelAck)
	handleClose(*pb.TunnelClose)
	storeInboundSeq(uint32)
	stop()
}

// id returns the tunnel's identifier; satisfies activeTunnel.
func (t *tcpTunnel) id() string { return t.tunnelID }

// storeInboundSeq records the latest inbound sequence number observed; the
// terminator routes inbound frames through here before invoking handleData.
func (t *tcpTunnel) storeInboundSeq(seq uint32) { t.inboundSeq.Store(seq) }

// tunnelManager registers active tunnels keyed by tunnel_id and routes
// inbound DownstreamMessage{TunnelData/TunnelAck/TunnelClose} envelopes to
// the matching tunnel. The terminator owns one manager and consults it on
// every downstream tunnel envelope it receives from the gateway.
type tunnelManager struct {
	mu      sync.RWMutex
	tunnels map[string]activeTunnel
}

func newTunnelManager() *tunnelManager {
	return &tunnelManager{tunnels: make(map[string]activeTunnel)}
}

// register inserts t as the active tunnel for t.id(). When a
// pendingTunnel placeholder previously reserved by the receive loop is
// present, register swaps it for t and flushes any pre-dial frames the
// placeholder buffered. Returns false if a non-placeholder tunnel
// already exists with the same id (genuine duplicate) or if the
// placeholder was cancelled mid-dial (peer sent TunnelClose before the
// dial completed) — in either false case the caller should stop t to
// release its goroutines.
//
// Direct callers (test scaffolding that bypasses the receive-loop
// reserve path) still get the original "insert or fail on duplicate"
// behaviour because the no-entry branch fires first.
func (m *tunnelManager) register(t activeTunnel) bool {
	m.mu.Lock()
	entry, exists := m.tunnels[t.id()]
	if !exists {
		m.tunnels[t.id()] = t
		m.mu.Unlock()
		return true
	}
	pending, isPending := entry.(*pendingTunnel)
	if !isPending {
		m.mu.Unlock()
		return false
	}
	m.tunnels[t.id()] = t
	m.mu.Unlock()
	return pending.activate(t)
}

// reserve atomically inserts a pendingTunnel placeholder for tunnelID
// so that subsequent inbound TunnelData/Ack/Close frames route into
// the placeholder's pre-dial buffer instead of being dropped while the
// async dial goroutine is still running. Returns the placeholder or
// nil if an entry with that id already exists (caller should reject
// the open as a duplicate).
//
// Called synchronously on the receive loop before spawning the dial
// goroutine; the goroutine later calls activate to swap the placeholder
// for the real tunnel and flush buffered frames.
func (m *tunnelManager) reserve(tunnelID string) *pendingTunnel {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.tunnels[tunnelID]; exists {
		return nil
	}
	p := &pendingTunnel{tunnelID: tunnelID}
	m.tunnels[tunnelID] = p
	return p
}

// unregister drops the entry without stopping the tunnel.
func (m *tunnelManager) unregister(tunnelID string) {
	m.mu.Lock()
	delete(m.tunnels, tunnelID)
	m.mu.Unlock()
}

// get returns the tunnel for tunnelID or nil.
func (m *tunnelManager) get(tunnelID string) activeTunnel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tunnels[tunnelID]
}

// stopAll forces every active tunnel down. Used during shutdown.
func (m *tunnelManager) stopAll() {
	m.mu.Lock()
	tunnels := make([]activeTunnel, 0, len(m.tunnels))
	for _, t := range m.tunnels {
		tunnels = append(tunnels, t)
	}
	m.tunnels = make(map[string]activeTunnel)
	m.mu.Unlock()
	for _, t := range tunnels {
		t.stop()
	}
}

// selectTCPBackend returns the TCP backend that should handle open. When the
// caller specifies BackendName the named backend is consulted directly and
// must still admit the remote_hint via its allow-list — explicit naming does
// not bypass ACL. Otherwise the first backend whose allow-list admits the
// hint is returned.
func (t *Terminator) selectTCPBackend(open *pb.TunnelOpen) *tcpBackend {
	hint := open.GetRemoteHint()
	t.backendMu.RLock()
	backends := t.tcpBackends
	t.backendMu.RUnlock()
	if name := open.GetBackendName(); name != "" {
		for _, b := range backends {
			if b.cfg.Name != name {
				continue
			}
			if _, err := resolveTCPAddress(b.cfg, hint); err != nil {
				return nil
			}
			return b
		}
		return nil
	}
	for _, b := range backends {
		if _, err := resolveTCPAddress(b.cfg, hint); err == nil {
			return b
		}
	}
	return nil
}

// selectWSBackend returns the WS backend that should handle open. Mirrors
// selectTCPBackend's BackendName + allow-list semantics.
func (t *Terminator) selectWSBackend(open *pb.TunnelOpen) *wsBackend {
	hint := open.GetRemoteHint()
	t.backendMu.RLock()
	backends := t.wsBackends
	t.backendMu.RUnlock()
	if name := open.GetBackendName(); name != "" {
		for _, b := range backends {
			if b.cfg.Name != name {
				continue
			}
			if _, err := resolveWSAddress(b.cfg, hint); err != nil {
				return nil
			}
			return b
		}
		return nil
	}
	for _, b := range backends {
		if _, err := resolveWSAddress(b.cfg, hint); err == nil {
			return b
		}
	}
	return nil
}

// selectUDPBackend returns the UDP backend that should handle open. Mirrors
// selectTCPBackend's BackendName + allow-list semantics.
func (t *Terminator) selectUDPBackend(open *pb.TunnelOpen) *udpBackend {
	hint := open.GetRemoteHint()
	t.backendMu.RLock()
	backends := t.udpBackends
	t.backendMu.RUnlock()
	if name := open.GetBackendName(); name != "" {
		for _, b := range backends {
			if b.cfg.Name != name {
				continue
			}
			if _, err := resolveUDPAddress(b.cfg, hint); err != nil {
				return nil
			}
			return b
		}
		return nil
	}
	for _, b := range backends {
		if _, err := resolveUDPAddress(b.cfg, hint); err == nil {
			return b
		}
	}
	return nil
}
