package proxysidecar

import (
	"sync"

	pb "github.com/scitrera/aether/api/proto"
)

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

// register adds a tunnel to the manager. Returns false if a tunnel with the
// same id is already registered (the caller should reject the duplicate).
func (m *tunnelManager) register(t activeTunnel) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.tunnels[t.id()]; exists {
		return false
	}
	m.tunnels[t.id()] = t
	return true
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
