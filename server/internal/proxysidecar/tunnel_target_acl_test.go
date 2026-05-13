package proxysidecar

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

// resolverFor returns a fixed-pattern tunnelTargetResolver for tests.
func resolverFor(patterns []string) tunnelTargetResolver {
	return func(*pb.TunnelOpen) []string { return patterns }
}

// TestTCPTunnelTargetScopeAllow verifies that a grant whose tunnel_target
// scope admits the chosen backend, protocol, and remote_hint allows dial.
func TestTCPTunnelTargetScopeAllow(t *testing.T) {
	_, addr := echoServer(t)

	cfg := BackendConfig{
		Name:             "db-prod",
		Kind:             BackendKindTCP,
		URL:              addr,
		AllowRemoteHints: []string{addr},
	}
	b := newTCPBackend(cfg, defaultTCPDialer)
	b.targetResolver = resolverFor([]string{"db-prod::tcp " + addr})

	tt, err := b.open(context.Background(), &pb.TunnelOpen{
		TunnelId:   "t1",
		Protocol:   pb.TunnelOpen_TCP,
		RemoteHint: addr,
	}, newFakeTransport())
	if err != nil {
		t.Fatalf("expected open to succeed, got %v", err)
	}
	tt.stop()
}

// TestTCPTunnelTargetScopeDenyBackend ensures a grant tied to a specific
// backend rejects opens against a different backend, regardless of remote.
func TestTCPTunnelTargetScopeDenyBackend(t *testing.T) {
	_, addr := echoServer(t)

	cfg := BackendConfig{
		Name:             "db-staging",
		Kind:             BackendKindTCP,
		URL:              addr,
		AllowRemoteHints: []string{addr},
	}
	b := newTCPBackend(cfg, defaultTCPDialer)
	b.targetResolver = resolverFor([]string{"db-prod::tcp " + addr})

	_, err := b.open(context.Background(), &pb.TunnelOpen{
		TunnelId:   "t1",
		Protocol:   pb.TunnelOpen_TCP,
		RemoteHint: addr,
	}, newFakeTransport())
	if !errors.Is(err, errTunnelTargetScopeDenied) {
		t.Fatalf("expected tunnel_target_scope_denied, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "tunnel_target_scope_denied") {
		t.Fatalf("error must surface sentinel string, got %v", err)
	}
}

// TestUDPTunnelTargetScopeDenyProtocol ensures a tcp-only grant rejects a
// udp tunnel even when the backend name and remote_hint would otherwise
// match.
func TestUDPTunnelTargetScopeDenyProtocol(t *testing.T) {
	_, addr := udpEchoServer(t)

	cfg := BackendConfig{
		Name:             "db-prod",
		Kind:             BackendKindUDP,
		URL:              addr,
		AllowRemoteHints: []string{addr},
		MaxDatagramBytes: udpDefaultMaxDatagramBytes,
	}
	b := newUDPBackend(cfg, defaultUDPDialer)
	b.targetResolver = resolverFor([]string{"db-prod::tcp " + addr})

	_, err := b.open(context.Background(), &pb.TunnelOpen{
		TunnelId:   "t1",
		Protocol:   pb.TunnelOpen_UDP,
		RemoteHint: addr,
	}, newFakeTransport())
	if !errors.Is(err, errTunnelTargetScopeDenied) {
		t.Fatalf("expected tunnel_target_scope_denied, got %v", err)
	}
}

// TestTCPTunnelTargetScopeBlanketWhenAbsent confirms grants without a
// tunnel_target scope (resolver returns nil) keep the legacy blanket-allow
// behaviour.
func TestTCPTunnelTargetScopeBlanketWhenAbsent(t *testing.T) {
	_, addr := echoServer(t)

	cfg := BackendConfig{
		Name:             "db-prod",
		Kind:             BackendKindTCP,
		URL:              addr,
		AllowRemoteHints: []string{addr},
	}
	b := newTCPBackend(cfg, defaultTCPDialer)
	b.targetResolver = resolverFor(nil)

	tt, err := b.open(context.Background(), &pb.TunnelOpen{
		TunnelId:   "t1",
		Protocol:   pb.TunnelOpen_TCP,
		RemoteHint: addr,
	}, newFakeTransport())
	if err != nil {
		t.Fatalf("expected open to succeed with empty scope, got %v", err)
	}
	tt.stop()
}

// TestWSTunnelTargetScopeWildcard exercises a glob pattern that admits the
// chosen ws backend and remote hint via cross-segment globbing.
func TestWSTunnelTargetScopeWildcard(t *testing.T) {
	wsURL := wsEchoServer(t, nil)

	cfg := BackendConfig{
		Name:             "ws-edge",
		Kind:             BackendKindWS,
		URL:              wsURL,
		AllowRemoteHints: []string{wsURL},
	}
	b := newWSBackend(cfg, nil)
	// "*" by itself is the documented blanket-allow shorthand even when
	// the backend or remote_hint contain slashes that path.Match would
	// otherwise refuse to span.
	b.targetResolver = resolverFor([]string{"*"})

	tt, err := b.open(context.Background(), &pb.TunnelOpen{
		TunnelId:   "t1",
		Protocol:   pb.TunnelOpen_WEBSOCKET,
		RemoteHint: wsURL,
	}, newFakeTransport())
	if err != nil {
		t.Fatalf("expected ws open to succeed under wildcard scope, got %v", err)
	}
	tt.stop()
}

// TestWSTunnelTargetScopeDeny ensures a tcp-only grant rejects WS opens.
func TestWSTunnelTargetScopeDeny(t *testing.T) {
	wsURL := wsEchoServer(t, nil)

	cfg := BackendConfig{
		Name:             "ws-edge",
		Kind:             BackendKindWS,
		URL:              wsURL,
		AllowRemoteHints: []string{wsURL},
	}
	b := newWSBackend(cfg, nil)
	b.targetResolver = resolverFor([]string{"db-prod::tcp prod-*:5432"})

	_, err := b.open(context.Background(), &pb.TunnelOpen{
		TunnelId:   "t1",
		Protocol:   pb.TunnelOpen_WEBSOCKET,
		RemoteHint: wsURL,
	}, newFakeTransport())
	if !errors.Is(err, errTunnelTargetScopeDenied) {
		t.Fatalf("expected tunnel_target_scope_denied, got %v", err)
	}
}
