package proxysidecar

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// =============================================================================
// aggregator test harness.
//
// Two registered servers / two listeners (template: relaymux_test.go:214-239):
// one Aggregator instance backs BOTH the AetherGateway provider surface and the
// SandboxRelayTunnel relay surface, each on its own TCP listener. A fakeProvider
// dials the provider surface (sending x-aether-tenant metadata); a fakeRelay
// dials the tunnel surface (sending TunnelHello). For the pairing/splice/watch
// tests the surfaces run over insecure transport and the Aggregator's peerCN is
// stubbed to a fixed CN so the tenant resolves without real certs; one dedicated
// test (TestAggregator_CNValidation) exercises the real mTLS CN-binding path.
// =============================================================================

type aggHarness struct {
	t            *testing.T
	agg          *Aggregator
	providerAddr string
	tunnelAddr   string
	providerSrv  *grpc.Server
	tunnelSrv    *grpc.Server
	conns        []*grpc.ClientConn

	// providerCN, when non-empty, overrides the CN the stubbed peerCN returns on
	// the PROVIDER surface (/…AetherGateway/Connect) while the tunnel surface
	// keeps tunnelCN. This lets a test give the provider a CN that encodes NO
	// tenant (forcing the metadata-only pairing path) while the relay still
	// presents a valid tenant-encoding CN. Set BEFORE any connect.
	providerCN string
	tunnelCN   string
}

// newAggHarness builds an insecure two-listener harness. stubCN is the CN the
// stubbed peerCN returns on BOTH surfaces by default. A test may set
// h.providerCN before connecting to give the provider surface a different CN
// (e.g. one that encodes no tenant) — see surfaceCN below.
func newAggHarness(t *testing.T, cfg *Config, stubCN string) *aggHarness {
	t.Helper()

	provLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen provider: %v", err)
	}
	tunLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tunnel: %v", err)
	}

	cfg.Aggregator.Enabled = true
	cfg.Aggregator.ProviderListen = "tcp://" + provLis.Addr().String()
	cfg.Aggregator.TunnelListen = "tcp://" + tunLis.Addr().String()
	cfg.Aggregator.ProviderTLS.Insecure = true
	cfg.Aggregator.TunnelTLS.Insecure = true
	if cfg.Aggregator.PairWaitTimeoutMs == 0 {
		cfg.Aggregator.PairWaitTimeoutMs = 30_000
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate cfg: %v", err)
	}

	agg, err := NewAggregator(cfg)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}

	h := &aggHarness{
		t:            t,
		agg:          agg,
		providerAddr: provLis.Addr().String(),
		tunnelAddr:   tunLis.Addr().String(),
		tunnelCN:     stubCN,
	}
	// Stub the peer-cert CN so the insecure surfaces resolve a tenant. The CN is
	// chosen per surface (see surfaceCN) so a test can give the provider hop a
	// tenant-less CN while the relay hop keeps a tenant-encoding one.
	agg.peerCN = func(ctx context.Context) (string, error) { return h.surfaceCN(ctx), nil }

	provSrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(provSrv, agg)
	go func() { _ = provSrv.Serve(provLis) }()

	tunSrv := grpc.NewServer()
	pb.RegisterSandboxRelayTunnelServer(tunSrv, agg)
	go func() { _ = tunSrv.Serve(tunLis) }()

	h.providerSrv = provSrv
	h.tunnelSrv = tunSrv
	t.Cleanup(func() {
		for _, c := range h.conns {
			_ = c.Close()
		}
		provSrv.Stop()
		tunSrv.Stop()
	})
	return h
}

// surfaceCN returns the CN the stubbed peerCN reports for the calling surface.
// The provider surface (/…AetherGateway/…) reports providerCN when a test has
// set it (e.g. a tenant-less shared CN to force the metadata-only path) and
// otherwise tunnelCN; every other surface (the relay tunnel + watch) reports
// tunnelCN. grpc.Method reads the full method off the server-side stream ctx.
func (h *aggHarness) surfaceCN(ctx context.Context) string {
	method, _ := grpc.Method(ctx)
	if strings.Contains(method, "AetherGateway") && h.providerCN != "" {
		return h.providerCN
	}
	return h.tunnelCN
}

func (h *aggHarness) dial(addr string) *grpc.ClientConn {
	h.t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatalf("dial %s: %v", addr, err)
	}
	h.conns = append(h.conns, conn)
	return conn
}

// providerConnect opens an AetherGateway.Connect with the x-aether-tenant hint.
func (h *aggHarness) providerConnect(ctx context.Context, tenant string) pb.AetherGateway_ConnectClient {
	h.t.Helper()
	conn := h.dial(h.providerAddr)
	ctx = metadata.AppendToOutgoingContext(ctx, metadataTenantKey, tenant)
	stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx)
	if err != nil {
		h.t.Fatalf("provider connect: %v", err)
	}
	return stream
}

// relayTunnel opens a SandboxRelayTunnel.Tunnel and sends the opening hello.
func (h *aggHarness) relayTunnel(ctx context.Context, tenant string) pb.SandboxRelayTunnel_TunnelClient {
	h.t.Helper()
	conn := h.dial(h.tunnelAddr)
	stream, err := pb.NewSandboxRelayTunnelClient(conn).Tunnel(ctx)
	if err != nil {
		h.t.Fatalf("relay tunnel: %v", err)
	}
	if err := stream.Send(&pb.TunnelFrame{F: &pb.TunnelFrame_Hello{Hello: &pb.TunnelHello{Tenant: tenant}}}); err != nil {
		h.t.Fatalf("relay hello: %v", err)
	}
	return stream
}

func (h *aggHarness) watchTenants(ctx context.Context) grpc.ServerStreamingClient[pb.TenantEvent] {
	h.t.Helper()
	// WatchTenants lives on the SandboxRelayTunnel service → dial the tunnel
	// listener, not the provider (AetherGateway) one.
	conn := h.dial(h.tunnelAddr)
	w, err := pb.NewSandboxRelayTunnelClient(conn).WatchTenants(ctx, &pb.WatchTenantsRequest{})
	if err != nil {
		h.t.Fatalf("watch tenants: %v", err)
	}
	return w
}

// =============================================================================
// 1. Pairing by tenant + 1:1 splice fidelity, BOTH directions.
// =============================================================================

func TestAggregator_PairAndSpliceBothDirections(t *testing.T) {
	const tenant = "tenant-alice"
	h := newAggHarness(t, &Config{}, "sv::sandbox-provider::"+tenant)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Both sides connect; pairing is by tenant.
	provider := h.providerConnect(ctx, tenant)
	relay := h.relayTunnel(ctx, tenant)

	// provider→relay: an UpstreamMessage sent by the provider must arrive
	// identical as TunnelFrame.up at the relay.
	up := &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{
		ResumeSessionId: "resume-xyz",
		ClientType: &pb.InitConnection_Service{Service: &pb.ServiceIdentity{
			Implementation: "sandbox-provider", Specifier: "pod-7",
		}},
	}}}
	if err := provider.Send(up); err != nil {
		t.Fatalf("provider send up: %v", err)
	}
	gotFrame, err := relay.Recv()
	if err != nil {
		t.Fatalf("relay recv: %v", err)
	}
	gotUp := gotFrame.GetUp()
	if gotUp == nil {
		t.Fatalf("relay got frame %T, want up", gotFrame.GetF())
	}
	if gotUp.GetInit().GetResumeSessionId() != "resume-xyz" {
		t.Fatalf("up resume_session_id = %q, want resume-xyz", gotUp.GetInit().GetResumeSessionId())
	}
	if gotUp.GetInit().GetService().GetSpecifier() != "pod-7" {
		t.Fatalf("up specifier = %q, want pod-7", gotUp.GetInit().GetService().GetSpecifier())
	}

	// relay→provider: a TunnelFrame.down sent by the relay must arrive identical
	// as a DownstreamMessage at the provider.
	down := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_ConnectionAck{
		ConnectionAck: &pb.ConnectionAck{SessionId: "gw-session-1", AssignedId: "sv::sandbox-provider::pod-7"},
	}}
	if err := relay.Send(&pb.TunnelFrame{F: &pb.TunnelFrame_Down{Down: down}}); err != nil {
		t.Fatalf("relay send down: %v", err)
	}
	gotDown, err := provider.Recv()
	if err != nil {
		t.Fatalf("provider recv: %v", err)
	}
	if gotDown.GetConnectionAck().GetSessionId() != "gw-session-1" {
		t.Fatalf("down session_id = %q, want gw-session-1", gotDown.GetConnectionAck().GetSessionId())
	}
	if gotDown.GetConnectionAck().GetAssignedId() != "sv::sandbox-provider::pod-7" {
		t.Fatalf("down assigned_id = %q, want sv::sandbox-provider::pod-7", gotDown.GetConnectionAck().GetAssignedId())
	}
}

// Provider presents a generic shared cert whose CN does NOT encode a tenant; the
// x-aether-tenant metadata supplies the pairing key. The relay's per-tenant CN
// is still the authority on its side.
//
// This genuinely drives the metadata-only branch: the provider surface reports a
// CN with NO "::" tenant segment ("sandbox-provider-shared"), so tenantFromCN
// returns "" on the provider hop and the cnTenant=="" path forces the pairing
// key to come from metadata. The relay surface still reports a valid
// tenant-encoding CN. (The earlier version of this test gave BOTH surfaces a
// tenant-encoding CN, so the CN branch was taken and the metadata-only path was
// never exercised — a false positive.)
func TestAggregator_ProviderMetadataOnlyPairs(t *testing.T) {
	const tenant = "tenant-bravo"
	h := newAggHarness(t, &Config{}, "sv::sandbox-provider::"+tenant)
	// Provider hop presents a shared CN that encodes NO tenant → tenantFromCN==""
	// → the x-aether-tenant metadata is the only pairing key.
	h.providerCN = "sandbox-provider-shared"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	provider := h.providerConnect(ctx, tenant)
	relay := h.relayTunnel(ctx, tenant)

	if err := provider.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{}}}); err != nil {
		t.Fatalf("provider send: %v", err)
	}
	if _, err := relay.Recv(); err != nil {
		t.Fatalf("relay recv (metadata-only pairing did not happen): %v", err)
	}
}

// Sibling of the metadata-only case: with RequireTenantMetadata=true (the
// default) AND a provider CN that encodes no tenant, a Connect that omits the
// x-aether-tenant metadata header has no way to resolve a tenant and MUST be
// rejected (rather than silently parking or pairing the wrong tenant).
func TestAggregator_ProviderMissingMetadataRejected(t *testing.T) {
	requireMeta := true
	cfg := &Config{}
	cfg.Aggregator.RequireTenantMetadata = &requireMeta
	h := newAggHarness(t, cfg, "sv::sandbox-provider::tenant-bravo")
	// Tenant-less provider CN: metadata is the only possible pairing key, and it
	// is absent → reject.
	h.providerCN = "sandbox-provider-shared"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect WITHOUT the x-aether-tenant metadata header.
	conn := h.dial(h.providerAddr)
	stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("provider connect: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected provider Connect to be rejected when metadata is required but absent")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("provider stream did not close on missing required metadata")
	}
}

// =============================================================================
// 2. WatchTenants emits online when a relay registers and offline when it goes.
// =============================================================================

func TestAggregator_WatchTenantsOnlineOffline(t *testing.T) {
	const tenant = "tenant-charlie"
	h := newAggHarness(t, &Config{}, "sv::sandbox-provider::"+tenant)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch := h.watchTenants(ctx)

	// The WatchTenants client call returns before the server-side handler has
	// run addWatcher; wait until the watcher is actually registered so the
	// relay's online event is delivered live (rather than racing the replay
	// window). The aggregator's pairingTable is in-process, so we can poll it.
	waitForWatcherCount(t, h.agg.pairs, 1, 3*time.Second)

	// A SINGLE persistent reader goroutine drains the stream into events: gRPC
	// client streams forbid concurrent Recv, so per-assertion readers would race
	// and one would silently steal the other's event.
	events := drainTenantEvents(watch)

	// Connect a relay → expect an online event. We use a cancellable per-relay
	// context so we can disconnect it and observe the offline event.
	relayCtx, relayCancel := context.WithCancel(ctx)
	_ = h.relayTunnel(relayCtx, tenant)

	ev := nextEventByTenant(t, events, tenant, 5*time.Second)
	if !ev.GetOnline() {
		t.Fatalf("first event online = %v, want true", ev.GetOnline())
	}

	// Disconnect the relay → expect an offline event.
	relayCancel()
	ev = nextEventByTenant(t, events, tenant, 5*time.Second)
	if ev.GetOnline() {
		t.Fatalf("second event online = %v, want false", ev.GetOnline())
	}
}

// TestAggregator_WatchTenantsSnapshotComplete asserts the resync-prune contract:
// a WatchTenants subscriber receives the currently-online tenants as a replay
// burst, then EXACTLY ONE terminal TenantEvent with SnapshotComplete==true, and
// every subsequent live online/offline transition carries SnapshotComplete==false
// (the zero value). The provider relies on this sentinel to prune stale tenants
// on a watch reconnect.
func TestAggregator_WatchTenantsSnapshotComplete(t *testing.T) {
	const (
		preTenant  = "tenant-hotel" // online BEFORE the watcher subscribes → replay
		liveTenant = "tenant-india" // comes online AFTER the sentinel → live event
	)
	h := newAggHarness(t, &Config{}, "sv::sandbox-provider::"+preTenant)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register a relay for preTenant BEFORE the watcher subscribes so it appears
	// in the replay burst (not as a live event).
	preCtx, preCancel := context.WithCancel(ctx)
	defer preCancel()
	_ = h.relayTunnel(preCtx, preTenant)
	waitForTenantOnline(t, h.agg.pairs, preTenant, 3*time.Second)

	watch := h.watchTenants(ctx)
	events := drainTenantEvents(watch)

	// Phase 1: replay burst. preTenant must arrive as an online replay event with
	// SnapshotComplete=false, in any order before the sentinel.
	sawPreReplay := false
	sentinel := nextEventUntilSentinel(t, events, 5*time.Second, func(ev *pb.TenantEvent) {
		if ev.GetSnapshotComplete() {
			t.Fatalf("replay event unexpectedly had SnapshotComplete=true: %+v", ev)
		}
		if ev.GetTenant() == preTenant {
			if !ev.GetOnline() {
				t.Fatalf("replay event for %q online=false, want true", preTenant)
			}
			sawPreReplay = true
		}
	})
	if !sawPreReplay {
		t.Fatalf("never saw replay online event for pre-online tenant %q", preTenant)
	}

	// Phase 2: the sentinel itself. Exactly one, SnapshotComplete=true, empty tenant.
	if !sentinel.GetSnapshotComplete() {
		t.Fatalf("sentinel SnapshotComplete=false, want true")
	}
	if sentinel.GetTenant() != "" {
		t.Fatalf("sentinel tenant = %q, want empty", sentinel.GetTenant())
	}

	// Phase 3: live events. A relay coming online AFTER the sentinel must be a
	// live online event with SnapshotComplete=false, then offline likewise.
	// Switch the stub CN so the new relay's CN encodes liveTenant.
	h.tunnelCN = "sv::sandbox-provider::" + liveTenant
	liveCtx, liveCancel := context.WithCancel(ctx)
	_ = h.relayTunnel(liveCtx, liveTenant)

	onEv := nextEventByTenant(t, events, liveTenant, 5*time.Second)
	if !onEv.GetOnline() {
		t.Fatalf("live event online = false, want true")
	}
	if onEv.GetSnapshotComplete() {
		t.Fatalf("live online event had SnapshotComplete=true, want false")
	}

	liveCancel()
	offEv := nextEventByTenant(t, events, liveTenant, 5*time.Second)
	if offEv.GetOnline() {
		t.Fatalf("live event online = true, want false")
	}
	if offEv.GetSnapshotComplete() {
		t.Fatalf("live offline event had SnapshotComplete=true, want false")
	}
}

// waitForTenantOnline blocks until the pairingTable has a registered relay for
// tenant, closing the relay-register vs watcher-subscribe race so the tenant is
// guaranteed to land in the replay burst rather than as a live event.
func waitForTenantOnline(t *testing.T, p *pairingTable, tenant string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		p.mu.Lock()
		_, ok := p.relays[tenant]
		p.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for tenant %q relay to register", tenant)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// nextEventUntilSentinel reads events, invoking inspect on each non-sentinel
// (replay) event, and returns the FIRST event with SnapshotComplete==true. It
// fails if the stream closes or the deadline fires before the sentinel arrives.
func nextEventUntilSentinel(t *testing.T, events <-chan *pb.TenantEvent, timeout time.Duration, inspect func(*pb.TenantEvent)) *pb.TenantEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("watch stream closed before snapshot_complete sentinel")
			}
			if ev.GetSnapshotComplete() {
				return ev
			}
			inspect(ev)
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot_complete sentinel")
			return nil
		}
	}
}

// waitForWatcherCount blocks until the pairingTable has at least n registered
// watchers, closing the client/server registration race in the watch test.
func waitForWatcherCount(t *testing.T, p *pairingTable, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		p.mu.Lock()
		got := len(p.watchers)
		p.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d watcher(s); have %d", n, got)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// drainTenantEvents starts ONE reader on the watch stream and forwards every
// received event onto a channel (closed when the stream ends/errors).
func drainTenantEvents(w grpc.ServerStreamingClient[pb.TenantEvent]) <-chan *pb.TenantEvent {
	out := make(chan *pb.TenantEvent, 16)
	go func() {
		defer close(out)
		for {
			ev, err := w.Recv()
			if err != nil {
				return
			}
			out <- ev
		}
	}()
	return out
}

// nextEventByTenant reads from the drained channel until one event matches
// tenant or the deadline fires (events for other tenants are skipped).
func nextEventByTenant(t *testing.T, events <-chan *pb.TenantEvent, tenant string, timeout time.Duration) *pb.TenantEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("watch stream closed before tenant %q event", tenant)
			}
			if ev.GetTenant() == tenant {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for tenant %q event", tenant)
			return nil
		}
	}
}

// =============================================================================
// 3. Pair-wait timeout fails cleanly when only one side connects.
// =============================================================================

func TestAggregator_PairWaitTimeout(t *testing.T) {
	const tenant = "tenant-delta"
	cfg := &Config{}
	cfg.Aggregator.PairWaitTimeoutMs = 150 // small, no counterpart will arrive
	h := newAggHarness(t, cfg, "sv::sandbox-provider::"+tenant)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Only the provider connects; no relay ever arrives.
	provider := h.providerConnect(ctx, tenant)

	// The provider's Recv should return an error once the pair-wait times out
	// (the handler sends an error downstream then returns, closing the stream).
	done := make(chan error, 1)
	go func() {
		for {
			_, err := provider.Recv()
			if err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected non-nil error on timeout")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("provider stream did not close after pair-wait timeout")
	}
}

// =============================================================================
// 3b. Mid-session relay teardown unblocks the provider and unregisters both
// endpoints from the pairing table.
// =============================================================================

func TestAggregator_RelayTeardownMidSession(t *testing.T) {
	const tenant = "tenant-golf"
	h := newAggHarness(t, &Config{}, "sv::sandbox-provider::"+tenant)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Pair provider + relay.
	provider := h.providerConnect(ctx, tenant)
	relayCtx, relayCancel := context.WithCancel(ctx)
	relay := h.relayTunnel(relayCtx, tenant)

	// Exchange one frame each direction to prove the splice is live before we
	// tear the relay down.
	if err := provider.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{ResumeSessionId: "r1"}}}); err != nil {
		t.Fatalf("provider send: %v", err)
	}
	gotUp, err := relay.Recv()
	if err != nil {
		t.Fatalf("relay recv up: %v", err)
	}
	if gotUp.GetUp().GetInit().GetResumeSessionId() != "r1" {
		t.Fatalf("relay up resume = %q, want r1", gotUp.GetUp().GetInit().GetResumeSessionId())
	}
	down := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_ConnectionAck{ConnectionAck: &pb.ConnectionAck{SessionId: "s1"}}}
	if err := relay.Send(&pb.TunnelFrame{F: &pb.TunnelFrame_Down{Down: down}}); err != nil {
		t.Fatalf("relay send down: %v", err)
	}
	if got, err := provider.Recv(); err != nil || got.GetConnectionAck().GetSessionId() != "s1" {
		t.Fatalf("provider recv down: got=%v err=%v", got, err)
	}

	// Cancel the relay side mid-session.
	relayCancel()

	// The provider's Connect must return as the splice tears down. The relay→
	// provider pump errors immediately when the relay stream closes; the splice
	// then cancels and bounded-drains the sibling provider→relay pump (which is
	// parked in Recv) for up to its 5s drain cap before Connect returns. Allow
	// for that drain bound plus margin — the assertion is that it returns at all
	// (no hang), not instantaneously.
	provDone := make(chan error, 1)
	go func() {
		for {
			_, err := provider.Recv()
			if err != nil {
				provDone <- err
				return
			}
		}
	}()
	select {
	case <-provDone:
		// returned — good (error or EOF both acceptable; the point is it did not
		// hang past the bounded splice drain).
	case <-time.After(8 * time.Second):
		t.Fatalf("provider Connect did not return after relay teardown")
	}

	// Both endpoints must be unregistered from the pairing table.
	waitForTenantUnregistered(t, h.agg.pairs, tenant, 5*time.Second)
}

// waitForTenantUnregistered polls the pairing table until neither a provider nor
// a relay nor a session remains for tenant, or the deadline fires.
func waitForTenantUnregistered(t *testing.T, p *pairingTable, tenant string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		p.mu.Lock()
		_, hasProv := p.providers[tenant]
		_, hasRelay := p.relays[tenant]
		_, hasSession := p.sessions[tenant]
		p.mu.Unlock()
		if !hasProv && !hasRelay && !hasSession {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("tenant %q still registered after teardown (provider=%v relay=%v session=%v)", tenant, hasProv, hasRelay, hasSession)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// =============================================================================
// 4. CN / hello tenant mismatch on the tunnel side is rejected.
// =============================================================================

func TestAggregator_TunnelHelloCNMismatchRejected(t *testing.T) {
	// peerCN reports tenant-echo; the relay's hello claims tenant-foxtrot.
	h := newAggHarness(t, &Config{}, "sv::sandbox-provider::tenant-echo")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relay := h.relayTunnel(ctx, "tenant-foxtrot")

	// The handler rejects the mismatch and returns an error, closing the stream.
	done := make(chan error, 1)
	go func() {
		_, err := relay.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected tunnel stream to be rejected on CN/hello mismatch")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("tunnel stream did not close on CN/hello mismatch")
	}
}

// =============================================================================
// Dedicated mTLS test: the real peer-cert CN-binding path.
//
// Runs the tunnel surface with real mTLS (RequireAndVerifyClientCert + the
// in-test CA). A relay cert whose CN encodes the WRONG tenant (different from
// the hello) must be rejected by the CN-vs-hello validation in Tunnel — proving
// the CN, not the hello, is the authority. A matching cert is accepted.
// =============================================================================

func TestAggregator_CNValidation(t *testing.T) {
	caCert, caKey := aggGenCA(t)
	caPEM := aggCertPEM(t, caCert)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := writeFile(caFile, caPEM); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	serverCertFile, serverKeyFile := aggWriteLeaf(t, "agg-server", caCert, caKey, true)

	cfg := &Config{}
	cfg.Aggregator.Enabled = true
	cfg.Aggregator.ProviderListen = "tcp://127.0.0.1:0" // unused in this test, but required by Validate
	tunLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tunnel: %v", err)
	}
	cfg.Aggregator.TunnelListen = "tcp://" + tunLis.Addr().String()
	cfg.Aggregator.ProviderTLS.Insecure = true
	cfg.Aggregator.TunnelTLS.CertFile = serverCertFile
	cfg.Aggregator.TunnelTLS.KeyFile = serverKeyFile
	cfg.Aggregator.TunnelTLS.ClientCAFile = caFile
	cfg.Aggregator.PairWaitTimeoutMs = 30_000
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate cfg: %v", err)
	}

	agg, err := NewAggregator(cfg)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	// Use the REAL peerCertCN so the cert CN flows through the live handshake.
	creds, err := agg.serverCreds(cfg.Aggregator.TunnelTLS)
	if err != nil {
		t.Fatalf("server creds: %v", err)
	}
	tunSrv := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterSandboxRelayTunnelServer(tunSrv, agg)
	go func() { _ = tunSrv.Serve(tunLis) }()
	t.Cleanup(tunSrv.Stop)

	addr := tunLis.Addr().String()

	// (a) Mismatch: relay cert CN pins tenant-mike, hello claims tenant-november.
	mismatchCert := aggGenLeafTLS(t, "sv::sandbox-provider::tenant-mike", caCert, caKey)
	if err := aggTunnelHelloOnce(t, addr, mismatchCert, "tenant-november"); err == nil {
		t.Fatalf("expected CN/hello mismatch to be rejected over real mTLS")
	}

	// (b) Match: relay cert CN pins tenant-mike and hello agrees → no rejection
	// (the stream stays open; we then time out waiting for a provider, which is
	// fine — we only assert the handshake/hello was NOT rejected).
	matchCert := aggGenLeafTLS(t, "sv::sandbox-provider::tenant-mike", caCert, caKey)
	if err := aggTunnelHelloOnce(t, addr, matchCert, "tenant-mike"); err != nil {
		// A matching CN must NOT produce a CN-mismatch rejection. The only
		// acceptable "error" is the deadline/EOF from no provider arriving,
		// which aggTunnelHelloOnce maps to nil.
		t.Fatalf("matching CN was rejected: %v", err)
	}
}

// =============================================================================
// Dedicated mTLS test: the provider-side CN-vs-metadata binding.
//
// Runs the PROVIDER surface with real mTLS. A provider cert whose CN encodes
// tenant-A while the x-aether-tenant metadata claims tenant-B must be rejected
// by the CN-vs-metadata validation in Connect (the CN, when it pins a tenant, is
// the authority and a mismatch is an authz violation). A provider cert whose CN
// AGREES with the metadata is accepted (no CN-mismatch rejection).
// =============================================================================

func TestAggregator_ProviderCNMetadataMismatch(t *testing.T) {
	caCert, caKey := aggGenCA(t)
	caPEM := aggCertPEM(t, caCert)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := writeFile(caFile, caPEM); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	serverCertFile, serverKeyFile := aggWriteLeaf(t, "agg-server", caCert, caKey, true)

	cfg := &Config{}
	cfg.Aggregator.Enabled = true
	provLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen provider: %v", err)
	}
	cfg.Aggregator.ProviderListen = "tcp://" + provLis.Addr().String()
	cfg.Aggregator.TunnelListen = "tcp://127.0.0.1:0" // unused here, but required by Validate
	cfg.Aggregator.TunnelTLS.Insecure = true
	cfg.Aggregator.ProviderTLS.CertFile = serverCertFile
	cfg.Aggregator.ProviderTLS.KeyFile = serverKeyFile
	cfg.Aggregator.ProviderTLS.ClientCAFile = caFile
	cfg.Aggregator.PairWaitTimeoutMs = 30_000
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate cfg: %v", err)
	}

	agg, err := NewAggregator(cfg)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	// Use the REAL peerCertCN so the cert CN flows through the live handshake.
	creds, err := agg.serverCreds(cfg.Aggregator.ProviderTLS)
	if err != nil {
		t.Fatalf("server creds: %v", err)
	}
	provSrv := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterAetherGatewayServer(provSrv, agg)
	go func() { _ = provSrv.Serve(provLis) }()
	t.Cleanup(provSrv.Stop)

	addr := provLis.Addr().String()

	// (a) Mismatch: provider cert CN pins tenant-alpha, metadata claims tenant-beta.
	mismatchCert := aggGenLeafTLS(t, "sv::sandbox-provider::tenant-alpha", caCert, caKey)
	if err := aggProviderConnectOnce(t, addr, mismatchCert, "tenant-beta"); err == nil {
		t.Fatalf("expected provider CN/metadata mismatch to be rejected over real mTLS")
	}

	// (b) Match: provider cert CN pins tenant-alpha and metadata agrees → no
	// CN-mismatch rejection (the stream parks waiting for a relay, which the
	// helper masks to nil).
	matchCert := aggGenLeafTLS(t, "sv::sandbox-provider::tenant-alpha", caCert, caKey)
	if err := aggProviderConnectOnce(t, addr, matchCert, "tenant-alpha"); err != nil {
		t.Fatalf("matching provider CN/metadata was rejected: %v", err)
	}
}

// aggProviderConnectOnce dials the provider surface over mTLS with clientCert,
// opens Connect with the x-aether-tenant metadata set to metaTenant, and reports
// whether the server rejected it. Mirrors aggTunnelHelloOnce: a genuine
// server-side rejection returns a status before the deadline; the match case
// parks waiting for a relay and hits the client deadline, which is masked to nil.
func aggProviderConnectOnce(t *testing.T, addr string, clientCert tls.Certificate, metaTenant string) error {
	t.Helper()
	tlsCreds := credentials.NewTLS(&tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		InsecureSkipVerify: true, // server identity not under test
	})
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(tlsCreds))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, metadataTenantKey, metaTenant)

	stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx)
	if err != nil {
		return err
	}
	// The provider surface rejects by FIRST sending a DownstreamMessage carrying
	// an ErrorResponse and THEN returning a non-nil status, so the client sees a
	// valid error frame (err==nil) before any stream error. Treat an error frame
	// as a rejection. The match case never receives a frame (no relay) and hits
	// the client deadline → "not rejected" (nil).
	msg, err := stream.Recv()
	if err == nil {
		if msg.GetError() != nil {
			return fmt.Errorf("provider rejected: %s: %s", msg.GetError().GetCode(), msg.GetError().GetMessage())
		}
		return nil
	}
	if ctx.Err() == context.DeadlineExceeded {
		return nil
	}
	return err
}

// aggTunnelHelloOnce dials the tunnel surface over mTLS with clientCert, sends a
// TunnelHello for helloTenant, and reports whether the server rejected it. A
// rejection surfaces as a non-nil Recv error that is NOT a deadline (the
// no-provider pair-wait path is masked to nil so the match case is clean).
func aggTunnelHelloOnce(t *testing.T, addr string, clientCert tls.Certificate, helloTenant string) error {
	t.Helper()
	tlsCreds := credentials.NewTLS(&tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		InsecureSkipVerify: true, // server identity not under test
	})
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(tlsCreds))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Bound the call: the match case parks waiting for a provider, so cap it.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	stream, err := pb.NewSandboxRelayTunnelClient(conn).Tunnel(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&pb.TunnelFrame{F: &pb.TunnelFrame_Hello{Hello: &pb.TunnelHello{Tenant: helloTenant}}}); err != nil {
		return err
	}
	_, err = stream.Recv()
	// The match case never receives a frame (no provider) and instead hits the
	// client deadline → treat that as "not rejected" (nil). A genuine server
	// rejection returns a server-originated status before the deadline.
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// In-test CA / cert helpers (minimal inline gen, same approach as
// spike_tenant_relay_test.go's spikeGenCA/spikeGenLeaf).
// ---------------------------------------------------------------------------

func aggGenCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "agg-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return cert, key
}

// aggGenLeafTLS returns a CA-signed client leaf as a tls.Certificate (for dial).
func aggGenLeafTLS(t *testing.T, cn string, ca *x509.Certificate, caKey *rsa.PrivateKey) tls.Certificate {
	t.Helper()
	der, key := aggGenLeafDER(t, cn, ca, caKey, false)
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// aggWriteLeaf writes a CA-signed leaf cert+key to temp files and returns their
// paths (for loading server creds from disk via serverCreds).
func aggWriteLeaf(t *testing.T, cn string, ca *x509.Certificate, caKey *rsa.PrivateKey, server bool) (certFile, keyFile string) {
	t.Helper()
	der, key := aggGenLeafDER(t, cn, ca, caKey, server)
	certPEM := pemBlock("CERTIFICATE", der)
	keyPEM := pemBlock("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	dir := t.TempDir()
	certFile = filepath.Join(dir, "leaf.crt")
	keyFile = filepath.Join(dir, "leaf.key")
	if err := writeFile(certFile, certPEM); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := writeFile(keyFile, keyPEM); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

func aggGenLeafDER(t *testing.T, cn string, ca *x509.Certificate, caKey *rsa.PrivateKey, server bool) ([]byte, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	eku := x509.ExtKeyUsageClientAuth
	if server {
		eku = x509.ExtKeyUsageServerAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{eku},
		BasicConstraintsValid: true,
	}
	if server {
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tmpl.DNSNames = []string{"localhost"}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	return der, key
}

func aggCertPEM(t *testing.T, cert *x509.Certificate) []byte {
	t.Helper()
	return pemBlock("CERTIFICATE", cert.Raw)
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}

// tenantFromCN unit coverage: only the exact sv::sandbox-provider::<tenant>
// service-CN shape yields a tenant; every other (malformed / extra-segment /
// tenant-less) shape yields "" so the metadata path engages and no wrong tenant
// is ever trusted (the hardening in finding #4).
func TestAggregator_TenantFromCN(t *testing.T) {
	cases := map[string]string{
		// Canonical service CN → trailing segment is the tenant.
		"sv::sandbox-provider::tenant-alice": "tenant-alice",
		"sv::sandbox-provider::pod-shared":   "pod-shared",
		"  sv::sandbox-provider::trimmed  ":  "trimmed", // surrounding space is trimmed
		// Tenant-less shared identity (no "::") → "" (provider metadata path).
		"no-separator":            "",
		"sandbox-provider-shared": "",
		"":                        "",
		// Malformed / extra-segment shapes must NOT yield a tenant (authz hazard
		// the greedy LastIndex previously had): the old code would have returned
		// "b" / "extra" here.
		"sv::sandbox-provider::a::b":      "", // too many segments
		"sv::sandbox-provider::tenant::x": "", // too many segments
		"sv::sandbox-provider::":          "", // empty tenant segment
		"x::sandbox-provider::tenant":     "", // wrong type segment
		"sv::other-service::tenant":       "", // wrong service segment
		"sv::sandbox-provider":            "", // missing tenant segment
		"a::b":                            "", // unrelated two-segment CN
	}
	for cn, want := range cases {
		if got := tenantFromCN(cn); got != want {
			t.Fatalf("tenantFromCN(%q) = %q, want %q", cn, got, want)
		}
	}
}
