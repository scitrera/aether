package proxysidecar

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// metadataTenantKey is the lowercase metadata header a provider sets on its
// Connect to declare which tenant's relay it wants paired. It is a HINT only:
// the authoritative tenant binding is the peer-cert CN (see the trust model in
// Connect/Tunnel below).
const metadataTenantKey = "x-aether-tenant"

// Aggregator runs the sidecar in aggregator mode: it owns two gRPC server
// surfaces — AetherGateway.Connect (providers dial in with an x-aether-tenant
// hint) and SandboxRelayTunnel (tenant-relays dial in and announce their tenant
// via TunnelHello) — and pairs them 1:1 by tenant (validated against the
// peer-cert CN), splicing each pair NOT as a mux but as N independent gateway
// sessions. It satisfies relaySurface.
//
// Trust model (decision 2/4 of the design spec): both the provider hop and the
// relay hop are mTLS over the sandboxes-CA. The tenant-identity authority is the
// peer-cert CN, NOT the x-aether-tenant metadata / TunnelHello.tenant hint. The
// hint is validated against the CN and a mismatch is rejected. The relay side is
// the load-bearing binding: a tenant-relay presents a per-tenant cert whose CN
// encodes the tenant (e.g. sv::sandbox-provider::<tenant>), so the relay cannot
// announce a tenant it does not hold a cert for. The provider side may present a
// single shared sandboxes-CA identity whose CN does NOT encode a tenant (one
// cert serves every tenant); in that case we can only validate metadata
// presence, and we accept the metadata tenant as the pairing key. When the
// provider CN DOES encode a tenant we additionally require it to match the
// metadata claim. Either way the relay's CN is what guarantees a provider can
// only ever be spliced to a relay that genuinely holds the tenant's cert.
//
// SECURITY: when a provider presents a shared sandboxes-CA cert whose CN encodes
// NO tenant, the pairing key is the x-aether-tenant metadata, not the cert. That
// means ANY holder of a valid sandboxes-CA client cert can pair with (and reach)
// ANY tenant's relay simply by claiming that tenant in metadata — the cert alone
// confers no per-tenant restriction on the provider hop. The provider_listen
// surface MUST therefore be network-restricted (P4 NetworkPolicy) to the trusted
// provider controller; do not expose it broadly. The relay hop stays bound by
// its per-tenant CN regardless.
type Aggregator struct {
	pb.UnimplementedAetherGatewayServer
	pb.UnimplementedSandboxRelayTunnelServer

	cfg   *Config
	pairs *pairingTable

	// peerCN extracts the peer-cert CN from a stream context. Production code
	// uses peerCertCN; tests that run the surfaces over insecure transport
	// inject a stub so the splice/pairing/watch paths don't require real certs.
	peerCN func(ctx context.Context) (string, error)
}

// NewAggregator constructs an Aggregator from cfg. The listeners are not opened
// until Run is invoked. cfg.Validate() must have been called first (NewRunner
// does this).
func NewAggregator(cfg *Config) (*Aggregator, error) {
	a := &Aggregator{
		cfg:   cfg,
		pairs: newPairingTable(),
	}
	a.peerCN = peerCertCN
	return a, nil
}

// Run binds both listeners, registers the two gRPC server surfaces (the
// AetherGateway provider surface on ProviderListen, the SandboxRelayTunnel relay
// surface on TunnelListen), and serves until ctx is cancelled. Each surface gets
// its own mTLS server credentials built from ProviderTLS / TunnelTLS. Mirrors
// relay.go's serve/stop shape (a goroutine per Serve, bounded GracefulStop on
// teardown).
func (a *Aggregator) Run(ctx context.Context) error {
	providerLis, providerCleanup, err := openRelayListener(a.cfg.Aggregator.ProviderListen)
	if err != nil {
		return fmt.Errorf("aggregator: open provider listener: %w", err)
	}
	defer func() {
		_ = providerLis.Close()
		if providerCleanup != nil {
			providerCleanup()
		}
	}()

	tunnelLis, tunnelCleanup, err := openRelayListener(a.cfg.Aggregator.TunnelListen)
	if err != nil {
		return fmt.Errorf("aggregator: open tunnel listener: %w", err)
	}
	defer func() {
		_ = tunnelLis.Close()
		if tunnelCleanup != nil {
			tunnelCleanup()
		}
	}()

	providerSrv, err := a.newSurfaceServer(a.cfg.Aggregator.ProviderTLS)
	if err != nil {
		return fmt.Errorf("aggregator: build provider server: %w", err)
	}
	pb.RegisterAetherGatewayServer(providerSrv, a)

	tunnelSrv, err := a.newSurfaceServer(a.cfg.Aggregator.TunnelTLS)
	if err != nil {
		return fmt.Errorf("aggregator: build tunnel server: %w", err)
	}
	pb.RegisterSandboxRelayTunnelServer(tunnelSrv, a)

	serveErr := make(chan error, 2)
	go func() { serveErr <- providerSrv.Serve(providerLis) }()
	go func() { serveErr <- tunnelSrv.Serve(tunnelLis) }()

	log.Info().
		Str("provider_listen", a.cfg.Aggregator.ProviderListen).
		Str("tunnel_listen", a.cfg.Aggregator.TunnelListen).
		Bool("require_tenant_metadata", a.cfg.Aggregator.RequireTenantMetadataEnabled()).
		Int64("pair_wait_timeout_ms", a.cfg.Aggregator.PairWaitTimeoutMs).
		Msg("proxy sidecar aggregator running")

	select {
	case <-ctx.Done():
		log.Info().Msg("proxy sidecar aggregator shutting down")
		a.stopServers(providerSrv, tunnelSrv)
		// Drain both Serve goroutines so we don't leak them past Run's return.
		<-serveErr
		<-serveErr
		return nil
	case err := <-serveErr:
		// One surface failed to serve; tear the other down and surface the error.
		a.stopServers(providerSrv, tunnelSrv)
		<-serveErr
		if err != nil {
			return fmt.Errorf("aggregator: serve: %w", err)
		}
		return nil
	}
}

// stopServers gracefully stops both surface servers, bounding the wait so a
// mid-handshake inbound connection that never sent the HTTP/2 preface can't pin
// shutdown forever (same rationale as relay.go's bounded GracefulStop).
func (a *Aggregator) stopServers(servers ...*grpc.Server) {
	const gracePeriod = 3 * time.Second
	var wg sync.WaitGroup
	for _, srv := range servers {
		srv := srv
		wg.Add(1)
		go func() {
			defer wg.Done()
			done := make(chan struct{})
			go func() {
				srv.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(gracePeriod):
				srv.Stop()
				<-done
			}
		}()
	}
	wg.Wait()
}

// newSurfaceServer builds a gRPC server for one aggregator surface. When the
// surface's TLS is not Insecure it enforces mTLS: it presents the configured
// server cert and requires-and-verifies a client cert chained to ClientCAFile
// (the sandboxes-CA), so the peer-cert CN is always available as the tenant
// authority. When Insecure is set the server runs plaintext (test/dev only).
func (a *Aggregator) newSurfaceServer(tlsConf ServerTLSConfig) (*grpc.Server, error) {
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 15 * time.Second,
		}),
		// Permit the dialer-side keepalive the relay/provider SDK uses (idle
		// streamless pings every 30s) without tripping ENHANCE_YOUR_CALM, same
		// rationale as relay.go's enforcement policy.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		// Bound the HTTP/2 handshake so a peer that opens a socket but never
		// sends the preface can't pin a Serve goroutine (and thus shutdown) for
		// the grpc default 120s. See relay.go for the full rationale.
		grpc.ConnectionTimeout(10 * time.Second),
		grpc.MaxRecvMsgSize(10 * 1024 * 1024),
		grpc.MaxSendMsgSize(10 * 1024 * 1024),
	}
	if !tlsConf.Insecure {
		creds, err := a.serverCreds(tlsConf)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	return grpc.NewServer(opts...), nil
}

// serverCreds loads the surface's server cert/key and client-CA pool into mTLS
// transport credentials (RequireAndVerifyClientCert + ClientCAs).
func (a *Aggregator) serverCreds(tlsConf ServerTLSConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(tlsConf.CertFile, tlsConf.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server keypair (%q/%q): %w", tlsConf.CertFile, tlsConf.KeyFile, err)
	}
	caPEM, err := os.ReadFile(tlsConf.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client_ca_file %q: %w", tlsConf.ClientCAFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("client_ca_file %q: no certs parsed", tlsConf.ClientCAFile)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}), nil
}

// Connect implements the provider surface (pb.AetherGatewayServer). A provider
// dials in declaring its target tenant via the x-aether-tenant metadata header.
// Each invocation registers the provider for its tenant, waits up to
// PairWaitTimeoutMs for a matching relay tunnel, and then splices the two 1:1.
//
// Template: relay.go Connect (relay.go:219). The difference is that the relay
// surface here is not a per-session gateway dial — it is the wait-for-tunnel
// pairing.
func (a *Aggregator) Connect(stream pb.AetherGateway_ConnectServer) error {
	ctx := stream.Context()

	// Read the x-aether-tenant hint. RequireTenantMetadata (default true)
	// rejects a Connect that omits it.
	tenant, hasMeta := tenantFromMetadata(ctx)
	if !hasMeta {
		if a.cfg.Aggregator.RequireTenantMetadataEnabled() {
			_ = stream.Send(relayErrorDownstream("AGG_TENANT_METADATA_REQUIRED",
				"provider Connect must carry the x-aether-tenant metadata header"))
			return fmt.Errorf("aggregator: provider Connect missing %s metadata", metadataTenantKey)
		}
	}

	// Validate the metadata hint against the provider peer-cert CN. The provider
	// may present a single shared sandboxes-CA cert whose CN does NOT encode a
	// tenant, so a CN that yields no tenant is accepted (we trust the relay-side
	// CN for the authoritative binding). When the provider CN DOES encode a
	// tenant it must match the metadata claim.
	cn, err := a.peerCN(ctx)
	if err != nil {
		_ = stream.Send(relayErrorDownstream("AGG_PEER_CERT_MISSING",
			fmt.Sprintf("provider peer cert: %v", err)))
		return fmt.Errorf("aggregator: provider peer cert: %w", err)
	}
	if cnTenant := tenantFromCN(cn); cnTenant != "" {
		if tenant == "" {
			// No metadata hint but the CN pins a tenant: trust the CN.
			tenant = cnTenant
		} else if cnTenant != tenant {
			_ = stream.Send(relayErrorDownstream("AGG_TENANT_CN_MISMATCH",
				fmt.Sprintf("provider cert CN tenant %q != x-aether-tenant %q", cnTenant, tenant)))
			return fmt.Errorf("aggregator: provider CN tenant %q != metadata %q", cnTenant, tenant)
		}
	}
	if tenant == "" {
		_ = stream.Send(relayErrorDownstream("AGG_TENANT_UNRESOLVED",
			"could not resolve provider tenant from metadata or cert CN"))
		return fmt.Errorf("aggregator: could not resolve provider tenant (cn=%q)", cn)
	}

	provider := &providerEndpoint{stream: stream}
	if err := a.pairs.registerProvider(tenant, provider); err != nil {
		_ = stream.Send(relayErrorDownstream("AGG_PROVIDER_DUPLICATE", err.Error()))
		return fmt.Errorf("aggregator: register provider for tenant %q: %w", tenant, err)
	}
	defer a.pairs.unregisterProvider(tenant, provider)

	log.Info().Str("tenant", tenant).Str("cn", cn).Msg("aggregator: provider connected; awaiting relay")

	relay, err := a.pairs.awaitRelay(ctx, tenant, provider, a.pairWait())
	if err != nil {
		_ = stream.Send(relayErrorDownstream("AGG_PAIR_TIMEOUT",
			fmt.Sprintf("no relay for tenant %q: %v", tenant, err)))
		return fmt.Errorf("aggregator: await relay for tenant %q: %w", tenant, err)
	}

	// The PROVIDER handler is the elected splicer (exactly one goroutine may
	// drive each stream's Recv/Send). It publishes a shared session that the
	// relay handler retrieves and blocks on, so the two streams are never
	// touched concurrently from both handlers.
	session := newPairSession(provider, relay)
	a.pairs.publishSession(tenant, session)
	defer a.pairs.retractSession(tenant, session)

	log.Info().Str("tenant", tenant).Msg("aggregator: provider/relay paired; splicing")
	err = a.splice(ctx, tenant, provider, relay)
	session.finish(err)
	return err
}

// Tunnel implements the relay surface (pb.SandboxRelayTunnelServer). The relay's
// FIRST frame MUST be a TunnelHello announcing its tenant (cf relay.go:230-242).
// The hello tenant is a hint validated against the relay peer-cert CN (the CN is
// the authority): a mismatch is rejected. The relay is then registered and
// paired with a waiting or future provider Connect, and the two are spliced 1:1.
func (a *Aggregator) Tunnel(stream pb.SandboxRelayTunnel_TunnelServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("aggregator: tunnel recv hello: %w", err)
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("aggregator: first tunnel frame was %T, expected hello", first.GetF())
	}
	helloTenant := hello.GetTenant()

	// The relay peer-cert CN is the authoritative tenant binding: a per-tenant
	// relay holds a cert whose CN encodes its tenant, so it cannot announce a
	// tenant it does not own. The hello is a hint; reject any mismatch.
	cn, err := a.peerCN(ctx)
	if err != nil {
		return fmt.Errorf("aggregator: relay peer cert: %w", err)
	}
	cnTenant := tenantFromCN(cn)
	if cnTenant == "" {
		return fmt.Errorf("aggregator: relay cert CN %q does not encode a tenant", cn)
	}
	if helloTenant != "" && helloTenant != cnTenant {
		return fmt.Errorf("aggregator: relay hello tenant %q != cert CN tenant %q (CN is authority)", helloTenant, cnTenant)
	}
	tenant := cnTenant

	relay := &tunnelEndpoint{stream: stream}
	if err := a.pairs.registerRelay(tenant, relay); err != nil {
		return fmt.Errorf("aggregator: register relay for tenant %q: %w", tenant, err)
	}
	defer a.pairs.unregisterRelay(tenant, relay)

	log.Info().Str("tenant", tenant).Str("cn", cn).Msg("aggregator: relay connected; awaiting provider")

	// The relay handler is the PASSIVE side: the provider's Connect handler runs
	// the splice (single-splicer invariant — only one goroutine may Recv/Send a
	// given stream). The relay must keep its stream open while that splice runs
	// (returning here would close the stream), so it waits for the provider to
	// publish a session whose endpoints match this relay, then blocks until that
	// session finishes (or its own ctx is cancelled).
	session, err := a.pairs.awaitSession(ctx, tenant, relay, a.pairWait())
	if err != nil {
		return fmt.Errorf("aggregator: await provider for tenant %q: %w", tenant, err)
	}

	log.Info().Str("tenant", tenant).Msg("aggregator: relay/provider paired; provider splicing")
	select {
	case spliceErr := <-session.done:
		return spliceErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WatchTenants streams TenantEvent{tenant, online} to a provider's controller so
// it can dynamically dial the provider surface as tenants' relays come and go.
// It replays the currently-online tenants, then forwards mutations until ctx is
// done. The watcher channel is cleaned up on return.
func (a *Aggregator) WatchTenants(_ *pb.WatchTenantsRequest, stream grpc.ServerStreamingServer[pb.TenantEvent]) error {
	ctx := stream.Context()

	ch, online := a.pairs.addWatcher()
	defer a.pairs.removeWatcher(ch)

	// Replay the current online set so a late-joining watcher converges.
	for _, tenant := range online {
		if err := stream.Send(&pb.TenantEvent{Tenant: tenant, Online: true}); err != nil {
			return err
		}
	}

	// Emit a terminal snapshot_complete sentinel once the replay burst is done
	// and before the live-event phase begins. The provider accumulates the
	// replay online-set until this sentinel arrives, then prunes any tenant it
	// still holds that is absent from the set — deterministically reaping the
	// tenants that left during a watch disconnect (the resync-prune fix). Sent
	// per-(re)subscribe, so every reconnecting watcher gets: replay → sentinel →
	// live events. Live events keep SnapshotComplete=false (the zero value).
	if err := stream.Send(&pb.TenantEvent{SnapshotComplete: true}); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

// splice copies frames between one paired provider and relay, 1:1.
//
// IMPORTANT — this is NOT a relaymux. relaymux.go (relaymux.go:1-28) multiplexes
// MANY sub-client streams onto ONE shared upstream gateway connection, demuxing
// replies by request_id and rewriting every sub-client to the SAME service
// identity. The aggregator deliberately does the OPPOSITE: each (provider, relay)
// pair is an INDEPENDENT splice. The relay forwards the provider's InitConnection
// verbatim to the tenant gateway, so every pair gets its own gateway session,
// lock, and session_id and its own resumable identity. Multiplexing here would
// collapse N tenants' sessions onto one identity and break per-session state —
// exactly what we must avoid. Hence: one goroutine pair per splice, no demux.
//
// Direction provider→relay: provider.Recv() yields *pb.UpstreamMessage; we wrap
// it as TunnelFrame.up and Send to the relay. Direction relay→provider:
// relay.Recv() yields *pb.TunnelFrame; we unwrap .down and Send to the provider.
// Unexpected up/hello frames arriving relay→provider are logged and dropped.
//
// Teardown mirrors tenant_relay.go runPumps: errCh-of-two, cancel on the first
// return, a bounded drain of the sibling, EOF→nil.
func (a *Aggregator) splice(ctx context.Context, tenant string, provider *providerEndpoint, relay *tunnelEndpoint) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- a.pumpProviderToRelay(ctx, provider, relay) }()
	go func() { errCh <- a.pumpRelayToProvider(ctx, provider, relay) }()

	first := <-errCh
	cancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
	}

	log.Info().Str("tenant", tenant).Err(spliceErr(first)).Msg("aggregator: splice closed")
	if errors.Is(first, io.EOF) {
		return nil
	}
	return first
}

// pumpProviderToRelay copies provider → relay, wrapping each upstream envelope
// in a TunnelFrame.up for the relay to forward to the tenant gateway verbatim.
func (a *Aggregator) pumpProviderToRelay(ctx context.Context, provider *providerEndpoint, relay *tunnelEndpoint) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		up, err := provider.stream.Recv()
		if err != nil {
			return err
		}
		if err := relay.stream.Send(&pb.TunnelFrame{F: &pb.TunnelFrame_Up{Up: up}}); err != nil {
			return err
		}
	}
}

// pumpRelayToProvider copies relay → provider, unwrapping each TunnelFrame.down
// into the DownstreamMessage the provider's gateway client expects. up/hello
// frames are not expected on this direction (the relay only sends down frames
// after the hello handshake); they are logged and dropped rather than forwarded.
func (a *Aggregator) pumpRelayToProvider(ctx context.Context, provider *providerEndpoint, relay *tunnelEndpoint) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		frame, err := relay.stream.Recv()
		if err != nil {
			return err
		}
		down := frame.GetDown()
		if down == nil {
			log.Debug().
				Str("frame", fmt.Sprintf("%T", frame.GetF())).
				Msg("aggregator: dropping non-down tunnel frame on relay→provider")
			continue
		}
		if err := provider.stream.Send(down); err != nil {
			return err
		}
	}
}

// pairWait converts the configured PairWaitTimeoutMs into a Duration.
func (a *Aggregator) pairWait() time.Duration {
	return time.Duration(a.cfg.Aggregator.PairWaitTimeoutMs) * time.Millisecond
}

// spliceErr normalises a clean EOF teardown to nil for logging.
func spliceErr(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// providerEndpoint holds a registered provider's Connect stream.
type providerEndpoint struct {
	stream pb.AetherGateway_ConnectServer
}

// tunnelEndpoint holds a registered relay's Tunnel stream.
type tunnelEndpoint struct {
	stream pb.SandboxRelayTunnel_TunnelServer
}

// pairSession is the shared rendezvous for one paired (provider, relay). The
// provider's Connect handler is the elected splicer; it publishes the session,
// runs the splice, and calls finish. The relay's Tunnel handler retrieves the
// same session and blocks on done — guaranteeing exactly one goroutine drives
// each stream (the single-splicer invariant; concurrent Recv on a gRPC stream
// is a data race).
type pairSession struct {
	provider *providerEndpoint
	relay    *tunnelEndpoint
	done     chan error
}

func newPairSession(provider *providerEndpoint, relay *tunnelEndpoint) *pairSession {
	return &pairSession{provider: provider, relay: relay, done: make(chan error, 1)}
}

// finish reports the splice result to the waiting relay handler. Buffered (cap
// 1) so the provider never blocks even if the relay has already departed.
func (s *pairSession) finish(err error) {
	if errors.Is(err, io.EOF) {
		err = nil
	}
	s.done <- err
}

// pairingTable tracks at most one provider and one relay per tenant and pairs
// them 1:1 as each side arrives. WatchTenants watchers are fanned out whenever a
// tenant's relay comes online or goes offline.
//
// A tenant is considered "online" (and emitted to watchers) when its RELAY is
// registered: providers dynamically dial the provider surface in response to
// online events, so relay-presence is the meaningful signal. Duplicate policy
// (documented): a second relay for an already-bound tenant is REJECTED with a
// clear error (the existing relay keeps the tenant) rather than displacing it,
// so a misconfigured/duplicate relay cannot silently steal a live pairing. A
// second provider for an already-registered provider is likewise rejected.
type pairingTable struct {
	mu        sync.Mutex
	relays    map[string]*tunnelEndpoint
	providers map[string]*providerEndpoint

	// sessions holds the published rendezvous for a tenant once its provider has
	// elected itself splicer (keyed by tenant).
	sessions map[string]*pairSession

	// waiters are notified (by close) when a counterpart appears. relayWaiters
	// are woken when a relay registers; sessionWaiters when a provider publishes
	// a session. Each tenant's waiters live in a set (keyed by the wake channel)
	// so an individual waiter that times out or whose ctx is cancelled can remove
	// ITSELF before returning — otherwise a provider repeatedly dialing a tenant
	// with no relay would grow the waiter set unbounded (the leak fixed here).
	relayWaiters   map[string]map[chan struct{}]struct{}
	sessionWaiters map[string]map[chan struct{}]struct{}

	watchers []chan *pb.TenantEvent
}

func newPairingTable() *pairingTable {
	return &pairingTable{
		relays:         map[string]*tunnelEndpoint{},
		providers:      map[string]*providerEndpoint{},
		sessions:       map[string]*pairSession{},
		relayWaiters:   map[string]map[chan struct{}]struct{}{},
		sessionWaiters: map[string]map[chan struct{}]struct{}{},
	}
}

// registerProvider records the provider for tenant. Rejects a second provider
// for a tenant that already has one (duplicate policy above). The waiting relay
// is not woken here — it is woken when the provider publishes the splice session
// (publishSession), which only happens after the relay is confirmed present.
func (p *pairingTable) registerProvider(tenant string, ep *providerEndpoint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.providers[tenant]; exists {
		return fmt.Errorf("a provider is already connected for tenant %q", tenant)
	}
	p.providers[tenant] = ep
	return nil
}

// unregisterProvider removes ep iff it is still the registered provider for
// tenant (a later provider that replaced a stale one is not clobbered).
func (p *pairingTable) unregisterProvider(tenant string, ep *providerEndpoint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.providers[tenant] == ep {
		delete(p.providers, tenant)
	}
}

// registerRelay records the relay for tenant and emits an online TenantEvent.
// Rejects a second relay for a tenant that already has one (duplicate policy).
func (p *pairingTable) registerRelay(tenant string, ep *tunnelEndpoint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.relays[tenant]; exists {
		return fmt.Errorf("a relay is already connected for tenant %q", tenant)
	}
	p.relays[tenant] = ep
	p.wake(p.relayWaiters, tenant)
	p.emitLocked(&pb.TenantEvent{Tenant: tenant, Online: true})
	return nil
}

// unregisterRelay removes ep iff it is still the registered relay for tenant and
// emits an offline TenantEvent in that case.
func (p *pairingTable) unregisterRelay(tenant string, ep *tunnelEndpoint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.relays[tenant] == ep {
		delete(p.relays, tenant)
		p.emitLocked(&pb.TenantEvent{Tenant: tenant, Online: false})
	}
}

// awaitRelay returns the relay paired with this provider's tenant, blocking up
// to timeout for one to register. Called from Connect after registerProvider.
func (p *pairingTable) awaitRelay(ctx context.Context, tenant string, _ *providerEndpoint, timeout time.Duration) (*tunnelEndpoint, error) {
	return awaitEndpoint(ctx, timeout, &p.mu, func() *tunnelEndpoint { return p.relays[tenant] }, func(ch chan struct{}) func() {
		return p.subscribeLocked(p.relayWaiters, tenant, ch)
	})
}

// publishSession stores the provider-elected splice session for tenant and wakes
// the relay handler blocked in awaitSession. Called by the provider's Connect
// handler once both sides are confirmed present.
func (p *pairingTable) publishSession(tenant string, s *pairSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions[tenant] = s
	p.wake(p.sessionWaiters, tenant)
}

// retractSession removes s iff it is still the published session for tenant (a
// later session is not clobbered). Called by the provider on splice teardown.
func (p *pairingTable) retractSession(tenant string, s *pairSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sessions[tenant] == s {
		delete(p.sessions, tenant)
	}
}

// awaitSession returns the splice session published for this relay's tenant,
// blocking up to timeout. It only returns a session whose relay endpoint is THIS
// relay, so a stale session from a prior pairing is ignored. Called from Tunnel
// after registerRelay.
func (p *pairingTable) awaitSession(ctx context.Context, tenant string, relay *tunnelEndpoint, timeout time.Duration) (*pairSession, error) {
	return awaitEndpoint(ctx, timeout, &p.mu, func() *pairSession {
		if s := p.sessions[tenant]; s != nil && s.relay == relay {
			return s
		}
		return nil
	}, func(ch chan struct{}) func() {
		return p.subscribeLocked(p.sessionWaiters, tenant, ch)
	})
}

// waitFor blocks until present() returns a non-nil endpoint, ctx is done, or
// timeout elapses. It checks once under the lock (covering the already-present
// case with no race), and otherwise registers a wake channel via subscribe and
// re-checks on each wake. The generic T is the counterpart endpoint type.
//
// subscribe registers the wake channel under the lock and returns an unsubscribe
// closure that removes that SPECIFIC channel from the tenant's waiter set. On a
// timed-out or ctx-cancelled exit we must call unsubscribe (under the lock) so
// the orphaned waiter does not accumulate — a provider repeatedly dialing a
// tenant with no relay would otherwise grow the waiter set without bound. The
// wake-path exit needs no unsubscribe: wake() already closed and cleared the
// whole set, so the channel is gone (re-removing it is a harmless no-op anyway).
func awaitEndpoint[T any](ctx context.Context, timeout time.Duration, mu *sync.Mutex, present func() *T, subscribe func(chan struct{}) func()) (*T, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		mu.Lock()
		if ep := present(); ep != nil {
			mu.Unlock()
			return ep, nil
		}
		wake := make(chan struct{})
		unsubscribe := subscribe(wake)
		mu.Unlock()

		select {
		case <-wake:
			// Counterpart registered (or a spurious wake); loop and re-check. The
			// wake() that closed this channel also cleared the set, so there is
			// nothing to unsubscribe.
		case <-deadline.C:
			// Final check in case the counterpart raced in with the timer.
			mu.Lock()
			ep := present()
			if ep != nil {
				mu.Unlock()
				return ep, nil
			}
			unsubscribe()
			mu.Unlock()
			return nil, fmt.Errorf("pairing timed out after %s", timeout)
		case <-ctx.Done():
			mu.Lock()
			unsubscribe()
			mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

// subscribeLocked adds ch to the tenant's waiter set and returns an unsubscribe
// closure that removes that specific channel (and prunes the now-empty tenant
// entry). Callers hold p.mu when calling this AND when invoking the returned
// closure. Removal is idempotent: a wake() that already closed+cleared the set
// makes the closure a no-op.
func (p *pairingTable) subscribeLocked(waiters map[string]map[chan struct{}]struct{}, tenant string, ch chan struct{}) func() {
	set := waiters[tenant]
	if set == nil {
		set = map[chan struct{}]struct{}{}
		waiters[tenant] = set
	}
	set[ch] = struct{}{}
	return func() {
		set, ok := waiters[tenant]
		if !ok {
			return
		}
		delete(set, ch)
		if len(set) == 0 {
			delete(waiters, tenant)
		}
	}
}

// wake closes and clears every waiter channel registered for tenant. Callers
// hold p.mu. Closing wakes the waiter's select; the waiter re-checks the table
// under the lock, so a closed-but-stale wake is harmless. Clearing the tenant
// entry means the waiter's unsubscribe closure becomes a no-op (it must never
// close an already-closed channel).
func (p *pairingTable) wake(waiters map[string]map[chan struct{}]struct{}, tenant string) {
	for ch := range waiters[tenant] {
		close(ch)
	}
	delete(waiters, tenant)
}

// addWatcher registers a watcher channel and returns it alongside a snapshot of
// the currently-online tenants for replay. The channel is buffered so a slow
// watcher does not block table mutations; if it fills, events are dropped for
// that watcher (it will reconverge on the next event or reconnect).
func (p *pairingTable) addWatcher() (chan *pb.TenantEvent, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := make(chan *pb.TenantEvent, 64)
	p.watchers = append(p.watchers, ch)
	online := make([]string, 0, len(p.relays))
	for tenant := range p.relays {
		online = append(online, tenant)
	}
	return ch, online
}

// removeWatcher unregisters ch and closes it so the WatchTenants loop returns.
func (p *pairingTable) removeWatcher(ch chan *pb.TenantEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.watchers {
		if w == ch {
			p.watchers = append(p.watchers[:i], p.watchers[i+1:]...)
			close(ch)
			return
		}
	}
}

// emitLocked fans an event out to every watcher. Callers hold p.mu. A full
// watcher channel drops the event (non-blocking send) rather than wedging the
// table under the lock.
func (p *pairingTable) emitLocked(ev *pb.TenantEvent) {
	for _, ch := range p.watchers {
		select {
		case ch <- ev:
		default:
			log.Warn().Str("tenant", ev.GetTenant()).Bool("online", ev.GetOnline()).
				Msg("aggregator: watcher channel full; dropping tenant event")
		}
	}
}

// peerCertCN extracts the peer-cert CN from ctx, mirroring
// internal/gateway/identity.go:80-94 (peer.FromContext → credentials.TLSInfo →
// PeerCertificates[0].Subject.CommonName). It is a LOCAL copy by design: the
// design spec (decision 4) forbids importing internal/gateway from the
// proxysidecar layer. pkg/certident extraction is noted as a future cleanup.
func peerCertCN(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("no peer info in context")
	}
	tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", fmt.Errorf("peer auth info is not TLS")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", fmt.Errorf("no client certificate")
	}
	return tlsInfo.State.PeerCertificates[0].Subject.CommonName, nil
}

// tenantFromCN parses the tenant out of a peer-cert CN. CNs follow the canonical
// identity format {type}::{...} using the "::" separator (cf
// identity.go:104-243). A tenant-bearing service CN has the exact three-segment
// shape sv::sandbox-provider::<tenant>, matching the tenant-relay cert layout
// proven in the gateway spike (spike_tenant_relay_test.go:190); the trailing
// segment is the tenant.
//
// The shape is validated strictly before the trailing segment is trusted: a
// greedy LastIndex("::") would silently treat a malformed or extra-segment CN
// (e.g. sv::sandbox-provider::a::b) as pinning tenant "b", an authz hazard. We
// therefore require exactly the known service-CN field count and a non-empty
// tenant segment. Anything else — a generic shared identity with no "::" (the
// provider may legitimately present one), or any unexpected shape — yields ""
// so the caller falls back to the metadata pairing path. The caller decides
// whether an empty tenant is acceptable (it is for the provider hop, not the
// relay hop).
func tenantFromCN(cn string) string {
	cn = strings.TrimSpace(cn)
	parts := strings.Split(cn, "::")
	// Expect exactly: ["sv", "sandbox-provider", "<tenant>"].
	if len(parts) != 3 || parts[0] != "sv" || parts[1] != "sandbox-provider" || parts[2] == "" {
		return ""
	}
	return parts[2]
}

// tenantFromMetadata reads the x-aether-tenant hint from a stream's incoming
// metadata (cf gateway/connect.go's metadata.FromIncomingContext usage). The
// bool reports whether the header was present at all (so RequireTenantMetadata
// can distinguish "absent" from "present but empty").
func tenantFromMetadata(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	vals := md.Get(metadataTenantKey)
	if len(vals) == 0 {
		return "", false
	}
	return strings.TrimSpace(vals[0]), true
}
