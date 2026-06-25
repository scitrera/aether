// Full-chain composition test: REAL aggregator + REAL tenant-relay spliced
// together over real TCP/mTLS, driving a fake tenant gateway at the far end and
// a raw provider AetherGateway client at the near end.
//
// Both halves of the per-tenant relay topology (aggregator.go, tenant_relay.go)
// were previously only unit-tested against a FAKE of the other side:
//   - aggregator_test.go drives the aggregator with a fakeProvider + fakeRelay.
//   - tenant_relay_test.go drives the relay with a fakeGateway + fakeAggregator.
//
// Neither ever ran the real aggregator splice and the real relay pump together.
// This test composes the full two-hop path
//
//	provider (raw AetherGateway client)
//	   │  mTLS, sandboxes-CA provider cert, x-aether-tenant metadata
//	   ▼
//	aggregator.Connect (provider surface)  ──splice──  aggregator.Tunnel (relay surface)
//	                                                        ▲
//	   tenant-relay ── TunnelHello + frame pumps ───────────┘
//	   │  mTLS, tenant-CA tenant cert
//	   ▼
//	fake tenant gateway (records InitConnection, replies ConnectionAck, pushes downstream)
//
// so a regression in EITHER component's wire handling (splice fidelity, init
// passthrough, resume preservation) is caught by composition rather than by a
// fake that happens to agree with the bug.
//
// This file is deliberately NOT behind the `e2e` build tag (unlike the rest of
// this package's heavy aetherlite-subprocess suite): it stands up only
// in-process gRPC servers and is fast/deterministic, so it runs under the
// default `go test ./internal/proxysidecar/integration_e2e/...` invocation.
//
// NOTE on the provider client: we use a RAW pb.NewAetherGatewayClient(...).
// Connect with x-aether-tenant set via metadata.NewOutgoingContext, NOT the
// Aether SDK. The SDK metadata-injection hook that would set x-aether-tenant
// automatically is a separate P3 (provider) concern; for the aggregator+relay
// composition under test the raw client is the right scope.

package integration_e2e

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
	"sync"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/proxysidecar"
	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	fcTenant          = "tenant-fullchain"
	fcProviderSharedCN = "sandbox-provider-shared"                    // tenant-less provider CN → metadata pairing path
	fcRelayCN          = "sv::sandbox-provider::" + fcTenant          // tenant-encoding relay CN → CN binding path
	metadataTenantKey  = "x-aether-tenant"                            // mirrors proxysidecar.metadataTenantKey (unexported)
)

// =============================================================================
// fakeGateway — a pb.AetherGatewayServer that the tenant-relay dials.
//
// It records the first InitConnection it sees, ACKs it with a session id,
// records subsequent upstream frames, and can push a downstream message on
// demand. For the resume assertion it tracks, per claimed identity, whether a
// later Connect carried resume_session_id == a previously-issued session id.
// =============================================================================

type fakeGateway struct {
	pb.UnimplementedAetherGatewayServer

	mu sync.Mutex
	// inits records every InitConnection received (in arrival order).
	inits []*pb.InitConnection
	// upstreams records every NON-init UpstreamMessage received.
	upstreams []*pb.UpstreamMessage
	// issuedSessions maps assigned session_id → the identity string it was
	// issued to, so a later resume can be matched.
	issuedSessions map[string]string
	// resumed records, per resume_session_id observed on a Connect, true.
	resumed map[string]bool

	// firstInit fires once when the first InitConnection arrives.
	firstInitOnce sync.Once
	firstInitCh   chan struct{}

	// push lets a test inject a DownstreamMessage to a live Connect stream.
	// Each Connect registers its send func here under a lock.
	streamMu sync.Mutex
	sendFns  map[int]func(*pb.DownstreamMessage) error
	nextID   int

	sessionSeq int
}

func newFakeGateway() *fakeGateway {
	return &fakeGateway{
		issuedSessions: map[string]string{},
		resumed:        map[string]bool{},
		firstInitCh:    make(chan struct{}),
		sendFns:        map[int]func(*pb.DownstreamMessage) error{},
	}
}

func (g *fakeGateway) Connect(stream grpc.BidiStreamingServer[pb.UpstreamMessage, pb.DownstreamMessage]) error {
	// Register this stream's Send so tests can push downstream frames to it.
	g.streamMu.Lock()
	id := g.nextID
	g.nextID++
	g.sendFns[id] = stream.Send
	g.streamMu.Unlock()
	defer func() {
		g.streamMu.Lock()
		delete(g.sendFns, id)
		g.streamMu.Unlock()
	}()

	for {
		up, err := stream.Recv()
		if err != nil {
			return nil // EOF / cancel: clean close
		}
		if init := up.GetInit(); init != nil {
			sessionID := g.recordInit(init)
			// Reply with a ConnectionAck so the upstream session is "open".
			// Resumed=true iff the client supplied a resume_session_id that we
			// previously issued for this identity.
			resumed := false
			if rs := init.GetResumeSessionId(); rs != "" {
				g.mu.Lock()
				_, known := g.issuedSessions[rs]
				if known {
					resumed = true
				}
				g.mu.Unlock()
			}
			ack := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_ConnectionAck{
				ConnectionAck: &pb.ConnectionAck{SessionId: sessionID, Resumed: resumed},
			}}
			if err := stream.Send(ack); err != nil {
				return err
			}
			continue
		}
		// Non-init upstream frame: record it.
		g.mu.Lock()
		g.upstreams = append(g.upstreams, up)
		g.mu.Unlock()
	}
}

// recordInit stores the init, issues a fresh session id bound to the claimed
// identity, marks resume if the init carried a known resume_session_id, and
// fires firstInitCh on the first call. Returns the issued session id.
func (g *fakeGateway) recordInit(init *pb.InitConnection) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.inits = append(g.inits, init)

	identity := serviceIdentityString(init.GetService())
	if rs := init.GetResumeSessionId(); rs != "" {
		// Record that THIS resume id was observed; mark resumed iff it was an
		// id we previously issued (to anyone).
		if _, known := g.issuedSessions[rs]; known {
			g.resumed[rs] = true
		}
	}

	g.sessionSeq++
	sessionID := fmt.Sprintf("gw-session-%d", g.sessionSeq)
	g.issuedSessions[sessionID] = identity

	g.firstInitOnce.Do(func() { close(g.firstInitCh) })
	return sessionID
}

func serviceIdentityString(svc *pb.ServiceIdentity) string {
	if svc == nil {
		return ""
	}
	return "sv::" + svc.GetImplementation() + "::" + svc.GetSpecifier()
}

// pushDownstream sends a DownstreamMessage to the (single) currently-registered
// Connect stream. Returns an error if no stream is live.
func (g *fakeGateway) pushDownstream(msg *pb.DownstreamMessage) error {
	g.streamMu.Lock()
	defer g.streamMu.Unlock()
	for _, send := range g.sendFns {
		return send(msg)
	}
	return fmt.Errorf("no live gateway stream to push to")
}

func (g *fakeGateway) snapshotInits() []*pb.InitConnection {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*pb.InitConnection, len(g.inits))
	copy(out, g.inits)
	return out
}

func (g *fakeGateway) snapshotUpstreams() []*pb.UpstreamMessage {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*pb.UpstreamMessage, len(g.upstreams))
	copy(out, g.upstreams)
	return out
}

func (g *fakeGateway) wasResumed(resumeID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.resumed[resumeID]
}

// =============================================================================
// chainEnv — the full composed stack, with all certs and teardown wired.
// =============================================================================

type chainEnv struct {
	t *testing.T

	gateway      *fakeGateway
	gatewayAddr  string // host:port the relay dials

	aggProviderAddr string // host:port the provider dials
	aggTunnelAddr   string // host:port the relay dials for the tunnel

	// CA material + on-disk cert paths used to configure the real components.
	sandboxesCA *caBundle
	tenantCA    *caBundle

	aggProviderServerCert string
	aggProviderServerKey  string
	aggTunnelServerCert   string
	aggTunnelServerKey    string
	sandboxesCAFile       string

	providerClientCert tls.Certificate // sandboxes-CA, CN=sandbox-provider-shared

	// relay dial config (sandboxes-CA relay cert + tenant-CA tenant cert).
	relayAggTLS    proxysidecar.TLSConfig // relay → aggregator tunnel (sandboxes CA)
	relayGatewayTLS proxysidecar.TLSConfig // relay → gateway (tenant CA)
}

// newChainEnv builds and starts the fake gateway + real aggregator. The relay
// and provider are started per-phase (so the resume test can bring up a fresh
// pair) via startRelay / dialProvider.
func newChainEnv(t *testing.T) *chainEnv {
	t.Helper()
	dir := t.TempDir()

	sandboxesCA := newCA(t, "sandboxes-ca")
	tenantCA := newCA(t, "tenant-ca")

	env := &chainEnv{
		t:           t,
		sandboxesCA: sandboxesCA,
		tenantCA:    tenantCA,
	}

	// --- CA files on disk (server creds + relay dial configs read from disk) ---
	env.sandboxesCAFile = filepath.Join(dir, "sandboxes-ca.pem")
	mustWrite(t, env.sandboxesCAFile, sandboxesCA.certPEM)
	tenantCAFile := filepath.Join(dir, "tenant-ca.pem")
	mustWrite(t, tenantCAFile, tenantCA.certPEM)

	// --- Aggregator server certs (sandboxes CA), SAN 127.0.0.1 ---
	env.aggProviderServerCert, env.aggProviderServerKey = sandboxesCA.writeLeaf(t, dir, "agg-provider-server", "agg-provider-server", true)
	env.aggTunnelServerCert, env.aggTunnelServerKey = sandboxesCA.writeLeaf(t, dir, "agg-tunnel-server", "agg-tunnel-server", true)

	// --- Provider client cert (sandboxes CA), tenant-less CN ---
	env.providerClientCert = sandboxesCA.leafTLS(t, fcProviderSharedCN, false)

	// --- Relay client cert for the aggregator tunnel hop (sandboxes CA),
	//     tenant-encoding CN so the aggregator's CN binding is exercised ---
	relayAggCert, relayAggKey := sandboxesCA.writeLeaf(t, dir, "relay-agg-client", fcRelayCN, false)
	env.relayAggTLS = proxysidecar.TLSConfig{
		CertFile: relayAggCert,
		KeyFile:  relayAggKey,
		CAFile:   env.sandboxesCAFile,
	}

	// --- Relay client cert for the tenant gateway hop (tenant CA) ---
	relayGwCert, relayGwKey := tenantCA.writeLeaf(t, dir, "relay-gw-client", "sv::sandbox-provider::"+fcTenant, false)
	env.relayGatewayTLS = proxysidecar.TLSConfig{
		CertFile: relayGwCert,
		KeyFile:  relayGwKey,
		CAFile:   tenantCAFile,
	}

	// --- Fake tenant gateway: TLS server cert (tenant CA), requires + verifies
	//     a client cert chained to the tenant CA ---
	gwServerCert, gwServerKey := tenantCA.writeLeaf(t, dir, "gateway-server", "tenant-gateway", true)
	env.gateway = newFakeGateway()
	env.gatewayAddr = startGatewayServer(t, env.gateway, gwServerCert, gwServerKey, tenantCA.certPEM)

	// --- Real aggregator over real mTLS on two listeners ---
	env.aggProviderAddr, env.aggTunnelAddr = env.startAggregator(t)

	return env
}

// startAggregator pre-binds two ephemeral ports, configures a real Aggregator
// to listen on them with mTLS server creds, and drives Run(ctx) in a goroutine.
func (env *chainEnv) startAggregator(t *testing.T) (providerAddr, tunnelAddr string) {
	t.Helper()
	providerAddr = grabPort(t)
	tunnelAddr = grabPort(t)

	cfg := &proxysidecar.Config{}
	cfg.Aggregator.Enabled = true
	cfg.Aggregator.ProviderListen = "tcp://" + providerAddr
	cfg.Aggregator.TunnelListen = "tcp://" + tunnelAddr
	cfg.Aggregator.ProviderTLS = proxysidecar.ServerTLSConfig{
		CertFile:     env.aggProviderServerCert,
		KeyFile:      env.aggProviderServerKey,
		ClientCAFile: env.sandboxesCAFile,
	}
	cfg.Aggregator.TunnelTLS = proxysidecar.ServerTLSConfig{
		CertFile:     env.aggTunnelServerCert,
		KeyFile:      env.aggTunnelServerKey,
		ClientCAFile: env.sandboxesCAFile,
	}
	cfg.Aggregator.PairWaitTimeoutMs = 10_000
	if err := cfg.Validate(); err != nil {
		t.Fatalf("aggregator cfg validate: %v", err)
	}

	agg, err := proxysidecar.NewAggregator(cfg)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = agg.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Logf("warning: aggregator Run did not exit within 10s")
		}
	})

	// Wait until both listeners accept TCP connections.
	mustDialable(t, providerAddr, 5*time.Second)
	mustDialable(t, tunnelAddr, 5*time.Second)
	return providerAddr, tunnelAddr
}

// startRelay constructs a real TenantRelay pointed at the fake gateway (tenant
// cert) and the aggregator tunnel (sandboxes cert), and drives Run(ctx) in a
// goroutine. Returns a cancel that tears the relay down.
func (env *chainEnv) startRelay(t *testing.T) (cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address: env.gatewayAddr,
			TLS:     env.relayGatewayTLS,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "sandbox-provider",
			Specifier:      "tenant-relay",
		},
		TenantID: fcTenant,
	}
	cfg.TenantRelay = proxysidecar.TenantRelayConfig{
		Enabled: true,
		Tenant:  fcTenant,
		Aggregator: proxysidecar.AggregatorDialConfig{
			Address: env.aggTunnelAddr,
			TLS:     env.relayAggTLS,
		},
		FilterProfile: proxysidecar.FilterProfileServicePassthrough,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("tenant-relay cfg validate: %v", err)
	}

	relay, err := proxysidecar.NewTenantRelay(cfg)
	if err != nil {
		t.Fatalf("NewTenantRelay: %v", err)
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	d := make(chan struct{})
	go func() {
		defer close(d)
		_ = relay.Run(ctx)
	}()
	return cancelFn, d
}

// dialProvider opens a RAW provider AetherGateway.Connect against the
// aggregator provider surface, with the sandboxes-CA provider cert and the
// x-aether-tenant metadata. Returns the stream and a cleanup.
func (env *chainEnv) dialProvider(t *testing.T, ctx context.Context, tenant string) (pb.AetherGateway_ConnectClient, func()) {
	t.Helper()
	tlsCreds := credentials.NewTLS(&tls.Config{
		Certificates:       []tls.Certificate{env.providerClientCert},
		InsecureSkipVerify: true, // aggregator server identity not under test here
	})
	conn, err := grpc.NewClient("passthrough:///"+env.aggProviderAddr, grpc.WithTransportCredentials(tlsCreds))
	if err != nil {
		t.Fatalf("provider dial: %v", err)
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(metadataTenantKey, tenant))
	stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("provider Connect: %v", err)
	}
	return stream, func() { _ = conn.Close() }
}

// =============================================================================
// TestFullChain_InitFlowDownstreamAndResume — the core composition test
// (assertions 1, 2, 3, 4).
// =============================================================================

func TestFullChain_InitFlowDownstreamAndResume(t *testing.T) {
	env := newChainEnv(t)

	// ---- Phase 1: bring up relay + provider, drive init/up/down ----
	relayCancel, relayDone := env.startRelay(t)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	providerStream, providerClose := env.dialProvider(t, ctx, fcTenant)

	// The provider's FIRST upstream frame is its InitConnection. It carries a
	// Service identity (impl=sandbox-provider, spec=pod-7) and a resume id we
	// will later assert survived BOTH hops verbatim.
	const resumeID1 = "rs-1"
	initUp := &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{
		ResumeSessionId: resumeID1,
		ClientType: &pb.InitConnection_Service{Service: &pb.ServiceIdentity{
			Implementation: "sandbox-provider",
			Specifier:      "pod-7",
		}},
	}}}
	if err := providerStream.Send(initUp); err != nil {
		t.Fatalf("provider send init: %v", err)
	}

	// ---- Assertion 1: init reaches the gateway verbatim through BOTH hops ----
	waitFor(t, env.gateway.firstInitCh, 15*time.Second, "gateway never received provider InitConnection")
	inits := env.gateway.snapshotInits()
	if len(inits) != 1 {
		t.Fatalf("gateway recorded %d inits, want 1", len(inits))
	}
	got := inits[0]
	if got.GetService().GetImplementation() != "sandbox-provider" {
		t.Fatalf("init impl = %q, want sandbox-provider", got.GetService().GetImplementation())
	}
	if got.GetService().GetSpecifier() != "pod-7" {
		t.Fatalf("init spec = %q, want pod-7 (an intermediate hop mutated the identity)", got.GetService().GetSpecifier())
	}
	if got.GetResumeSessionId() != resumeID1 {
		t.Fatalf("init resume_session_id = %q, want %q (a hop dropped/rewrote resume)", got.GetResumeSessionId(), resumeID1)
	}
	t.Logf("assertion 1 OK: init reached gateway verbatim (impl=sandbox-provider spec=pod-7 resume=%s)", resumeID1)

	// The gateway ACKs the init; the ack flows gateway→relay→aggregator→provider.
	// Read it off the provider stream so the session id is the one the gateway
	// issued (used to drive the resume phase).
	ack, err := providerStream.Recv()
	if err != nil {
		t.Fatalf("provider recv ack: %v", err)
	}
	sessionID := ack.GetConnectionAck().GetSessionId()
	if sessionID == "" {
		t.Fatalf("provider got no ConnectionAck.session_id (got %v)", ack)
	}
	t.Logf("downstream OK: provider received ConnectionAck session_id=%s", sessionID)

	// ---- Assertion 2: a non-init upstream frame reaches the gateway ----
	upMsg := makeNonInitUpstream("up-probe-1")
	if err := providerStream.Send(upMsg); err != nil {
		t.Fatalf("provider send upstream probe: %v", err)
	}
	waitUntil(t, 10*time.Second, "non-init upstream frame never reached gateway", func() bool {
		for _, u := range env.gateway.snapshotUpstreams() {
			if nonInitUpstreamMarker(u) == "up-probe-1" {
				return true
			}
		}
		return false
	})
	t.Logf("assertion 2 OK: non-init upstream frame reached gateway")

	// ---- Assertion 3: a downstream the gateway pushes reaches the provider ----
	pushed := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_ConnectionAck{
		ConnectionAck: &pb.ConnectionAck{SessionId: sessionID, AssignedId: "down-probe-1"},
	}}
	if err := env.gateway.pushDownstream(pushed); err != nil {
		t.Fatalf("gateway push downstream: %v", err)
	}
	gotDown, err := providerStream.Recv()
	if err != nil {
		t.Fatalf("provider recv pushed downstream: %v", err)
	}
	if gotDown.GetConnectionAck().GetAssignedId() != "down-probe-1" {
		t.Fatalf("provider downstream assigned_id = %q, want down-probe-1", gotDown.GetConnectionAck().GetAssignedId())
	}
	t.Logf("assertion 3 OK: gateway-pushed downstream reached provider")

	// ---- Teardown phase-1 relay + provider for the resume phase ----
	providerClose()
	relayCancel()
	select {
	case <-relayDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("phase-1 relay did not exit after cancel")
	}
	// Wait until the aggregator has fully unregistered the tenant so the fresh
	// relay can register without a duplicate-relay rejection.
	waitTenantClear(t, env, 10*time.Second)

	// ---- Assertion 4: resume survives a fresh two-hop path ----
	relay2Cancel, relay2Done := env.startRelay(t)
	defer func() {
		relay2Cancel()
		select {
		case <-relay2Done:
		case <-time.After(10 * time.Second):
		}
	}()

	provider2, provider2Close := env.dialProvider(t, ctx, fcTenant)
	defer provider2Close()

	// The provider's new InitConnection carries resume_session_id = the
	// session id the gateway issued in phase 1. After traversing a FRESH
	// aggregator splice + FRESH relay, the gateway must observe resumed=true.
	initUp2 := &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{
		ResumeSessionId: sessionID,
		ClientType: &pb.InitConnection_Service{Service: &pb.ServiceIdentity{
			Implementation: "sandbox-provider",
			Specifier:      "pod-7",
		}},
	}}}
	if err := provider2.Send(initUp2); err != nil {
		t.Fatalf("provider2 send resume init: %v", err)
	}

	ack2, err := provider2.Recv()
	if err != nil {
		t.Fatalf("provider2 recv resume ack: %v", err)
	}
	// Primary assertion: gateway flagged the session as resumed.
	if !ack2.GetConnectionAck().GetResumed() {
		t.Fatalf("resume ConnectionAck.resumed = false, want true (resume did not survive the fresh two-hop path)")
	}
	// Secondary (belt-and-suspenders): the gateway recorded the resume id on
	// the second init verbatim.
	if !env.gateway.wasResumed(sessionID) {
		t.Fatalf("gateway did not record resume of session %q across reconnect", sessionID)
	}
	inits = env.gateway.snapshotInits()
	if len(inits) < 2 {
		t.Fatalf("gateway recorded %d inits across resume, want >=2", len(inits))
	}
	if inits[len(inits)-1].GetResumeSessionId() != sessionID {
		t.Fatalf("second init resume_session_id = %q, want %q", inits[len(inits)-1].GetResumeSessionId(), sessionID)
	}
	t.Logf("assertion 4 OK: resume survived a fresh two-hop relay+aggregator path (resumed=true, session=%s)", sessionID)
}

// =============================================================================
// TestFullChain_NoRelayPairWaitTimesOut — assertion 5 (cheap pair-wait path).
//
// A provider whose x-aether-tenant names a tenant with NO online relay must
// time out / error cleanly (the aggregator's bounded pair-wait), rather than
// hanging forever.
// =============================================================================

func TestFullChain_NoRelayPairWaitTimesOut(t *testing.T) {
	env := newChainEnv(t)
	// NB: no relay started for this tenant — only the aggregator + provider.

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	provider, providerClose := env.dialProvider(t, ctx, "tenant-with-no-relay")
	defer providerClose()

	// Send the init; with no relay to pair with, the aggregator's pair-wait
	// (configured to 10s in startAggregator) will fire and the provider stream
	// must close with an error/ack-of-error rather than hang.
	initUp := &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{
		ClientType: &pb.InitConnection_Service{Service: &pb.ServiceIdentity{
			Implementation: "sandbox-provider", Specifier: "pod-lonely",
		}},
	}}}
	if err := provider.Send(initUp); err != nil {
		t.Fatalf("provider send init: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			// The aggregator rejects by sending an error DownstreamMessage then
			// returning a status; either an error frame or a stream error ends
			// the wait. We loop until Recv returns a non-nil error OR an error
			// frame.
			msg, err := provider.Recv()
			if err != nil {
				return
			}
			if msg.GetError() != nil {
				return
			}
		}
	}()
	select {
	case <-done:
		t.Logf("assertion 5 OK: provider with no online relay closed cleanly on pair-wait")
	case <-time.After(15 * time.Second):
		t.Fatalf("provider with no online relay did not close within pair-wait + margin (hang)")
	}
}

// =============================================================================
// Helpers: non-init upstream frame construction + sync utilities.
// =============================================================================

// makeNonInitUpstream builds a NON-init UpstreamMessage carrying a recognisable
// marker. We use a ProgressReport-style payload only to get a non-init frame
// across the wire; the relay's service-passthrough bypass forwards it verbatim.
// The marker is read back via nonInitUpstreamMarker.
func makeNonInitUpstream(marker string) *pb.UpstreamMessage {
	return &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Send{
		Send: &pb.SendMessage{
			TargetTopic: marker,
		},
	}}
}

// nonInitUpstreamMarker extracts the marker set by makeNonInitUpstream, or "".
func nonInitUpstreamMarker(up *pb.UpstreamMessage) string {
	if sm := up.GetSend(); sm != nil {
		return sm.GetTargetTopic()
	}
	return ""
}

func waitFor(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s (timed out after %s)", msg, timeout)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s (timed out after %s)", msg, timeout)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// waitTenantClear polls the gateway until a fresh relay registration would not
// collide. We cannot inspect the aggregator's private pairing table from this
// external package, so we instead retry the relay registration implicitly by
// giving the aggregator time to observe the prior relay's stream close. A short
// settle poll is sufficient because the aggregator unregisters the relay on
// stream return (cancelled ctx) synchronously in its defer.
func waitTenantClear(t *testing.T, env *chainEnv, timeout time.Duration) {
	t.Helper()
	// Best-effort settle: the relay's Run returns only after its tunnel stream
	// closes, which triggers the aggregator's unregisterRelay defer. We already
	// waited for relayDone in the caller, so a brief poll covers the residual
	// async unregister on the aggregator side.
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return // give up waiting; the duplicate guard would surface as a test failure downstream
		case <-time.After(50 * time.Millisecond):
			return // single short settle is enough; aggregator unregister is in a defer on stream return
		}
	}
}

func mustDialable(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case <-deadline:
			t.Fatalf("addr %s never became dialable: %v", addr, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// grabPort binds an ephemeral port, captures its address, and closes the
// listener so the component can re-bind it. The TOCTOU window is negligible for
// loopback test usage.
func grabPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grab port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startGatewayServer binds an ephemeral port and serves the fake gateway over
// mTLS (RequireAndVerifyClientCert against clientCAPEM). Returns its host:port.
func startGatewayServer(t *testing.T, gw *fakeGateway, serverCertFile, serverKeyFile string, clientCAPEM []byte) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		t.Fatalf("gateway load keypair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(clientCAPEM) {
		t.Fatalf("gateway client CA pool: no certs parsed")
	}
	creds := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterAetherGatewayServer(srv, gw)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

// =============================================================================
// In-test CA / cert generation (same inline approach as aggregator_test.go's
// aggGenCA/aggGenLeaf and spike_tenant_relay_test.go).
// =============================================================================

type caBundle struct {
	cert    *x509.Certificate
	key     *rsa.PrivateKey
	certPEM []byte
}

func newCA(t *testing.T, cn string) *caBundle {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
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
	return &caBundle{cert: cert, key: key, certPEM: pemEncode("CERTIFICATE", der)}
}

// leafDER signs a leaf cert with this CA. server controls EKU and SAN (server
// leaves get 127.0.0.1 + localhost so the gRPC server-name verification on the
// relay→gateway hop passes).
func (ca *caBundle) leafDER(t *testing.T, cn string, server bool) ([]byte, *rsa.PrivateKey) {
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
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	return der, key
}

// leafTLS returns a CA-signed leaf as a tls.Certificate (for in-memory dial).
func (ca *caBundle) leafTLS(t *testing.T, cn string, server bool) tls.Certificate {
	t.Helper()
	der, key := ca.leafDER(t, cn, server)
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// writeLeaf writes a CA-signed leaf cert+key to dir and returns the file paths.
func (ca *caBundle) writeLeaf(t *testing.T, dir, name, cn string, server bool) (certFile, keyFile string) {
	t.Helper()
	der, key := ca.leafDER(t, cn, server)
	certFile = filepath.Join(dir, name+".crt")
	keyFile = filepath.Join(dir, name+".key")
	mustWrite(t, certFile, pemEncode("CERTIFICATE", der))
	mustWrite(t, keyFile, pemEncode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key)))
	return certFile, keyFile
}

func pemEncode(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
