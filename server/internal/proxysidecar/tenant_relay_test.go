package proxysidecar

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeAggregator implements pb.SandboxRelayTunnelServer for tenant-relay tests.
// Its Tunnel handler accepts the relay's TunnelHello, pushes a provider
// InitConnection (with a non-empty resume_session_id and a Service identity) as
// the first up frame, optionally pushes additional up frames the test stages,
// and records every down frame the relay sends back.
type fakeAggregator struct {
	pb.UnimplementedSandboxRelayTunnelServer

	// upFrames are sent (in order) after the relay's hello is received. The
	// first must carry the provider InitConnection.
	upFrames []*pb.TunnelFrame

	// eofAfterSend, when true, makes Tunnel return (closing its send-half)
	// after pushing upFrames instead of looping on Recv. The relay's pumpUp
	// then observes io.EOF on tunnel.Recv() → runPumps returns nil, exercising
	// the clean tunnel-EOF teardown path.
	eofAfterSend bool

	// eofGate, when non-nil, is awaited before the eofAfterSend half-close so a
	// test can order its assertions ahead of the relay's teardown. It must be
	// set before the harness starts. Without it the half-close races Run's
	// return, and Run's deferred gateway conn.Close() can tear the connection
	// down before the gateway server ever dispatches its Connect handler —
	// making any server-side assertion (awaitGatewayStream) flaky under load.
	eofGate chan struct{}

	mu       sync.Mutex
	hello    *pb.TunnelHello
	downs    []*pb.DownstreamMessage
	streamCh chan struct{} // signalled once the hello has been received
}

func newFakeAggregator(upFrames []*pb.TunnelFrame) *fakeAggregator {
	return &fakeAggregator{upFrames: upFrames, streamCh: make(chan struct{}, 1)}
}

func (a *fakeAggregator) Tunnel(stream grpc.BidiStreamingServer[pb.TunnelFrame, pb.TunnelFrame]) error {
	// First frame from the relay MUST be a TunnelHello.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return errors.New("fakeAggregator: first frame was not a hello")
	}
	a.mu.Lock()
	a.hello = hello
	a.mu.Unlock()
	select {
	case a.streamCh <- struct{}{}:
	default:
	}

	// Push the staged provider up frames (init first, then any follow-ups).
	for _, f := range a.upFrames {
		if err := stream.Send(f); err != nil {
			return err
		}
	}

	if a.eofAfterSend {
		// Hold the half-close until the test releases it, so server-side
		// assertions can run before the relay tears down (see eofGate).
		if a.eofGate != nil {
			select {
			case <-a.eofGate:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		// Returning closes the server's send-half; the relay's pumpUp sees
		// io.EOF on tunnel.Recv().
		return nil
	}

	// Record down frames until the relay closes the stream.
	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if down := frame.GetDown(); down != nil {
			a.mu.Lock()
			a.downs = append(a.downs, down)
			a.mu.Unlock()
		}
	}
}

func (a *fakeAggregator) snapshotDowns() []*pb.DownstreamMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*pb.DownstreamMessage, len(a.downs))
	copy(out, a.downs)
	return out
}

func (a *fakeAggregator) helloTenant() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hello == nil {
		return ""
	}
	return a.hello.GetTenant()
}

// providerInit builds the provider's InitConnection as it would arrive over the
// tunnel: a Service identity (impl=sandbox-provider, spec=pod-7) and a
// non-empty resume_session_id. The tenant-relay must forward this VERBATIM —
// neither field may be rewritten or discarded.
func providerInit() *pb.UpstreamMessage {
	return &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Service{
					Service: &pb.ServiceIdentity{
						Implementation: "sandbox-provider",
						Specifier:      "pod-7",
					},
				},
				Credentials:     map[string]string{"api_key": "provider-key"},
				ResumeSessionId: "resume-xyz",
			},
		},
	}
}

// tenantRelayHarness wires a fakeGateway (the local tenant gateway), a real
// TenantRelay with injected dialers, and a fakeAggregator the relay tunnels
// through. The TenantRelay's Run loop is started in a goroutine and cancelled
// via t.Cleanup.
type tenantRelayHarness struct {
	t          *testing.T
	gateway    *fakeGateway
	gatewaySrv *grpc.Server
	gatewayLis net.Listener
	aggregator *fakeAggregator
	relay      *TenantRelay
	runErr     chan error
	cancel     context.CancelFunc
}

func newTenantRelayHarness(t *testing.T, upFrames []*pb.TunnelFrame) *tenantRelayHarness {
	t.Helper()
	return newTenantRelayHarnessAgg(t, newFakeAggregator(upFrames))
}

func newTenantRelayHarnessAgg(t *testing.T, aggregator *fakeAggregator) *tenantRelayHarness {
	t.Helper()

	// 1. Local tenant gateway (reused fakeGateway from relay_test.go).
	gateway := newFakeGateway()
	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	gwSrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(gwSrv, gateway)
	go func() { _ = gwSrv.Serve(gwLis) }()

	// 2. Aggregator tunnel surface.
	aggLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen aggregator: %v", err)
	}
	aggSrv := grpc.NewServer()
	pb.RegisterSandboxRelayTunnelServer(aggSrv, aggregator)
	go func() { _ = aggSrv.Serve(aggLis) }()

	// 3. Build a validated tenant-relay config pointing at both fakes.
	cfg := &Config{
		Gateway: GatewayConfig{
			Address:  gwLis.Addr().String(),
			Insecure: true,
		},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "instance-1"},
		TenantRelay: TenantRelayConfig{
			Enabled: true,
			Tenant:  "acme",
			Aggregator: AggregatorDialConfig{
				Address:  aggLis.Addr().String(),
				Insecure: true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate cfg: %v", err)
	}

	relay, err := NewTenantRelay(cfg)
	if err != nil {
		t.Fatalf("NewTenantRelay: %v", err)
	}
	// Inject dialers that dial the in-process fakes over plaintext gRPC.
	relay.gatewayDialer = func(_ context.Context) (pb.AetherGatewayClient, func() error, error) {
		conn, derr := grpc.NewClient(gwLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if derr != nil {
			return nil, nil, derr
		}
		return pb.NewAetherGatewayClient(conn), conn.Close, nil
	}
	relay.tunnelDialer = func(_ context.Context) (pb.SandboxRelayTunnelClient, func() error, error) {
		conn, derr := grpc.NewClient(aggLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if derr != nil {
			return nil, nil, derr
		}
		return pb.NewSandboxRelayTunnelClient(conn), conn.Close, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- relay.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		aggSrv.GracefulStop()
		_ = aggLis.Close()
		gwSrv.GracefulStop()
		_ = gwLis.Close()
	})

	return &tenantRelayHarness{
		t:          t,
		gateway:    gateway,
		gatewaySrv: gwSrv,
		gatewayLis: gwLis,
		aggregator: aggregator,
		relay:      relay,
		runErr:     runErr,
		cancel:     cancel,
	}
}

// awaitGatewayStream blocks until the fake gateway records a new accepted
// stream and returns it.
func (h *tenantRelayHarness) awaitGatewayStream() *fakeGatewayStream {
	h.t.Helper()
	select {
	case s := <-h.gateway.streamCh:
		return s
	case <-time.After(15 * time.Second):
		// Generous: a passing test returns the instant the gateway server
		// dispatches Connect, so this only bounds a genuinely stuck run.
		// NOTE: raising this bound does not fix ordering bugs. A caller must not
		// let the relay's Run return before this call — Run's deferred
		// conn.Close() can tear the gateway connection down before the server
		// dispatches Connect, in which case no timeout is long enough (see
		// eofGate).
		h.t.Fatalf("timed out waiting for gateway stream")
		return nil
	}
}

// awaitGatewayMessages polls the gateway stream until it has observed at least
// n messages or the deadline elapses.
func awaitGatewayMessages(t *testing.T, s *fakeGatewayStream, n int) []*pb.UpstreamMessage {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap := s.snapshot()
		if len(snap) >= n {
			return snap
		}
		time.Sleep(20 * time.Millisecond)
	}
	return s.snapshot()
}

// =============================================================================
// Tests
// =============================================================================

// TestTenantRelay_ForwardsInitVerbatim is the key regression vs relay.go: the
// provider's InitConnection must reach the tenant gateway UNTOUCHED — the
// resume_session_id is preserved AND the Service identity is unchanged (no
// rewrite to the sidecar's own Service identity, no resume discard).
func TestTenantRelay_ForwardsInitVerbatim(t *testing.T) {
	up := []*pb.TunnelFrame{
		{F: &pb.TunnelFrame_Up{Up: providerInit()}},
	}
	h := newTenantRelayHarness(t, up)

	gwStream := h.awaitGatewayStream()
	snap := awaitGatewayMessages(t, gwStream, 1)
	if len(snap) < 1 {
		t.Fatalf("expected the gateway to receive the forwarded init, saw %d messages", len(snap))
	}

	initMsg, ok := snap[0].GetPayload().(*pb.UpstreamMessage_Init)
	if !ok {
		t.Fatalf("first gateway message is %T, expected Init", snap[0].GetPayload())
	}

	// Dedicated assertion 1: resume_session_id preserved verbatim (relay.go
	// discards this; tenant-relay must not).
	if got := initMsg.Init.GetResumeSessionId(); got != "resume-xyz" {
		t.Fatalf("resume_session_id = %q; want \"resume-xyz\" (must be forwarded verbatim, not discarded)", got)
	}

	// Dedicated assertion 2: the Service identity is unchanged (relay.go
	// rewrites every init to the sidecar's own Service identity; tenant-relay
	// must not touch it).
	svc, ok := initMsg.Init.GetClientType().(*pb.InitConnection_Service)
	if !ok {
		t.Fatalf("forwarded init client_type is %T, expected the provider's Service identity", initMsg.Init.GetClientType())
	}
	if svc.Service.GetImplementation() != "sandbox-provider" || svc.Service.GetSpecifier() != "pod-7" {
		t.Fatalf("forwarded init identity = %s/%s; want sandbox-provider/pod-7 (must not be rewritten)",
			svc.Service.GetImplementation(), svc.Service.GetSpecifier())
	}

	// And the announced tenant reached the aggregator hello.
	if got := h.aggregator.helloTenant(); got != "acme" {
		t.Fatalf("aggregator hello tenant = %q; want \"acme\"", got)
	}
}

// TestTenantRelay_BidirectionalForwarding confirms subsequent up frames reach
// the gateway and down frames from the gateway reach the aggregator tunnel.
func TestTenantRelay_BidirectionalForwarding(t *testing.T) {
	// Init frame followed by two additional up frames the gateway should see.
	send := func(topic string) *pb.TunnelFrame {
		return &pb.TunnelFrame{F: &pb.TunnelFrame_Up{Up: &pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_Send{Send: &pb.SendMessage{
				TargetTopic: topic,
				Payload:     []byte("hello"),
				MessageType: pb.MessageType_OPAQUE,
			}},
		}}}
	}
	up := []*pb.TunnelFrame{
		{F: &pb.TunnelFrame_Up{Up: providerInit()}},
		send("ag.ws.impl.spec-1"),
		send("ag.ws.impl.spec-2"),
	}
	h := newTenantRelayHarness(t, up)

	gwStream := h.awaitGatewayStream()

	// Init + the two SendMessage frames must all reach the gateway.
	snap := awaitGatewayMessages(t, gwStream, 3)
	if len(snap) < 3 {
		t.Fatalf("expected 3 forwarded messages, saw %d", len(snap))
	}
	var sends []string
	for _, m := range snap {
		if s, ok := m.GetPayload().(*pb.UpstreamMessage_Send); ok {
			sends = append(sends, s.Send.GetTargetTopic())
		}
	}
	if len(sends) != 2 || sends[0] != "ag.ws.impl.spec-1" || sends[1] != "ag.ws.impl.spec-2" {
		t.Fatalf("forwarded SendMessage topics = %v; want [ag.ws.impl.spec-1 ag.ws.impl.spec-2]", sends)
	}

	// Now push a downstream envelope from the gateway; it must reach the
	// aggregator tunnel as a down frame. (The fakeGateway already sent a
	// ConnectionAck on accept, so we expect at least that plus our error.)
	if err := gwStream.server.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: &pb.ErrorResponse{Code: "DOWN_TEST", Message: "from gateway"},
		},
	}); err != nil {
		t.Fatalf("gateway send downstream: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	var sawErr bool
	for time.Now().Before(deadline) {
		for _, d := range h.aggregator.snapshotDowns() {
			if e, ok := d.GetPayload().(*pb.DownstreamMessage_Error); ok && e.Error.GetCode() == "DOWN_TEST" {
				sawErr = true
			}
		}
		if sawErr {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawErr {
		t.Fatalf("downstream envelope from gateway never reached the aggregator tunnel")
	}
}

// TestTenantRelay_CleanTeardownOnCtxCancel confirms Run returns promptly and
// without panic when ctx is cancelled (the runner's shutdown path). The pumps'
// gateway/tunnel Recv calls surface a gRPC Canceled status rather than a clean
// EOF — mirroring relaySession.run, which special-cases only io.EOF — so the
// surface returns a Canceled error. We assert it is exactly that and that the
// teardown is bounded.
func TestTenantRelay_CleanTeardownOnCtxCancel(t *testing.T) {
	up := []*pb.TunnelFrame{
		{F: &pb.TunnelFrame_Up{Up: providerInit()}},
	}
	h := newTenantRelayHarness(t, up)

	// Wait until the init has been spliced through to the gateway.
	gwStream := h.awaitGatewayStream()
	_ = awaitGatewayMessages(t, gwStream, 1)

	// Cancel ctx to drive a clean shutdown; Run should return promptly.
	h.cancel()

	select {
	case err := <-h.runErr:
		if err != nil && !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("Run returned unexpected error on ctx-cancel teardown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return within teardown deadline")
	}
}

// TestTenantRelay_TunnelEOFReturnsNil drives the EOF→nil path: the aggregator
// pushes the provider init and then returns from Tunnel (eofAfterSend),
// half-closing the tunnel. The relay's pumpUp observes io.EOF on tunnel.Recv()
// and runPumps maps EOF→nil, so Run returns nil with ctx still live.
func TestTenantRelay_TunnelEOFReturnsNil(t *testing.T) {
	agg := newFakeAggregator([]*pb.TunnelFrame{
		{F: &pb.TunnelFrame_Up{Up: providerInit()}},
	})
	agg.eofAfterSend = true
	// Gate the half-close: assert the init reached the gateway FIRST, then let
	// the tunnel EOF. Ungated, the EOF races Run's return, whose deferred
	// gateway conn.Close() can beat the gateway server's dispatch of Connect
	// entirely — so the stream the relay demonstrably opened is never observed
	// server-side. That is a test-ordering bug, not slowness: it reproduces at
	// any timeout under CPU starvation.
	agg.eofGate = make(chan struct{})
	h := newTenantRelayHarnessAgg(t, agg)

	gwStream := h.awaitGatewayStream()
	_ = awaitGatewayMessages(t, gwStream, 1)

	close(agg.eofGate)

	select {
	case err := <-h.runErr:
		if err != nil {
			t.Fatalf("Run returned %v on clean tunnel EOF; want nil (EOF→nil)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after tunnel EOF")
	}
}
