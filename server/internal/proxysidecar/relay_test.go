package proxysidecar

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeGateway implements pb.AetherGatewayServer for tests. Each accepted
// stream records every message it receives and exposes a channel the test
// can use to push downstream frames at the relay.
type fakeGateway struct {
	pb.UnimplementedAetherGatewayServer

	mu       sync.Mutex
	streams  []*fakeGatewayStream
	streamCh chan *fakeGatewayStream
}

type fakeGatewayStream struct {
	server pb.AetherGateway_ConnectServer

	mu       sync.Mutex
	received []*pb.UpstreamMessage
	closed   bool
}

func newFakeGateway() *fakeGateway {
	return &fakeGateway{streamCh: make(chan *fakeGatewayStream, 4)}
}

func (g *fakeGateway) Connect(stream pb.AetherGateway_ConnectServer) error {
	s := &fakeGatewayStream{server: stream}
	g.mu.Lock()
	g.streams = append(g.streams, s)
	g.mu.Unlock()
	g.streamCh <- s

	// Send a ConnectionAck immediately so a real client would see the
	// session as established. Tests can ignore it.
	if err := stream.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{SessionId: "fake-session"},
		},
	}); err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		s.received = append(s.received, msg)
		s.mu.Unlock()
	}
}

func (s *fakeGatewayStream) snapshot() []*pb.UpstreamMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.UpstreamMessage, len(s.received))
	copy(out, s.received)
	return out
}

// relayHarness wires a fake gateway, a real Relay configured to forward
// to it, and a sandbox-side gRPC client.
type relayHarness struct {
	t           *testing.T
	gateway     *fakeGateway
	gatewayLis  net.Listener
	gatewaySrv  *grpc.Server
	relay       *Relay
	relayLis    net.Listener
	relaySrv    *grpc.Server
	sandboxConn *grpc.ClientConn
	sandboxCli  pb.AetherGatewayClient
}

func newRelayHarness(t *testing.T, cfg *Config) *relayHarness {
	t.Helper()

	// 1. Spin up the fake gateway.
	gateway := newFakeGateway()
	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	gwSrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(gwSrv, gateway)
	go func() { _ = gwSrv.Serve(gwLis) }()

	// 2. Build the Relay configured to forward at the fake gateway.
	cfg.Relay.Enabled = true
	cfg.Gateway.Address = gwLis.Addr().String()
	cfg.Gateway.Insecure = true
	cfg.Service.Implementation = "sidecar"
	cfg.Service.Specifier = "instance-1"
	if cfg.Relay.Listen == "" {
		cfg.Relay.Listen = filepath.Join(t.TempDir(), "relay.sock")
		cfg.Relay.Listen = "unix://" + cfg.Relay.Listen
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate cfg: %v", err)
	}

	r, err := NewRelay(cfg)
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	// 3. Open the relay's local listener and start the gRPC server
	//    manually so we control lifecycle inside the test.
	relayLis, cleanup, err := openRelayListener(cfg.Relay.Listen)
	if err != nil {
		t.Fatalf("open relay listener: %v", err)
	}
	relaySrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(relaySrv, r)
	go func() { _ = relaySrv.Serve(relayLis) }()

	// 4. Connect a sandbox-side client to the relay.
	addr := relayLis.Addr().String()
	scheme := "passthrough:///"
	if relayLis.Addr().Network() == "unix" {
		scheme = "unix://"
	}
	conn, err := grpc.NewClient(scheme+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	cli := pb.NewAetherGatewayClient(conn)

	t.Cleanup(func() {
		_ = conn.Close()
		relaySrv.GracefulStop()
		_ = relayLis.Close()
		if cleanup != nil {
			cleanup()
		}
		gwSrv.GracefulStop()
		_ = gwLis.Close()
	})

	return &relayHarness{
		t:           t,
		gateway:     gateway,
		gatewayLis:  gwLis,
		gatewaySrv:  gwSrv,
		relay:       r,
		relayLis:    relayLis,
		relaySrv:    relaySrv,
		sandboxConn: conn,
		sandboxCli:  cli,
	}
}

// awaitGatewayStream blocks until the fake gateway records a new
// accepted stream and returns it.
func (h *relayHarness) awaitGatewayStream() *fakeGatewayStream {
	h.t.Helper()
	select {
	case s := <-h.gateway.streamCh:
		return s
	case <-time.After(3 * time.Second):
		h.t.Fatalf("timed out waiting for gateway stream")
		return nil
	}
}

func sandboxInit() *pb.UpstreamMessage {
	return &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Agent{
					Agent: &pb.AgentIdentity{
						Workspace:      "sandbox-claimed-ws",
						Implementation: "sandbox-claimed-impl",
						Specifier:      "sandbox-claimed-spec",
					},
				},
				Credentials:     map[string]string{"api_key": "sandbox-fake-key"},
				ResumeSessionId: "sandbox-claimed-resume",
			},
		},
	}
}

// =============================================================================
// Tests
// =============================================================================

func TestRelay_ForwardsRoundTrip(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
		},
	}
	h := newRelayHarness(t, cfg)

	stream, err := h.sandboxCli.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(sandboxInit()); err != nil {
		t.Fatalf("send init: %v", err)
	}
	gwStream := h.awaitGatewayStream()

	// Send a SendMessage envelope from the sandbox; expect to see it on
	// the fake gateway side.
	out := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: "ag.workspace.demo.spec",
				Payload:     []byte("hello"),
				MessageType: pb.MessageType_OPAQUE,
			},
		},
	}
	if err := stream.Send(out); err != nil {
		t.Fatalf("send msg: %v", err)
	}

	// Wait for the fake gateway to observe both the (rewritten) Init and
	// the forwarded SendMessage.
	deadline := time.Now().Add(2 * time.Second)
	var snap []*pb.UpstreamMessage
	for time.Now().Before(deadline) {
		snap = gwStream.snapshot()
		if len(snap) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(snap) < 2 {
		t.Fatalf("expected 2 forwarded messages, saw %d", len(snap))
	}

	// First message must be the rewritten Init.
	initMsg, ok := snap[0].GetPayload().(*pb.UpstreamMessage_Init)
	if !ok {
		t.Fatalf("first message is %T, expected Init", snap[0].GetPayload())
	}
	svc, ok := initMsg.Init.GetClientType().(*pb.InitConnection_Service)
	if !ok {
		t.Fatalf("rewritten init client_type is %T, expected Service", initMsg.Init.GetClientType())
	}
	if svc.Service.GetImplementation() != "sidecar" || svc.Service.GetSpecifier() != "instance-1" {
		t.Fatalf("rewritten init identity = %s/%s; want sidecar/instance-1",
			svc.Service.GetImplementation(), svc.Service.GetSpecifier())
	}
	if initMsg.Init.GetResumeSessionId() != "" {
		t.Fatalf("sandbox-supplied resume_session_id was forwarded: %q", initMsg.Init.GetResumeSessionId())
	}

	send, ok := snap[1].GetPayload().(*pb.UpstreamMessage_Send)
	if !ok {
		t.Fatalf("second message is %T, expected Send", snap[1].GetPayload())
	}
	if send.Send.GetTargetTopic() != "ag.workspace.demo.spec" {
		t.Fatalf("forwarded target_topic = %q; want unchanged", send.Send.GetTargetTopic())
	}
}

func TestRelay_FilterRejectsDisallowedOp(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, err := h.sandboxCli.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(sandboxInit()); err != nil {
		t.Fatalf("send init: %v", err)
	}
	gwStream := h.awaitGatewayStream()

	// Drain the ConnectionAck the fake gateway sent.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("recv ack: %v", err)
	}

	// ProxyHttpRequest is NOT in sandbox-default → must be denied.
	denied := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId:   "req-1",
				TargetTopic: "ag.ws.impl.spec",
				Method:      "GET",
				Path:        "/",
			},
		},
	}
	if err := stream.Send(denied); err != nil {
		t.Fatalf("send denied: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv error frame: %v", err)
	}
	errResp, ok := resp.GetPayload().(*pb.DownstreamMessage_Error)
	if !ok {
		t.Fatalf("expected ErrorResponse, got %T", resp.GetPayload())
	}
	if errResp.Error.GetCode() != "RELAY_OP_DENIED" {
		t.Fatalf("error code = %q; want RELAY_OP_DENIED", errResp.Error.GetCode())
	}

	// And the gateway must NOT have seen the denied envelope. It saw
	// only the rewritten init.
	time.Sleep(50 * time.Millisecond)
	for _, m := range gwStream.snapshot() {
		if _, ok := m.GetPayload().(*pb.UpstreamMessage_ProxyHttpRequest); ok {
			t.Fatalf("denied ProxyHttpRequest was forwarded to gateway")
		}
	}
}

func TestRelay_TargetClampReject(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxTunnels, Set: true},
			TargetTopicClamp: TargetClampConfig{
				Mode:           TargetClampReject,
				AllowedTargets: []string{"ag.allowed-ws.allowed-impl.*"},
			},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, _ := h.sandboxCli.Connect(context.Background())
	_ = stream.Send(sandboxInit())
	_ = h.awaitGatewayStream()
	_, _ = stream.Recv() // drain ack

	bad := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId:   "req-bad",
				TargetTopic: "ag.other-ws.thing.spec",
			},
		},
	}
	_ = stream.Send(bad)
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	errResp, ok := resp.GetPayload().(*pb.DownstreamMessage_Error)
	if !ok || errResp.Error.GetCode() != "RELAY_TARGET_DENIED" {
		t.Fatalf("expected RELAY_TARGET_DENIED, got %#v", resp.GetPayload())
	}
}

func TestRelay_TargetClampRewriteFirstMatch(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxTunnels, Set: true},
			TargetTopicClamp: TargetClampConfig{
				Mode: TargetClampRewriteFirstMatch,
				// First entry is concrete and becomes the rewrite target.
				AllowedTargets: []string{"ag.canonical.thing.spec", "ag.canonical.*"},
			},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, _ := h.sandboxCli.Connect(context.Background())
	_ = stream.Send(sandboxInit())
	gwStream := h.awaitGatewayStream()
	_, _ = stream.Recv()

	out := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId:   "req-rw",
				TargetTopic: "ag.attacker.thing.spec",
			},
		},
	}
	if err := stream.Send(out); err != nil {
		t.Fatalf("send: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var found *pb.ProxyHttpRequest
	for time.Now().Before(deadline) {
		for _, m := range gwStream.snapshot() {
			if p, ok := m.GetPayload().(*pb.UpstreamMessage_ProxyHttpRequest); ok {
				found = p.ProxyHttpRequest
				break
			}
		}
		if found != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if found == nil {
		t.Fatalf("ProxyHttpRequest was not forwarded after rewrite")
	}
	if found.GetTargetTopic() != "ag.canonical.thing.spec" {
		t.Fatalf("rewritten target_topic = %q; want ag.canonical.thing.spec", found.GetTargetTopic())
	}
}

func TestRelay_HopDepthClampHybridFloor(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxTunnels, Set: true},
			TargetTopicClamp: TargetClampConfig{
				Mode:           TargetClampReject,
				AllowedTargets: []string{"ag.ws.impl.*"},
			},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, _ := h.sandboxCli.Connect(context.Background())
	_ = stream.Send(sandboxInit())
	gwStream := h.awaitGatewayStream()
	_, _ = stream.Recv()

	// Push an inbound ProxyHttpRequest with chain depth 3 — relay sees
	// this on the downstream side via the gateway stream.
	if err := gwStream.server.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId:       "inbound",
				TargetTopic:     "ag.ws.impl.spec",
				ProxyChainDepth: 3,
			},
		},
	}); err != nil {
		t.Fatalf("gw send inbound: %v", err)
	}
	// Wait for the sandbox to receive it — that confirms the relay's
	// pumpDownstream observed and recorded the depth.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("recv inbound: %v", err)
	}

	// Sandbox now claims depth=1 on an outbound proxy envelope. Relay
	// should clamp it up to max(1, 3+1) = 4.
	out := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId:       "outbound",
				TargetTopic:     "ag.ws.impl.target",
				ProxyChainDepth: 1,
			},
		},
	}
	if err := stream.Send(out); err != nil {
		t.Fatalf("send outbound: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var depth uint32
	var seen bool
	for time.Now().Before(deadline) {
		for _, m := range gwStream.snapshot() {
			if p, ok := m.GetPayload().(*pb.UpstreamMessage_ProxyHttpRequest); ok {
				if p.ProxyHttpRequest.GetRequestId() == "outbound" {
					depth = p.ProxyHttpRequest.GetProxyChainDepth()
					seen = true
				}
			}
		}
		if seen {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("outbound envelope never reached gateway")
	}
	if depth != 4 {
		t.Fatalf("clamped depth = %d; want 4 (max(claim=1, observed+1=4))", depth)
	}
}

func TestRelay_HopDepthClampClaimAboveFloor(t *testing.T) {
	if got := hybridFloor(7, 3); got != 7 {
		t.Fatalf("hybridFloor(7,3) = %d; want 7", got)
	}
	if got := hybridFloor(0, 0); got != 1 {
		// observed 0 → floor 1; sandbox claim 0 → 1 wins.
		t.Fatalf("hybridFloor(0,0) = %d; want 1", got)
	}
	if got := hybridFloor(2, 5); got != 6 {
		t.Fatalf("hybridFloor(2,5) = %d; want 6", got)
	}
}

func TestRelay_StreamLifecycleSandboxClose(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, _ := h.sandboxCli.Connect(context.Background())
	_ = stream.Send(sandboxInit())
	gwStream := h.awaitGatewayStream()
	_, _ = stream.Recv()

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// The gateway-side stream should observe EOF on the relay's
	// upstream forwarder shortly after.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		gwStream.mu.Lock()
		closed := gwStream.closed
		gwStream.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("gateway side did not observe sandbox close within deadline")
}

func TestRelay_DoubleInitDeniedNotForwarded(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, _ := h.sandboxCli.Connect(context.Background())
	_ = stream.Send(sandboxInit())
	gwStream := h.awaitGatewayStream()
	_, _ = stream.Recv()

	if err := stream.Send(sandboxInit()); err != nil {
		t.Fatalf("send second init: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	errResp, ok := resp.GetPayload().(*pb.DownstreamMessage_Error)
	if !ok || errResp.Error.GetCode() != "RELAY_DOUBLE_INIT" {
		t.Fatalf("expected RELAY_DOUBLE_INIT, got %#v", resp.GetPayload())
	}

	// Only one Init should have hit the gateway.
	count := 0
	for _, m := range gwStream.snapshot() {
		if _, ok := m.GetPayload().(*pb.UpstreamMessage_Init); ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("gateway saw %d inits; want 1", count)
	}
}

func TestRelay_AllowedOpsToolStubOnly(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileToolStubOnly, Set: true},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, _ := h.sandboxCli.Connect(context.Background())
	_ = stream.Send(sandboxInit())
	_ = h.awaitGatewayStream()
	_, _ = stream.Recv()

	// Even SendMessage is denied.
	_ = stream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: "ag.ws.impl.spec",
				Payload:     []byte("nope"),
				MessageType: pb.MessageType_OPAQUE,
			},
		},
	})
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	errResp, ok := resp.GetPayload().(*pb.DownstreamMessage_Error)
	if !ok || errResp.Error.GetCode() != "RELAY_OP_DENIED" {
		t.Fatalf("expected RELAY_OP_DENIED, got %#v", resp.GetPayload())
	}
}

// TestRelay_BackpressureSurfacesToSandbox confirms that when the upstream
// gateway stream is broken, the sandbox sees the error rather than
// hanging indefinitely.
func TestRelay_BackpressureSurfacesToSandbox(t *testing.T) {
	cfg := &Config{
		Relay: RelayConfig{
			AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
		},
	}
	h := newRelayHarness(t, cfg)
	stream, err := h.sandboxCli.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = stream.Send(sandboxInit())
	_ = h.awaitGatewayStream()
	_, _ = stream.Recv()

	// Tear down the gateway server: forces the relay's outbound stream
	// to error.
	h.gatewaySrv.Stop()
	_ = h.gatewayLis.Close()

	// Subsequent sends from the sandbox should eventually surface as an
	// error on Recv.
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_ = stream.Send(&pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_Send{
				Send: &pb.SendMessage{TargetTopic: "ag.ws.impl.spec", Payload: []byte("x"), MessageType: pb.MessageType_OPAQUE},
			},
		})
		if _, err := stream.Recv(); err != nil {
			lastErr = err
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastErr == nil {
		t.Fatalf("sandbox stream did not surface gateway shutdown as an error")
	}
}

// =============================================================================
// Unit tests for the helpers, no gRPC plumbing.
// =============================================================================

func TestResolveAllowedOps_LiteralAndProfile(t *testing.T) {
	cases := []struct {
		name       string
		cfg        AllowedOpsConfig
		wantHas    []string
		wantNotHas []string
		wantErr    bool
	}{
		{
			name:    "default profile",
			cfg:     AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
			wantHas: []string{OpInitConnection, OpSendMessage, OpProgressReport, OpKVOperation},
			wantNotHas: []string{
				OpProxyHttpRequest, OpTunnelOpen,
			},
		},
		{
			name:    "tunnels profile",
			cfg:     AllowedOpsConfig{Profile: AllowedOpsProfileSandboxTunnels, Set: true},
			wantHas: []string{OpProxyHttpRequest, OpTunnelOpen, OpTunnelData, OpTunnelClose, OpTunnelAck},
		},
		{
			name:       "tool-stub-only",
			cfg:        AllowedOpsConfig{Profile: AllowedOpsProfileToolStubOnly, Set: true},
			wantHas:    []string{OpInitConnection},
			wantNotHas: []string{OpSendMessage, OpProgressReport, OpKVOperation, OpProxyHttpRequest},
		},
		{
			name: "literal list",
			cfg: AllowedOpsConfig{
				Set: true,
				Ops: []string{OpSendMessage, OpKVOperation},
			},
			wantHas:    []string{OpSendMessage, OpKVOperation, OpInitConnection},
			wantNotHas: []string{OpProgressReport, OpProxyHttpRequest},
		},
		{
			name:    "unknown literal op",
			cfg:     AllowedOpsConfig{Set: true, Ops: []string{"NotARealOp"}},
			wantErr: true,
		},
		{
			name:    "unknown profile",
			cfg:     AllowedOpsConfig{Profile: "no-such-profile", Set: true},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, err := resolveAllowedOps(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAllowedOps: %v", err)
			}
			for _, op := range tc.wantHas {
				if !set.allows(op) {
					t.Errorf("expected %q to be allowed", op)
				}
			}
			for _, op := range tc.wantNotHas {
				if set.allows(op) {
					t.Errorf("expected %q to be denied", op)
				}
			}
		})
	}
}

func TestTargetClamp_Reject(t *testing.T) {
	clamp := newTargetClamp(TargetClampConfig{
		Mode:           TargetClampReject,
		AllowedTargets: []string{"ag.foo.bar.*", "tu.specific.task.unique"},
	})
	if r := clamp.evaluate("ag.foo.bar.spec"); !r.Allowed {
		t.Errorf("expected ag.foo.bar.spec to be allowed: %s", r.Reason)
	}
	if r := clamp.evaluate("tu.specific.task.unique"); !r.Allowed {
		t.Errorf("expected exact match to be allowed: %s", r.Reason)
	}
	if r := clamp.evaluate("ag.other.thing.spec"); r.Allowed {
		t.Errorf("expected mismatch to be rejected, got %#v", r)
	}
	if r := clamp.evaluate(""); r.Allowed {
		t.Errorf("expected empty target to be rejected")
	}
}

func TestTargetClamp_RewriteFirstMatch(t *testing.T) {
	clamp := newTargetClamp(TargetClampConfig{
		Mode: TargetClampRewriteFirstMatch,
		// Concrete entry first, glob second.
		AllowedTargets: []string{"ag.canonical.target.spec", "ag.canonical.*"},
	})
	r := clamp.evaluate("ag.attacker.thing.spec")
	if !r.Allowed {
		t.Fatalf("expected rewrite-allowed, got reason=%q", r.Reason)
	}
	if r.NewTarget != "ag.canonical.target.spec" {
		t.Fatalf("rewrite target = %q; want ag.canonical.target.spec", r.NewTarget)
	}

	// Already-allowed inputs must NOT be rewritten.
	r = clamp.evaluate("ag.canonical.foo.bar")
	if !r.Allowed || r.NewTarget != "" {
		t.Fatalf("matched input must pass through verbatim, got %#v", r)
	}
}

func TestTargetClamp_RewriteNoConcreteFallsBack(t *testing.T) {
	clamp := newTargetClamp(TargetClampConfig{
		Mode:           TargetClampRewriteFirstMatch,
		AllowedTargets: []string{"ag.*"},
	})
	r := clamp.evaluate("tu.something.else")
	if r.Allowed {
		t.Fatalf("expected fallback rejection when no concrete entry exists, got %#v", r)
	}
}

func TestUpstreamOpName(t *testing.T) {
	cases := []struct {
		msg  *pb.UpstreamMessage
		want string
	}{
		{nil, ""},
		{&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Init{Init: &pb.InitConnection{}}}, OpInitConnection},
		{&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Send{Send: &pb.SendMessage{}}}, OpSendMessage},
		{&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Progress{Progress: &pb.ProgressReport{}}}, OpProgressReport},
		{&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_KvOp{KvOp: &pb.KVOperation{}}}, OpKVOperation},
		{&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpRequest{ProxyHttpRequest: &pb.ProxyHttpRequest{}}}, OpProxyHttpRequest},
		{&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelOpen{TunnelOpen: &pb.TunnelOpen{}}}, OpTunnelOpen},
	}
	for _, tc := range cases {
		got := upstreamOpName(tc.msg)
		if got != tc.want {
			t.Errorf("upstreamOpName(%T) = %q; want %q", tc.msg.GetPayload(), got, tc.want)
		}
	}
}

func TestSplitListenSpec(t *testing.T) {
	cases := []struct {
		in       string
		wantS    string
		wantAddr string
		wantErr  bool
	}{
		{"unix:///tmp/foo.sock", "unix", "/tmp/foo.sock", false},
		{"tcp://localhost:55551", "tcp", "localhost:55551", false},
		{"localhost:55551", "tcp", "localhost:55551", false},
		{"", "", "", true},
	}
	for _, tc := range cases {
		s, addr, err := splitListenSpec(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("splitListenSpec(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("splitListenSpec(%q): unexpected error %v", tc.in, err)
			continue
		}
		if s != tc.wantS || addr != tc.wantAddr {
			t.Errorf("splitListenSpec(%q) = (%q,%q); want (%q,%q)", tc.in, s, addr, tc.wantS, tc.wantAddr)
		}
	}
}
