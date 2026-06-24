package proxysidecar

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
// Fake gateway (composite-aware) — a single bidi stream the runtime connects
// to. The composite sidecar's runtime dials this; tests use the recorded
// upstream stream to inject ProxyHttpRequest envelopes (terminator surface)
// and to observe forwarded relay traffic (relay surface).
// =============================================================================

type compositeFakeGateway struct {
	pb.UnimplementedAetherGatewayServer

	mu       sync.Mutex
	streams  []*compositeFakeStream
	streamCh chan *compositeFakeStream
}

type compositeFakeStream struct {
	server pb.AetherGateway_ConnectServer

	mu       sync.Mutex
	received []*pb.UpstreamMessage
	closed   bool
}

func newCompositeFakeGateway() *compositeFakeGateway {
	return &compositeFakeGateway{streamCh: make(chan *compositeFakeStream, 4)}
}

func (g *compositeFakeGateway) Connect(stream pb.AetherGateway_ConnectServer) error {
	s := &compositeFakeStream{server: stream}
	g.mu.Lock()
	g.streams = append(g.streams, s)
	g.mu.Unlock()
	g.streamCh <- s

	// Send a ConnectionAck so the runtime considers itself confirmed.
	if err := stream.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{SessionId: "fake-runtime-session"},
		},
	}); err != nil {
		return err
	}
	// Send an empty ConfigSnapshot so any KV bootstrap completes.
	_ = stream.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Config{Config: &pb.ConfigSnapshot{}},
	})

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

func (s *compositeFakeStream) snapshot() []*pb.UpstreamMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.UpstreamMessage, len(s.received))
	copy(out, s.received)
	return out
}

// findUpstream returns the first upstream message matching pred, or nil.
func (s *compositeFakeStream) findUpstream(pred func(*pb.UpstreamMessage) bool) *pb.UpstreamMessage {
	for _, m := range s.snapshot() {
		if pred(m) {
			return m
		}
	}
	return nil
}

// =============================================================================
// Composite (terminator + relay) test harness driven by Runner.
// =============================================================================

type compositeHarness struct {
	t *testing.T

	gateway       *compositeFakeGateway
	gatewayLis    net.Listener
	gatewaySrv    *grpc.Server
	gatewayStream *compositeFakeStream

	runner *Runner
	cancel context.CancelFunc
	done   chan struct{}

	httpBackend *httptest.Server

	sandboxConn *grpc.ClientConn
	sandboxCli  pb.AetherGatewayClient
	sandboxAddr string
	sandboxNet  string
}

// newCompositeHarness spins up a fake gateway, an httptest backend, a Runner
// with terminator + relay enabled, and a sandbox client connected to the
// relay listener.
//
// backendHandler is invoked for every inbound HTTP request the terminator
// surface forwards. Use it to assert audit-lineage / header-propagation
// behaviour.
func newCompositeHarness(t *testing.T, backendHandler http.HandlerFunc) *compositeHarness {
	t.Helper()

	// Fake gateway.
	gw := newCompositeFakeGateway()
	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	gwSrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(gwSrv, gw)
	go func() { _ = gwSrv.Serve(gwLis) }()

	// HTTP backend.
	if backendHandler == nil {
		backendHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	}
	backend := httptest.NewServer(backendHandler)

	// Composite config: terminator + relay over one shared connection.
	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	cfg := &Config{
		Gateway: GatewayConfig{Address: gwLis.Addr().String(), Insecure: true},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "instance-1"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Name:         "default",
				Kind:         BackendKindHTTP,
				URL:          backend.URL,
				AllowPaths:   []string{"/*"},
				AllowMethods: []string{"GET", "POST", "PUT", "DELETE"},
				MaxBodyBytes: 1 << 20,
				HeaderMode:   HeaderModePassthrough,
			}},
		},
		Relay: RelayConfig{
			Enabled: true,
			Listen:  "unix://" + relayPath,
			AllowedOps: AllowedOpsConfig{
				Profile: AllowedOpsProfileSandboxDefault,
				Set:     true,
			},
		},
		TenantID: "tenant-test",
	}

	runner, err := NewRunner(cfg, "")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(ctx)
	}()

	// Wait for the runtime to dial the fake gateway.
	var gwStream *compositeFakeStream
	select {
	case gwStream = <-gw.streamCh:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatalf("runner runtime never connected to fake gateway")
	}
	// And wait for the relay listener to accept.
	relayAddr := relayPath
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := net.Dial("unix", relayAddr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Open a sandbox client to the relay's UDS.
	conn, err := grpc.NewClient("unix://"+relayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		t.Fatalf("dial relay: %v", err)
	}

	h := &compositeHarness{
		t:             t,
		gateway:       gw,
		gatewayLis:    gwLis,
		gatewaySrv:    gwSrv,
		gatewayStream: gwStream,
		runner:        runner,
		cancel:        cancel,
		done:          done,
		httpBackend:   backend,
		sandboxConn:   conn,
		sandboxCli:    pb.NewAetherGatewayClient(conn),
		sandboxAddr:   relayAddr,
		sandboxNet:    "unix",
	}

	t.Cleanup(func() {
		_ = conn.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		gwSrv.GracefulStop()
		_ = gwLis.Close()
		backend.Close()
	})

	return h
}

// =============================================================================
// Tests
// =============================================================================

// TestComposite_ConfigValidatesBothSurfaces asserts that enabling both
// terminator and relay requires both backends and a relay listen address.
func TestComposite_ConfigValidatesBothSurfaces(t *testing.T) {
	t.Parallel()

	// Missing backends → error.
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:50051", Insecure: true},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "i1"},
		Terminator: TerminatorConfig{
			Enabled: true,
		},
		Relay: RelayConfig{
			Enabled: true,
			Listen:  "unix:///tmp/x.sock",
			AllowedOps: AllowedOpsConfig{
				Profile: AllowedOpsProfileSandboxDefault, Set: true,
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Errorf("composite cfg without backends should fail validation")
	} else if !strings.Contains(err.Error(), "backend") {
		t.Errorf("expected backend error, got %v", err)
	}

	// Missing relay.listen → error.
	cfg = &Config{
		Gateway: GatewayConfig{Address: "localhost:50051", Insecure: true},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "i1"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Kind: BackendKindHTTP, URL: "http://localhost:9000",
			}},
		},
		Relay: RelayConfig{Enabled: true},
	}
	if err := cfg.Validate(); err == nil {
		t.Errorf("composite cfg without relay.listen should fail validation")
	} else if !strings.Contains(err.Error(), "relay.listen") {
		t.Errorf("expected relay.listen error, got %v", err)
	}

	// All required fields present → ok.
	cfg = &Config{
		Gateway: GatewayConfig{Address: "localhost:50051", Insecure: true},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "i1"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Kind: BackendKindHTTP, URL: "http://localhost:9000",
			}},
		},
		Relay: RelayConfig{
			Enabled: true,
			Listen:  "unix:///tmp/x.sock",
			AllowedOps: AllowedOpsConfig{
				Profile: AllowedOpsProfileSandboxDefault, Set: true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("composite cfg with both surfaces should validate: %v", err)
	}
}

// TestComposite_ConfigDefaultsApplyToRelaySurface confirms that relay-side
// defaults (sandbox-default profile, reject clamp) get filled in when the
// operator omits them in a composite config.
func TestComposite_ConfigDefaultsApplyToRelaySurface(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:50051", Insecure: true},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "i1"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Kind: BackendKindHTTP, URL: "http://localhost:9000",
			}},
		},
		Relay: RelayConfig{Enabled: true, Listen: "unix:///tmp/x.sock"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Relay.AllowedOps.Profile != AllowedOpsProfileSandboxDefault {
		t.Errorf("expected default sandbox profile, got %q", cfg.Relay.AllowedOps.Profile)
	}
	if cfg.Relay.TargetTopicClamp.Mode != TargetClampReject {
		t.Errorf("expected default reject clamp, got %q", cfg.Relay.TargetTopicClamp.Mode)
	}
	if cfg.Relay.IdentityOverride != IdentityOverrideEnforce {
		t.Errorf("expected default enforce identity override, got %q", cfg.Relay.IdentityOverride)
	}
}

// TestComposite_InboundAndOutboundInterleave drives an in-flight inbound
// proxy HTTP request (terminator surface) AND an outbound sandbox
// SendMessage (relay surface) over the SAME shared gateway connection,
// confirming neither surface interferes with the other.
//
// Wire trace per direction:
//   - Inbound: gatewayStream.Send(ProxyHttpRequest) → runner dispatches to
//     terminator → terminator forwards to httptest backend → terminator
//     emits ProxyHttpResponse upstream → fakegateway.received contains the
//     response.
//   - Outbound: sandbox stream.Send(SendMessage) → relay's pumpUpstream →
//     shared session.Send → runtime queue → fakegateway.received contains
//     the SendMessage envelope, identity rewritten to
//     `service:sidecar/instance-1` (from runtime's Init).
func TestComposite_InboundAndOutboundInterleave(t *testing.T) {
	t.Parallel()

	var (
		backendMu      sync.Mutex
		backendHits    []string
		backendBodyErr error
	)
	h := newCompositeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		backendMu.Lock()
		backendHits = append(backendHits, r.Method+" "+r.URL.Path)
		backendMu.Unlock()
		if _, err := io.ReadAll(r.Body); err != nil {
			backendBodyErr = err
		}
		w.Header().Set("X-Composite", "yes")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"served":"composite"}`)
	})

	// Wait for the runtime's InitConnection to land on the gateway side
	// so we know the runtime is fully attached before we start mixing
	// surfaces.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.gatewayStream.findUpstream(func(m *pb.UpstreamMessage) bool {
			_, ok := m.GetPayload().(*pb.UpstreamMessage_Init)
			return ok
		}) != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if h.gatewayStream.findUpstream(func(m *pb.UpstreamMessage) bool {
		_, ok := m.GetPayload().(*pb.UpstreamMessage_Init)
		return ok
	}) == nil {
		t.Fatal("runtime never sent InitConnection to fake gateway")
	}

	// ----- Outbound (relay) surface: sandbox sends a SendMessage. -----
	sandboxStream, err := h.sandboxCli.Connect(context.Background())
	if err != nil {
		t.Fatalf("sandbox connect: %v", err)
	}
	if err := sandboxStream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Agent{
					Agent: &pb.AgentIdentity{
						Workspace:      "claimed-ws",
						Implementation: "claimed-impl",
						Specifier:      "claimed-spec",
					},
				},
				Credentials: map[string]string{"api_key": "fake-claimed-key"},
			},
		},
	}); err != nil {
		t.Fatalf("sandbox init: %v", err)
	}
	// Drain the synthetic ConnectionAck.
	if _, err := sandboxStream.Recv(); err != nil {
		t.Fatalf("recv synth ack: %v", err)
	}

	// Sandbox emits a SendMessage. This must reach the gateway as the
	// sidecar's identity (since the runtime owns the connection-level
	// init), via the shared queue.
	if err := sandboxStream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: "ag.workspace.demo.spec",
				Payload:     []byte("hello-from-sandbox"),
				MessageType: pb.MessageType_OPAQUE,
			},
		},
	}); err != nil {
		t.Fatalf("sandbox send: %v", err)
	}

	// ----- Inbound (terminator) surface: gateway pushes a ProxyHttpRequest. -----
	if err := h.gatewayStream.server.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId: "inbound-req-1",
				Method:    "GET",
				Path:      "/v1/ping",
			},
		},
	}); err != nil {
		t.Fatalf("gw send proxy req: %v", err)
	}

	// ----- Verify outbound surface: sandbox SendMessage was forwarded. -----
	deadline = time.Now().Add(3 * time.Second)
	var sandboxForwarded *pb.SendMessage
	for time.Now().Before(deadline) {
		for _, m := range h.gatewayStream.snapshot() {
			if s, ok := m.GetPayload().(*pb.UpstreamMessage_Send); ok && s.Send.GetTargetTopic() == "ag.workspace.demo.spec" {
				sandboxForwarded = s.Send
				break
			}
		}
		if sandboxForwarded != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sandboxForwarded == nil {
		t.Fatal("sandbox SendMessage never reached gateway via shared connection")
	}
	if string(sandboxForwarded.GetPayload()) != "hello-from-sandbox" {
		t.Errorf("forwarded payload = %q; want %q",
			string(sandboxForwarded.GetPayload()), "hello-from-sandbox")
	}

	// ----- Verify inbound surface: terminator dispatched + emitted response. -----
	var resp *pb.ProxyHttpResponse
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range h.gatewayStream.snapshot() {
			if r, ok := m.GetPayload().(*pb.UpstreamMessage_ProxyHttpResponse); ok && r.ProxyHttpResponse.GetRequestId() == "inbound-req-1" {
				resp = r.ProxyHttpResponse
				break
			}
		}
		if resp != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resp == nil {
		t.Fatal("terminator never emitted ProxyHttpResponse for inbound-req-1")
	}
	if resp.GetStatusCode() != 200 {
		t.Errorf("proxy response status = %d; want 200", resp.GetStatusCode())
	}
	if got := resp.GetHeaders()["X-Composite"]; got != "yes" {
		t.Errorf("response header X-Composite = %q; want yes", got)
	}
	if string(resp.GetBody()) != `{"served":"composite"}` {
		t.Errorf("response body = %q; want json", string(resp.GetBody()))
	}
	if backendBodyErr != nil {
		t.Errorf("backend body read err: %v", backendBodyErr)
	}

	// ----- Audit lineages -----
	// (a) Terminator audit: backend saw the request — that is its
	//     in-process audit trail (httptest handler ran), unchanged from
	//     standalone terminator mode.
	backendMu.Lock()
	hits := append([]string(nil), backendHits...)
	backendMu.Unlock()
	if len(hits) != 1 || hits[0] != "GET /v1/ping" {
		t.Errorf("terminator backend hits = %v; want [GET /v1/ping]", hits)
	}
	// (b) Relay audit: the SendMessage carries the *runtime's* identity
	//     into the gateway, not the sandbox's claim. Both Init (sent by
	//     the runtime at startup) and the forwarded SendMessage must
	//     attribute to service:sidecar/instance-1.
	initMsg := h.gatewayStream.findUpstream(func(m *pb.UpstreamMessage) bool {
		_, ok := m.GetPayload().(*pb.UpstreamMessage_Init)
		return ok
	})
	if initMsg == nil {
		t.Fatal("expected runtime InitConnection to have reached gateway")
	}
	svc, ok := initMsg.GetPayload().(*pb.UpstreamMessage_Init).Init.GetClientType().(*pb.InitConnection_Service)
	if !ok {
		t.Fatalf("runtime Init client_type = %T; want Service",
			initMsg.GetPayload().(*pb.UpstreamMessage_Init).Init.GetClientType())
	}
	if svc.Service.GetImplementation() != "sidecar" || svc.Service.GetSpecifier() != "instance-1" {
		t.Errorf("runtime identity = %s/%s; want sidecar/instance-1",
			svc.Service.GetImplementation(), svc.Service.GetSpecifier())
	}
	// And there must be exactly ONE InitConnection on the upstream
	// stream — the relay surface MUST NOT send a second Init.
	initCount := 0
	for _, m := range h.gatewayStream.snapshot() {
		if _, ok := m.GetPayload().(*pb.UpstreamMessage_Init); ok {
			initCount++
		}
	}
	if initCount != 1 {
		t.Errorf("upstream stream saw %d InitConnections; want 1 (one identity, one lock)", initCount)
	}
}

// TestComposite_ShutdownClosesBothSurfaces asserts that cancelling the runner
// Run context cleanly tears down the sandbox session AND the gateway upstream
// stream.
func TestComposite_ShutdownClosesBothSurfaces(t *testing.T) {
	t.Parallel()

	h := newCompositeHarness(t, nil)

	// Open a sandbox session.
	stream, err := h.sandboxCli.Connect(context.Background())
	if err != nil {
		t.Fatalf("sandbox connect: %v", err)
	}
	if err := stream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Agent{
					Agent: &pb.AgentIdentity{Workspace: "ws", Implementation: "i", Specifier: "s"},
				},
			},
		},
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("recv ack: %v", err)
	}

	// Close the sandbox stream first (mirrors a real-world sandbox
	// disconnecting before the sidecar receives SIGTERM). Then cancel
	// the runner. relay.Run uses GracefulStop, which blocks on active
	// streams; closing the sandbox first lets the relay drain cleanly
	// without the test having to force-kill it.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	// Drain remaining server-side messages until EOF, so the relay's
	// session pump exits cleanly.
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}

	h.cancel()

	// The Run goroutine should exit within a bounded time. 30s is generous
	// for any reasonable CI runner load spike (the original 5s flaked on
	// GHA under contention even though local runs return in ~1s). If we
	// genuinely deadlock, 30s is still well below the per-test default
	// timeout, and the goroutine-stack dump below names where Run is
	// blocked — turning a "didn't exit" mystery into actionable evidence.
	select {
	case <-h.done:
	case <-time.After(30 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("runner Run goroutine did not exit after context cancel; goroutine dump:\n%s", buf[:n])
	}
}

// TestComposite_AcceptsConcurrentSandboxSessions verifies that the
// shared-runtime relay sink multiplexes multiple concurrent sandbox
// sessions onto the sidecar's single upstream gateway connection.
// Previously this rejected any second concurrent session — which caused
// a reconnect storm when the sidecar's own RPC client occupied the slot
// and the in-sandbox SDK then tried to attach (cf. the v0.2.x bug).
//
// The complementary "cap exceeded" path is covered by
// TestSharedRelaySink_AttachRejectsBeyondMaxSessions (no full Runner
// needed for that property).
func TestComposite_AcceptsConcurrentSandboxSessions(t *testing.T) {
	t.Parallel()

	h := newCompositeHarness(t, nil)

	open := func(specifier string) pb.AetherGateway_ConnectClient {
		stream, err := h.sandboxCli.Connect(context.Background())
		if err != nil {
			t.Fatalf("sandbox dial (%s): %v", specifier, err)
		}
		if err := stream.Send(&pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_Init{
				Init: &pb.InitConnection{
					ClientType: &pb.InitConnection_Agent{
						Agent: &pb.AgentIdentity{Workspace: "ws", Implementation: "i", Specifier: specifier},
					},
				},
			},
		}); err != nil {
			t.Fatalf("init send (%s): %v", specifier, err)
		}
		// Each session synthesises its own ConnectionAck on attach.
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("ack recv (%s): %v", specifier, err)
		}
		if _, ok := ack.GetPayload().(*pb.DownstreamMessage_ConnectionAck); !ok {
			t.Fatalf("ack (%s): expected ConnectionAck, got %T", specifier, ack.GetPayload())
		}
		return stream
	}

	first := open("s1")
	second := open("s2")

	// Tidy: close both. Defer would also work; explicit is clearer for
	// the assertion that BOTH were live concurrently above.
	_ = first.CloseSend()
	_ = second.CloseSend()
}

// TestSharedRelaySink_AttachRejectsBeyondMaxSessions exercises the
// per-sink session cap directly so we don't need to spin up a full
// Runner + grpc transport just to count attaches. attachSession returns
// false once the cap is reached; detachSession reclaims slots.
func TestSharedRelaySink_AttachRejectsBeyondMaxSessions(t *testing.T) {
	t.Parallel()

	const cap = 3
	sink := newSharedRelaySink(nil, cap)
	sessions := make([]*sharedRuntimeSession, 0, cap)

	for i := 0; i < cap; i++ {
		sess := &sharedRuntimeSession{owner: sink}
		if !sink.attachSession(sess) {
			t.Fatalf("attach %d/%d unexpectedly rejected", i+1, cap)
		}
		sessions = append(sessions, sess)
	}

	overflow := &sharedRuntimeSession{owner: sink}
	if sink.attachSession(overflow) {
		t.Fatalf("attach beyond cap=%d should have been rejected", cap)
	}

	// Detaching a slot frees room for one more attach.
	sink.detachSession(sessions[0])
	replacement := &sharedRuntimeSession{owner: sink}
	if !sink.attachSession(replacement) {
		t.Fatalf("attach after detach unexpectedly rejected")
	}
}

// =============================================================================
// Composite RelayMux regression tests.
//
// These tests exercise the relaymux composite path (terminator + RelayMux on
// one shared runtime) through the full Runner stack.
// =============================================================================

// compositeDialSandbox opens a relay sub-client through the composite harness,
// sends an Init, and drains the synthesized ConnectionAck.
func compositeDialSandbox(t *testing.T, h *compositeHarness, specifier string) pb.AetherGateway_ConnectClient {
	t.Helper()
	stream, err := h.sandboxCli.Connect(context.Background())
	if err != nil {
		t.Fatalf("sandbox dial (%s): %v", specifier, err)
	}
	if err := stream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Agent{
					Agent: &pb.AgentIdentity{Workspace: "ws", Implementation: "i", Specifier: specifier},
				},
			},
		},
	}); err != nil {
		t.Fatalf("init send (%s): %v", specifier, err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("ack recv (%s): %v", specifier, err)
	}
	if _, ok := ack.GetPayload().(*pb.DownstreamMessage_ConnectionAck); !ok {
		t.Fatalf("ack (%s): expected ConnectionAck, got %T", specifier, ack.GetPayload())
	}
	return stream
}

// waitUpstreamFrame blocks until pred returns true for one of the recorded
// upstream frames, or the deadline passes.
func waitUpstreamFrame(t *testing.T, h *compositeHarness, pred func(*pb.UpstreamMessage) bool) *pb.UpstreamMessage {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range h.gatewayStream.snapshot() {
			if pred(m) {
				return m
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// TestComposite_RelayMux_CollidingRequestIDs is the regression test for the
// pinned production bug: two sandbox sub-clients both issue a KV op with the
// SAME origin request_id "req-1" (as the SDK's BaseClient.NextRequestID()
// mints per-client). In the old sharedRelaySink flat-map each sub-client's
// registerRequest overwrote the other's entry, so only one got its reply.
// RelayMux rewrites each to a globally-unique "rmx-N" id before forwarding
// and restores "req-1" on the reply, routing each reply to its originating
// sub-client.
func TestComposite_RelayMux_CollidingRequestIDs(t *testing.T) {
	t.Parallel()

	h := newCompositeHarness(t, nil)

	s1 := compositeDialSandbox(t, h, "agent-1")
	s2 := compositeDialSandbox(t, h, "agent-2")

	// Both sub-clients send a KV op with the SAME origin request_id "req-1".
	kvGet := func(key string) *pb.UpstreamMessage {
		return &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_KvOp{KvOp: &pb.KVOperation{
			RequestId: "req-1",
			Op:        pb.KVOperation_GET,
			Key:       key,
		}}}
	}
	if err := s1.Send(kvGet("k1")); err != nil {
		t.Fatalf("s1 send: %v", err)
	}
	if err := s2.Send(kvGet("k2")); err != nil {
		t.Fatalf("s2 send: %v", err)
	}

	// Wait until both KV ops appear upstream with distinct rmx-* ids.
	var muxIDk1, muxIDk2 string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range h.gatewayStream.snapshot() {
			kv, ok := m.GetPayload().(*pb.UpstreamMessage_KvOp)
			if !ok {
				continue
			}
			if kv.KvOp.GetKey() == "k1" {
				muxIDk1 = kv.KvOp.GetRequestId()
			}
			if kv.KvOp.GetKey() == "k2" {
				muxIDk2 = kv.KvOp.GetRequestId()
			}
		}
		if muxIDk1 != "" && muxIDk2 != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if muxIDk1 == "" || muxIDk2 == "" {
		t.Fatalf("upstream did not receive both KV ops (k1=%q k2=%q)", muxIDk1, muxIDk2)
	}
	if muxIDk1 == "req-1" || muxIDk2 == "req-1" {
		t.Fatalf("request_id was not rewritten (still req-1): k1=%q k2=%q", muxIDk1, muxIDk2)
	}
	if muxIDk1 == muxIDk2 {
		t.Fatalf("both sub-clients got the same mux id %q; expected distinct ids", muxIDk1)
	}

	// Inject replies in swapped order (k2 first, then k1) via the gateway
	// stream. The mux must restore "req-1" and route each to the right sub-client.
	kvReply := func(muxID, val string) *pb.DownstreamMessage {
		return &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Kv{Kv: &pb.KVResponse{
			RequestId: muxID,
			Success:   true,
			Value:     []byte(val),
		}}}
	}
	if err := h.gatewayStream.server.Send(kvReply(muxIDk2, "v2")); err != nil {
		t.Fatalf("inject reply k2: %v", err)
	}
	if err := h.gatewayStream.server.Send(kvReply(muxIDk1, "v1")); err != nil {
		t.Fatalf("inject reply k1: %v", err)
	}

	// s1 must get v1, s2 must get v2, both with request_id restored to "req-1".
	got1 := compositeRecvKV(t, s1)
	got2 := compositeRecvKV(t, s2)
	if got1.GetRequestId() != "req-1" || string(got1.GetValue()) != "v1" {
		t.Fatalf("s1 got req=%q val=%q; want req-1/v1", got1.GetRequestId(), string(got1.GetValue()))
	}
	if got2.GetRequestId() != "req-1" || string(got2.GetValue()) != "v2" {
		t.Fatalf("s2 got req=%q val=%q; want req-1/v2", got2.GetRequestId(), string(got2.GetValue()))
	}
}

// TestComposite_RelayMux_BroadcastToSubClients verifies that gateway push
// frames (TaskAssignment) are broadcast to all relay sub-clients in composite
// mode, while the terminator surface is unaffected.
func TestComposite_RelayMux_BroadcastToSubClients(t *testing.T) {
	t.Parallel()

	h := newCompositeHarness(t, nil)

	s1 := compositeDialSandbox(t, h, "agent-1")
	s2 := compositeDialSandbox(t, h, "agent-2")

	push := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TaskAssignment{
		TaskAssignment: &pb.TaskAssignment{TaskId: "composite-task-xyz"},
	}}
	if err := h.gatewayStream.server.Send(push); err != nil {
		t.Fatalf("inject push: %v", err)
	}

	for i, s := range []pb.AetherGateway_ConnectClient{s1, s2} {
		msg := compositeRecvWithTimeout(t, s)
		ta, ok := msg.GetPayload().(*pb.DownstreamMessage_TaskAssignment)
		if !ok {
			t.Fatalf("sub %d got %T; want TaskAssignment", i+1, msg.GetPayload())
		}
		if ta.TaskAssignment.GetTaskId() != "composite-task-xyz" {
			t.Fatalf("sub %d task id = %q; want composite-task-xyz", i+1, ta.TaskAssignment.GetTaskId())
		}
	}
}

// TestComposite_RelayMux_TerminatorProxyHttpUnaffected confirms that the
// terminator's own inbound ProxyHttpRequest still round-trips through the
// shared connection when RelayMux sub-clients are concurrently attached.
// This guards against the rawDownstreamTap accidentally swallowing the
// terminator's ProxyHttpResponse wakeup.
func TestComposite_RelayMux_TerminatorProxyHttpUnaffected(t *testing.T) {
	t.Parallel()

	h := newCompositeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Relay-Mux", "ok")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "mux-ok")
	})

	// Attach two relay sub-clients to prove the terminator path is independent.
	_ = compositeDialSandbox(t, h, "agent-1")
	_ = compositeDialSandbox(t, h, "agent-2")

	// Wait for the runtime Init to confirm the connection is live.
	if waitUpstreamFrame(t, h, func(m *pb.UpstreamMessage) bool {
		_, ok := m.GetPayload().(*pb.UpstreamMessage_Init)
		return ok
	}) == nil {
		t.Fatal("runtime never sent InitConnection")
	}

	// Gateway sends an inbound ProxyHttpRequest to the terminator.
	if err := h.gatewayStream.server.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId: "term-req-mux-1",
				Method:    "GET",
				Path:      "/mux-ping",
			},
		},
	}); err != nil {
		t.Fatalf("inject ProxyHttpRequest: %v", err)
	}

	// The terminator must emit a ProxyHttpResponse upstream.
	resp := waitUpstreamFrame(t, h, func(m *pb.UpstreamMessage) bool {
		r, ok := m.GetPayload().(*pb.UpstreamMessage_ProxyHttpResponse)
		return ok && r.ProxyHttpResponse.GetRequestId() == "term-req-mux-1"
	})
	if resp == nil {
		t.Fatal("terminator never emitted ProxyHttpResponse for term-req-mux-1")
	}
	r := resp.GetPayload().(*pb.UpstreamMessage_ProxyHttpResponse).ProxyHttpResponse
	if r.GetStatusCode() != 200 {
		t.Errorf("proxy response status = %d; want 200", r.GetStatusCode())
	}
	if string(r.GetBody()) != "mux-ok" {
		t.Errorf("proxy response body = %q; want mux-ok", string(r.GetBody()))
	}
}

// compositeRecvKV receives the next downstream frame and asserts it is a KV
// response, returning the inner KVResponse.
func compositeRecvKV(t *testing.T, s pb.AetherGateway_ConnectClient) *pb.KVResponse {
	t.Helper()
	msg := compositeRecvWithTimeout(t, s)
	kv, ok := msg.GetPayload().(*pb.DownstreamMessage_Kv)
	if !ok {
		t.Fatalf("got %T; want KVResponse", msg.GetPayload())
	}
	return kv.Kv
}

// compositeRecvWithTimeout receives one downstream frame with a 3s deadline.
func compositeRecvWithTimeout(t *testing.T, s pb.AetherGateway_ConnectClient) *pb.DownstreamMessage {
	t.Helper()
	type res struct {
		msg *pb.DownstreamMessage
		err error
	}
	ch := make(chan res, 1)
	go func() {
		m, err := s.Recv()
		ch <- res{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv: %v", r.err)
		}
		return r.msg
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for downstream frame")
		return nil
	}
}

// TestRunner_RejectsConfigWithNoSurfacesEnabled covers the validation rule
// that a config with every surface disabled is rejected.
func TestRunner_RejectsConfigWithNoSurfacesEnabled(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:50051", Insecure: true},
		Service: ServiceConfig{Implementation: "sidecar", Specifier: "i1"},
	}
	if _, err := NewRunner(cfg, ""); err == nil {
		t.Errorf("expected NewRunner to reject a config with no surfaces enabled")
	} else if !strings.Contains(err.Error(), "surface") {
		t.Errorf("expected surface-enabled error, got %v", err)
	}
}
