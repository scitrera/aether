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
	"github.com/scitrera/go-backpressure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
// Pure routing-decision table test (classifyDownstream).
//
// This exercises the demux routing logic without any live stream: we seed the
// pending/tunnel tables by hand and assert the classification for each frame
// shape. It is the cheapest place to cover the "unknown correlated id is
// dropped, not broadcast" requirement and the request_id-demux/restore route.
// =============================================================================

func newTestMux() *RelayMux {
	return &RelayMux{
		subs:    map[uint64]*muxSubClient{},
		pending: map[string]muxPending{},
		tunnels: map[string]uint64{},
	}
}

func TestRelayMux_ClassifyDownstream(t *testing.T) {
	m := newTestMux()
	// Two sub-clients each have an in-flight KV op under distinct mux ids but
	// the SAME original request id ("r1").
	m.subs[1] = &muxSubClient{id: 1}
	m.subs[2] = &muxSubClient{id: 2}
	m.pending["100"] = muxPending{subID: 1, origReqID: "r1", payloadTag: OpKVOperation}
	m.pending["101"] = muxPending{subID: 2, origReqID: "r1", payloadTag: OpKVOperation}
	m.tunnels["tun-a"] = 1

	kvReply := func(muxID string) *pb.DownstreamMessage {
		return &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Kv{Kv: &pb.KVResponse{RequestId: muxID}}}
	}

	cases := []struct {
		name      string
		msg       *pb.DownstreamMessage
		wantRoute muxRoute
		wantSub   uint64
		wantOrig  string
		wantTun   string
	}{
		{
			name:      "kv reply for sub 1",
			msg:       kvReply("100"),
			wantRoute: routeCorrelated,
			wantSub:   1,
			wantOrig:  "r1",
		},
		{
			name:      "kv reply for sub 2 (same orig req id)",
			msg:       kvReply("101"),
			wantRoute: routeCorrelated,
			wantSub:   2,
			wantOrig:  "r1",
		},
		{
			name:      "unknown correlated id is dropped not broadcast",
			msg:       kvReply("999"),
			wantRoute: routeDropUnknownCorrelated,
		},
		{
			name: "task assignment is broadcast",
			msg: &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TaskAssignment{
				TaskAssignment: &pb.TaskAssignment{TaskId: "t1"}}},
			wantRoute: routeBroadcast,
		},
		{
			name: "incoming message is broadcast",
			msg: &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Msg{
				Msg: &pb.IncomingMessage{}}},
			wantRoute: routeBroadcast,
		},
		{
			name: "tunnel data routes to owning sub",
			msg: &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TunnelData{
				TunnelData: &pb.TunnelData{TunnelId: "tun-a"}}},
			wantRoute: routeTunnel,
			wantSub:   1,
			wantTun:   "tun-a",
		},
		{
			name: "tunnel data for unknown tunnel is dropped",
			msg: &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TunnelData{
				TunnelData: &pb.TunnelData{TunnelId: "tun-zzz"}}},
			wantRoute: routeDropUnknownCorrelated,
		},
		{
			name: "error with known pending id demuxes",
			msg: &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Error{
				Error: &pb.ErrorResponse{Code: "X", RequestId: "100"}}},
			wantRoute: routeCorrelated,
			wantSub:   1,
			wantOrig:  "r1",
		},
		{
			name: "error without request id is broadcast",
			msg: &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Error{
				Error: &pb.ErrorResponse{Code: "CONN_LEVEL"}}},
			wantRoute: routeBroadcast,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m.mu.Lock()
			dec := m.classifyDownstream(tc.msg)
			m.mu.Unlock()
			if dec.route != tc.wantRoute {
				t.Fatalf("route = %v; want %v", dec.route, tc.wantRoute)
			}
			if tc.wantSub != 0 && dec.subID != tc.wantSub {
				t.Fatalf("subID = %d; want %d", dec.subID, tc.wantSub)
			}
			if tc.wantOrig != "" && dec.origReqID != tc.wantOrig {
				t.Fatalf("origReqID = %q; want %q", dec.origReqID, tc.wantOrig)
			}
			if tc.wantTun != "" && dec.tunnelID != tc.wantTun {
				t.Fatalf("tunnelID = %q; want %q", dec.tunnelID, tc.wantTun)
			}
		})
	}
}

// =============================================================================
// Integration harness: fake gateway + real RelayMux server + N sandbox clients.
//
// The fake gateway records every upstream frame and exposes the live server
// stream so the test can INJECT downstream frames at the mux's shared upstream.
// =============================================================================

type muxFakeGateway struct {
	pb.UnimplementedAetherGatewayServer
	mu       sync.Mutex
	received []*pb.UpstreamMessage
	streamCh chan pb.AetherGateway_ConnectServer
}

func newMuxFakeGateway() *muxFakeGateway {
	return &muxFakeGateway{streamCh: make(chan pb.AetherGateway_ConnectServer, 2)}
}

func (g *muxFakeGateway) Connect(stream pb.AetherGateway_ConnectServer) error {
	g.streamCh <- stream
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		g.mu.Lock()
		g.received = append(g.received, msg)
		g.mu.Unlock()
	}
}

func (g *muxFakeGateway) snapshot() []*pb.UpstreamMessage {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*pb.UpstreamMessage, len(g.received))
	copy(out, g.received)
	return out
}

// awaitUpstream returns the (single) shared upstream stream the mux opened.
func (g *muxFakeGateway) awaitUpstream(t *testing.T) pb.AetherGateway_ConnectServer {
	t.Helper()
	select {
	case s := <-g.streamCh:
		return s
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for mux upstream")
		return nil
	}
}

type muxHarness struct {
	t         *testing.T
	gateway   *muxFakeGateway
	mux       *RelayMux
	relayLis  net.Listener
	relaySrv  *grpc.Server
	relayAddr string
	relayUnix bool
	conns     []*grpc.ClientConn
}

func newMuxHarness(t *testing.T, cfg *Config) *muxHarness {
	t.Helper()

	gateway := newMuxFakeGateway()
	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	gwSrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(gwSrv, gateway)
	go func() { _ = gwSrv.Serve(gwLis) }()

	cfg.Relay.Enabled = true
	cfg.Gateway.Address = gwLis.Addr().String()
	cfg.Gateway.Insecure = true
	cfg.Service.Implementation = "sidecar"
	cfg.Service.Specifier = "instance-1"
	if cfg.Relay.Listen == "" {
		cfg.Relay.Listen = "unix://" + filepath.Join(t.TempDir(), "relaymux.sock")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate cfg: %v", err)
	}

	m, err := NewRelayMux(cfg)
	if err != nil {
		t.Fatalf("NewRelayMux: %v", err)
	}

	relayLis, cleanup, err := openRelayListener(cfg.Relay.Listen)
	if err != nil {
		t.Fatalf("open relay listener: %v", err)
	}
	relaySrv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(relaySrv, m)
	go func() { _ = relaySrv.Serve(relayLis) }()

	h := &muxHarness{
		t:         t,
		gateway:   gateway,
		mux:       m,
		relayLis:  relayLis,
		relaySrv:  relaySrv,
		relayAddr: relayLis.Addr().String(),
		relayUnix: relayLis.Addr().Network() == "unix",
	}

	t.Cleanup(func() {
		for _, c := range h.conns {
			_ = c.Close()
		}
		// Stop() (not GracefulStop) force-closes in-flight streams. The mux
		// keeps its shared upstream open across sub-client departures by
		// design, so the fake gateway's Connect handler is still parked in
		// Recv() at teardown; GracefulStop would block on it. Tests do not
		// need graceful drain.
		relaySrv.Stop()
		_ = relayLis.Close()
		if cleanup != nil {
			cleanup()
		}
		gwSrv.Stop()
		_ = gwLis.Close()
	})
	return h
}

// dialSub opens a sandbox-side client stream and completes the local init
// handshake, draining the synthesized ConnectionAck.
func (h *muxHarness) dialSub() pb.AetherGateway_ConnectClient {
	h.t.Helper()
	scheme := "passthrough:///"
	if h.relayUnix {
		scheme = "unix://"
	}
	conn, err := grpc.NewClient(scheme+h.relayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatalf("dial relay: %v", err)
	}
	h.conns = append(h.conns, conn)
	cli := pb.NewAetherGatewayClient(conn)
	stream, err := cli.Connect(context.Background())
	if err != nil {
		h.t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(sandboxInit()); err != nil {
		h.t.Fatalf("send init: %v", err)
	}
	// Drain the synthesized ConnectionAck.
	ack, err := stream.Recv()
	if err != nil {
		h.t.Fatalf("recv ack: %v", err)
	}
	if _, ok := ack.GetPayload().(*pb.DownstreamMessage_ConnectionAck); !ok {
		h.t.Fatalf("first downstream frame = %T; want ConnectionAck", ack.GetPayload())
	}
	return stream
}

// waitUpstreamCount blocks until the fake gateway has recorded at least n
// upstream frames (the first is always the single rewritten Init).
func (h *muxHarness) waitUpstreamCount(n int) []*pb.UpstreamMessage {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap := h.gateway.snapshot()
		if len(snap) >= n {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h.gateway.snapshot()
}

// (a) Two sub-clients each do a KV get with the SAME origin request_id; each
// must get its OWN response back (request_id demux + restore).
func TestRelayMux_KVDemuxByRequestID(t *testing.T) {
	cfg := &Config{Relay: RelayConfig{
		AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
	}}
	h := newMuxHarness(t, cfg)

	s1 := h.dialSub()
	s2 := h.dialSub()
	upstream := h.gateway.awaitUpstream(t)

	kvGet := func(reqID, key string) *pb.UpstreamMessage {
		return &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_KvOp{KvOp: &pb.KVOperation{
			RequestId: reqID,
			Op:        pb.KVOperation_GET,
			Key:       key,
		}}}
	}
	// Both sub-clients use the SAME origin request_id "r1".
	if err := s1.Send(kvGet("r1", "k1")); err != nil {
		t.Fatalf("s1 send: %v", err)
	}
	if err := s2.Send(kvGet("r1", "k2")); err != nil {
		t.Fatalf("s2 send: %v", err)
	}

	// Wait for both forwarded KV ops upstream (after the single Init = 3 total).
	snap := h.waitUpstreamCount(3)
	if len(snap) < 3 {
		t.Fatalf("expected 3 upstream frames (init + 2 kv), saw %d", len(snap))
	}
	// Map mux request id -> which key it carried, so we can craft the right reply.
	muxByKey := map[string]string{} // key -> muxID
	for _, u := range snap {
		if kv, ok := u.GetPayload().(*pb.UpstreamMessage_KvOp); ok {
			muxByKey[kv.KvOp.GetKey()] = kv.KvOp.GetRequestId()
			if kv.KvOp.GetRequestId() == "r1" {
				t.Fatalf("request_id was not rewritten to a mux id (still r1)")
			}
		}
	}
	if muxByKey["k1"] == "" || muxByKey["k2"] == "" || muxByKey["k1"] == muxByKey["k2"] {
		t.Fatalf("expected two distinct mux ids, got %v", muxByKey)
	}

	// Inject replies (in swapped order to prove routing isn't FIFO): reply for
	// k2 first, then k1. The mux must restore "r1" and route each to the right
	// sub-client.
	reply := func(muxID, val string) *pb.DownstreamMessage {
		return &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Kv{Kv: &pb.KVResponse{
			RequestId: muxID,
			Success:   true,
			Value:     []byte(val),
		}}}
	}
	if err := upstream.Send(reply(muxByKey["k2"], "v2")); err != nil {
		t.Fatalf("inject reply k2: %v", err)
	}
	if err := upstream.Send(reply(muxByKey["k1"], "v1")); err != nil {
		t.Fatalf("inject reply k1: %v", err)
	}

	got1 := recvKV(t, s1)
	got2 := recvKV(t, s2)
	if got1.GetRequestId() != "r1" || string(got1.GetValue()) != "v1" {
		t.Fatalf("s1 got req=%q val=%q; want r1/v1", got1.GetRequestId(), string(got1.GetValue()))
	}
	if got2.GetRequestId() != "r1" || string(got2.GetValue()) != "v2" {
		t.Fatalf("s2 got req=%q val=%q; want r1/v2", got2.GetRequestId(), string(got2.GetValue()))
	}
}

// (b) A TaskAssignment push is broadcast to BOTH sub-clients.
func TestRelayMux_BroadcastTaskAssignment(t *testing.T) {
	cfg := &Config{Relay: RelayConfig{
		AllowedOps: AllowedOpsConfig{Profile: AllowedOpsProfileSandboxDefault, Set: true},
	}}
	h := newMuxHarness(t, cfg)

	s1 := h.dialSub()
	s2 := h.dialSub()
	upstream := h.gateway.awaitUpstream(t)

	push := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TaskAssignment{
		TaskAssignment: &pb.TaskAssignment{TaskId: "task-xyz"}}}
	if err := upstream.Send(push); err != nil {
		t.Fatalf("inject push: %v", err)
	}

	for i, s := range []pb.AetherGateway_ConnectClient{s1, s2} {
		msg := recvWithTimeout(t, s)
		ta, ok := msg.GetPayload().(*pb.DownstreamMessage_TaskAssignment)
		if !ok {
			t.Fatalf("sub %d got %T; want TaskAssignment", i+1, msg.GetPayload())
		}
		if ta.TaskAssignment.GetTaskId() != "task-xyz" {
			t.Fatalf("sub %d task id = %q; want task-xyz", i+1, ta.TaskAssignment.GetTaskId())
		}
	}
}

// (c) A TunnelData downstream routes only to the tunnel's owning sub-client.
func TestRelayMux_TunnelRoutesToOwner(t *testing.T) {
	cfg := &Config{Relay: RelayConfig{
		AllowedOps:       AllowedOpsConfig{Profile: AllowedOpsProfileSandboxTunnels, Set: true},
		TargetTopicClamp: TargetClampConfig{Mode: TargetClampReject, AllowedTargets: []string{"sv.*"}},
	}}
	h := newMuxHarness(t, cfg)

	s1 := h.dialSub()
	s2 := h.dialSub()
	upstream := h.gateway.awaitUpstream(t)

	// s1 opens a tunnel.
	open := &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelOpen{TunnelOpen: &pb.TunnelOpen{
		TunnelId:    "tun-1",
		TargetTopic: "sv.backend.svc",
	}}}
	if err := s1.Send(open); err != nil {
		t.Fatalf("s1 tunnel open: %v", err)
	}
	// Wait for the open to reach upstream (init + tunnel_open = 2).
	h.waitUpstreamCount(2)

	// Inject tunnel data downstream for tun-1: must land on s1 only.
	data := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TunnelData{TunnelData: &pb.TunnelData{
		TunnelId: "tun-1",
		Data:     []byte("payload"),
	}}}
	if err := upstream.Send(data); err != nil {
		t.Fatalf("inject tunnel data: %v", err)
	}

	got := recvWithTimeout(t, s1)
	td, ok := got.GetPayload().(*pb.DownstreamMessage_TunnelData)
	if !ok || td.TunnelData.GetTunnelId() != "tun-1" {
		t.Fatalf("s1 got %T; want TunnelData tun-1", got.GetPayload())
	}
	// s2 must NOT receive it.
	if msg := recvMaybe(s2, 300*time.Millisecond); msg != nil {
		t.Fatalf("s2 unexpectedly received %T for a tunnel it doesn't own", msg.GetPayload())
	}
}

// (e) A clamp rejection returns relayErrorDownstream to the right sub-client.
func TestRelayMux_ClampRejectionToRightSub(t *testing.T) {
	cfg := &Config{Relay: RelayConfig{
		AllowedOps:       AllowedOpsConfig{Profile: AllowedOpsProfileSandboxTunnels, Set: true},
		TargetTopicClamp: TargetClampConfig{Mode: TargetClampReject, AllowedTargets: []string{"sv.allowed.*"}},
	}}
	h := newMuxHarness(t, cfg)

	s1 := h.dialSub()
	s2 := h.dialSub()
	_ = h.gateway.awaitUpstream(t)

	// s2 sends a ProxyHttpRequest to a denied target.
	req := &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpRequest{ProxyHttpRequest: &pb.ProxyHttpRequest{
		RequestId:   "p1",
		TargetTopic: "sv.denied.svc",
		Method:      "GET",
		Path:        "/",
	}}}
	if err := s2.Send(req); err != nil {
		t.Fatalf("s2 send: %v", err)
	}

	got := recvWithTimeout(t, s2)
	errResp, ok := got.GetPayload().(*pb.DownstreamMessage_Error)
	if !ok {
		t.Fatalf("s2 got %T; want Error", got.GetPayload())
	}
	if errResp.Error.GetCode() != "RELAY_TARGET_DENIED" {
		t.Fatalf("s2 error code = %q; want RELAY_TARGET_DENIED", errResp.Error.GetCode())
	}
	// s1 must NOT see the rejection.
	if msg := recvMaybe(s1, 300*time.Millisecond); msg != nil {
		t.Fatalf("s1 unexpectedly received %T for s2's rejected request", msg.GetPayload())
	}
}

// ---- recv helpers -----------------------------------------------------------

func recvKV(t *testing.T, s pb.AetherGateway_ConnectClient) *pb.KVResponse {
	t.Helper()
	msg := recvWithTimeout(t, s)
	kv, ok := msg.GetPayload().(*pb.DownstreamMessage_Kv)
	if !ok {
		t.Fatalf("got %T; want KVResponse", msg.GetPayload())
	}
	return kv.Kv
}

func recvWithTimeout(t *testing.T, s pb.AetherGateway_ConnectClient) *pb.DownstreamMessage {
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

// recvMaybe returns a frame if one arrives within d, else nil. Used to assert a
// sub-client does NOT receive a frame routed to a different sub-client.
// TestMuxSubClient_WedgedReaderDoesNotBlockHealthySub verifies the backpressure
// admission design: a sub-client whose inbox is full (simulating a wedged
// in-sandbox reader whose gRPC write window is exhausted) sheds incoming frames
// with a BACKPRESSURE error rather than blocking the caller. A second healthy
// sub-client must still receive its frame promptly — deliver() must return in
// bounded time regardless of the wedged sub's state.
func TestMuxSubClient_WedgedReaderDoesNotBlockHealthySub(t *testing.T) {
	t.Parallel()

	newSub := func(id uint64) *muxSubClient {
		return &muxSubClient{
			id:    id,
			inbox: make(chan *pb.DownstreamMessage, sharedRuntimeSessionDeliverCapacity*2),
			deliverSem: backpressure.NewSemaphore(
				5,
				sharedRuntimeSessionDeliverCapacity,
				backpressure.SemaphoreShortTimeout(sharedRuntimeSessionDeliverShortTimeout),
				backpressure.SemaphoreLongTimeout(sharedRuntimeSessionDeliverLongTimeout),
			),
		}
	}

	taskPush := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskAssignment{
			TaskAssignment: &pb.TaskAssignment{TaskId: "test-task"},
		},
	}

	// --- Wedged sub: fill inbox to capacity without draining. ---
	wedged := newSub(1)
	inboxCap := cap(wedged.inbox)
	for i := 0; i < inboxCap; i++ {
		wedged.inbox <- taskPush
	}
	// inbox is now full. deliver() must return promptly (not block 30s).
	done := make(chan struct{})
	go func() {
		wedged.deliver(taskPush)
		close(done)
	}()
	select {
	case <-done:
		// Good: deliver returned without blocking.
	case <-time.After(5 * time.Second):
		t.Fatal("wedged sub: deliver blocked for >5s; backpressure admission is broken")
	}
	// The inbox is still full; any BACKPRESSURE notice was also dropped (warn
	// path). That's acceptable — the important property is non-blocking return.
	wedged.deliverSem.Close()

	// --- Healthy sub: inbox empty, writer goroutine draining. ---
	healthy := newSub(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We don't need a real stream for this unit test — just drain the inbox
	// directly to simulate a live reader.
	received := make(chan *pb.DownstreamMessage, 4)
	go func() {
		for {
			select {
			case msg, ok := <-healthy.inbox:
				if !ok {
					return
				}
				received <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	t1 := time.Now()
	healthy.deliver(taskPush)
	elapsed := time.Since(t1)
	if elapsed > 500*time.Millisecond {
		t.Errorf("healthy sub: deliver took %v; want <500ms", elapsed)
	}

	select {
	case msg := <-received:
		if _, ok := msg.GetPayload().(*pb.DownstreamMessage_TaskAssignment); !ok {
			t.Errorf("healthy sub got %T; want TaskAssignment", msg.GetPayload())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("healthy sub: message never reached inbox reader")
	}

	healthy.closeSubClient()
}

func recvMaybe(s pb.AetherGateway_ConnectClient, d time.Duration) *pb.DownstreamMessage {
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
			return nil
		}
		return r.msg
	case <-time.After(d):
		return nil
	}
}
