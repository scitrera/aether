package gateway

// Tests for the single-node proxy/tunnel data-plane bypass (Phase 6 / T31).
//
// The contract under test:
//   - Return TunnelData/TunnelAck and ProxyHttpBodyChunk take a direct local
//     Deliver path when the destination is registered in the same gateway's
//     identityIndex AND the bypass flag is enabled. RMQ is not touched on a hit.
//   - Caller-to-target tunnel frames follow TunnelOpen through the broker,
//     so the local fast path cannot overtake an open still in transit.
//   - Control-plane envelopes (TunnelOpen, TunnelClose header,
//     ProxyHttpRequest header, ProxyHttpResponse header, ProxyError) ALWAYS
//     go through RMQ regardless of local connectivity, so audit emission is
//     preserved. This is the hard invariant.
//   - When the bypass flag is disabled (config or env), even data-plane
//     envelopes go through RMQ.
//   - When the target's deliveryCh is full, the helper falls back to RMQ
//     instead of stalling the routing loop.

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/models"
	"google.golang.org/protobuf/proto"
)

// seedTunnelPinPair writes the production three-tuple tunnel pin (primary
// wireID-keyed entry + per-caller forward index) directly into the session
// store. Used by bypass tests to mimic what routeTunnelOpen sets up.
func seedTunnelPinPair(s *GatewayServer, caller, service, originalID string, ttl time.Duration) string {
	wireID := newWireID()
	pinValue := encodeTunnelPin3(originalID, caller, service)
	_ = s.sessions.SetTunnelPin(context.Background(), wireID, pinValue, ttl)
	_ = s.sessions.SetTunnelPin(context.Background(), scopedPinID(caller, originalID), wireID, ttl)
	return wireID
}

// newLocalSidecarSession registers a fake sidecar ClientSession in the
// gateway's activeStreams + identityIndex with a deliveryCh of the requested
// buffer size. The returned session can be drained to assert local delivery.
func newLocalSidecarSession(s *GatewayServer, identity string, bufSize int) *ClientSession {
	parsed, _ := models.ParseIdentity(identity)
	stream := &mockStream{}
	cs := &ClientSession{
		ID:            "sidecar-session-" + identity,
		Identity:      parsed,
		Stream:        stream,
		subscriptions: make(map[string]func()),
		deliveryCh:    make(chan *pb.DownstreamMessage, bufSize),
	}
	s.activeStreams.Store(cs.ID, cs)
	s.identityIndex.Store(identity, cs.ID)
	return cs
}

// drainOne waits up to a short deadline for one message on the local sidecar's
// deliveryCh and returns it (or nil on timeout).
func drainOne(ch <-chan *pb.DownstreamMessage) *pb.DownstreamMessage {
	select {
	case msg := <-ch:
		return msg
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

// ---------------------------------------------------------------------------
// 1. TunnelData with local pin → fast path (Deliver, no RMQ publish)
// ---------------------------------------------------------------------------

func TestTunnelData_LocalCaller_TakesBypass(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "tcp-svc", Specifier: "pod-A"}
	client := newProxyClient(sender, stream)

	const target = "ag::ws1::caller::v1"
	caller := newLocalSidecarSession(s, target, 4)
	wireID := seedTunnelPinPair(s, target, sender.ToTopic(), "tun-local", time.Minute)

	data := &pb.TunnelData{TunnelId: wireID, Seq: 1, Data: []byte("ping")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	// RMQ must NOT be touched.
	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected 0 RMQ publishes on local fast path, got %d", pubs)
	}

	// Local delivery must have happened.
	got := drainOne(caller.deliveryCh)
	if got == nil {
		t.Fatal("expected TunnelData on caller deliveryCh")
	}
	if got.GetTunnelData().GetTunnelId() != "tun-local" || string(got.GetTunnelData().GetData()) != "ping" {
		t.Errorf("unexpected payload on bypass: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// 2. TunnelData with remote pin → RMQ path (no local sidecar registered)
// ---------------------------------------------------------------------------

func TestTunnelData_RemotePin_FallsBackToRMQ(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "tcp-svc", Specifier: "pod-A"}
	client := newProxyClient(sender, stream)

	// Pin to a target that is NOT registered locally, but IS active in
	// Redis (so the routing path treats the pinned principal as reachable).
	const target = "ag::ws1::caller::remote"
	wireID := seedTunnelPinPair(s, target, sender.ToTopic(), "tun-remote", time.Minute)
	s.sessions.(*mockSessionManager).isActiveResult = true

	data := &pb.TunnelData{TunnelId: wireID, Seq: 1, Data: []byte("via-rmq")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 RMQ publish on remote pin, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != target {
		t.Errorf("unexpected RMQ target: got %q want %q", router.publishedMessages[0].topic, target)
	}
}

// ---------------------------------------------------------------------------
// 3. TunnelAck local → fast path
// ---------------------------------------------------------------------------

func TestTunnelAck_LocalCaller_TakesBypass(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	// Sender is the service side; ack must travel back to the caller.
	sender := models.Identity{Type: models.PrincipalService, Implementation: "tcp-svc", Specifier: "pod-A"}
	client := newProxyClient(sender, stream)

	const callerTopic = "ag::ws1::caller::v1"
	const serviceTopic = "sv::tcp-svc::pod-A"
	caller := newLocalSidecarSession(s, callerTopic, 4)

	// Pin from the caller's perspective; routeTunnelAck looks up wireID
	// directly when the frame carries it (service-direction).
	wireID := seedTunnelPinPair(s, callerTopic, serviceTopic, "tun-ack", time.Minute)

	ack := &pb.TunnelAck{TunnelId: wireID, AckSeq: 7, Credits: 42}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelAck: ack})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected 0 RMQ publishes when caller is locally connected, got %d", pubs)
	}

	got := drainOne(caller.deliveryCh)
	if got == nil || got.GetTunnelAck() == nil {
		t.Fatalf("expected TunnelAck on caller's deliveryCh, got %+v", got)
	}
	if got.GetTunnelAck().Credits != 42 || got.GetTunnelAck().AckSeq != 7 {
		t.Errorf("unexpected ack payload: %+v", got.GetTunnelAck())
	}
}

// ---------------------------------------------------------------------------
// 4. ProxyHttpBodyChunk local → fast path
// ---------------------------------------------------------------------------

func TestProxyHttpBodyChunk_LocalService_TakesBypass(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	// Caller-direction chunk (is_request=true) goes from caller → service.
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	const callerTopic = "ag::ws1::caller::v1"
	const serviceTopic = "sv::memorylayer::pod-A"
	service := newLocalSidecarSession(s, serviceTopic, 4)

	// Production layout: primary wireID-keyed three-tuple + per-caller
	// forward index. is_request=true chunks travel caller → service; the
	// gateway translates "rid-local" (caller-side originalID) → wireID
	// using the forward index.
	wireID := newWireID()
	pinValue := encodeRequestPin3("rid-local", callerTopic, serviceTopic)
	_ = s.sessions.SetRequestPin(context.Background(), wireID, pinValue, time.Minute)
	_ = s.sessions.SetRequestPin(context.Background(), scopedPinID(sender.ToTopic(), "rid-local"), wireID, time.Minute)

	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-local",
		IsRequest: true,
		Seq:       1,
		Data:      []byte("body-bytes"),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpBodyChunk: chunk})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected 0 RMQ publishes on local body-chunk fast path, got %d", pubs)
	}

	got := drainOne(service.deliveryCh)
	if got == nil || got.GetProxyHttpBodyChunk() == nil {
		t.Fatalf("expected body chunk on service deliveryCh, got %+v", got)
	}
	if string(got.GetProxyHttpBodyChunk().Data) != "body-bytes" {
		t.Errorf("unexpected chunk payload on bypass: %+v", got.GetProxyHttpBodyChunk())
	}
}

// ---------------------------------------------------------------------------
// 5. Local bypass disabled → RMQ even when target is local
// ---------------------------------------------------------------------------

func TestTunnelData_BypassDisabled_UsesRMQEvenLocally(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.proxyLocalBypassEnabled = false
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "tcp-svc", Specifier: "pod-A"}
	client := newProxyClient(sender, stream)

	const target = "ag::ws1::caller::v1"
	caller := newLocalSidecarSession(s, target, 4)
	wireID := seedTunnelPinPair(s, target, sender.ToTopic(), "tun-disabled", time.Minute)

	data := &pb.TunnelData{TunnelId: wireID, Seq: 1, Data: []byte("rmq-only")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Errorf("expected RMQ publish when bypass disabled, got %d", pubs)
	}
	// Caller deliveryCh should be empty.
	select {
	case got := <-caller.deliveryCh:
		t.Errorf("expected empty deliveryCh when bypass disabled, got %+v", got)
	default:
	}
}

// ---------------------------------------------------------------------------
// 6. Deliver full buffer → falls back to RMQ
// ---------------------------------------------------------------------------

func TestTunnelData_FullDeliveryBuffer_FallsBackToRMQ(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "tcp-svc", Specifier: "pod-A"}
	client := newProxyClient(sender, stream)

	const target = "ag::ws1::caller::v1"
	caller := newLocalSidecarSession(s, target, 1)
	// Pre-fill the deliveryCh so the bypass select finds it full.
	caller.deliveryCh <- &pb.DownstreamMessage{}

	wireID := seedTunnelPinPair(s, target, sender.ToTopic(), "tun-full", time.Minute)

	data := &pb.TunnelData{TunnelId: wireID, Seq: 1, Data: []byte("overflow")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Errorf("expected RMQ fallback publish on full buffer, got %d publishes", pubs)
	}
}

// ---------------------------------------------------------------------------
// 7. CRITICAL: TunnelOpen with local pin → still goes through RMQ
// ---------------------------------------------------------------------------

func TestTunnelOpen_LocalSidecar_StillGoesThroughRMQ(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	const target = "sv::tcp-svc::pod-A"
	sidecar := newLocalSidecarSession(s, target, 4)
	// Tell wildcard resolver this instance exists locally so resolution lands here.

	open := &pb.TunnelOpen{
		TunnelId:    "tun-open-audit",
		TargetTopic: target,
		Protocol:    pb.TunnelOpen_TCP,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelOpen: open})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Fatalf("INVARIANT: TunnelOpen must go through RMQ, got %d publishes", pubs)
	}
	// Sidecar's deliveryCh should NOT receive the open frame via the bypass.
	select {
	case got := <-sidecar.deliveryCh:
		t.Errorf("INVARIANT BROKEN: TunnelOpen leaked into local deliveryCh: %+v", got)
	default:
	}
}

// ---------------------------------------------------------------------------
// 8. CRITICAL: TunnelClose with local pin → still goes through RMQ
// ---------------------------------------------------------------------------

func TestTunnelClose_LocalSidecar_StillGoesThroughRMQ(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	const target = "sv::tcp-svc::pod-A"
	sidecar := newLocalSidecarSession(s, target, 4)
	seedTunnelPinPair(s, sender.ToTopic(), target, "tun-close-audit", time.Minute)
	s.tunnelCounterFor("ws1").n.Store(1)

	closeMsg := &pb.TunnelClose{TunnelId: "tun-close-audit", Reason: pb.TunnelClose_NORMAL}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelClose: closeMsg})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Fatalf("INVARIANT: TunnelClose must go through RMQ, got %d publishes", pubs)
	}
	select {
	case got := <-sidecar.deliveryCh:
		t.Errorf("INVARIANT BROKEN: TunnelClose leaked into local deliveryCh: %+v", got)
	default:
	}
}

// ---------------------------------------------------------------------------
// 9. CRITICAL: ProxyHttpRequest header with local sidecar → still goes through RMQ
// ---------------------------------------------------------------------------

func TestProxyHttpRequest_LocalSidecar_StillGoesThroughRMQ(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newProxyClient(sender, stream)

	const target = "sv::memorylayer::pod-A"
	sidecar := newLocalSidecarSession(s, target, 4)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-audit",
		TargetTopic: target,
		Method:      "POST",
		Path:        "/v1/items",
		Body:        []byte("{}"),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Fatalf("INVARIANT: ProxyHttpRequest header must go through RMQ, got %d publishes", pubs)
	}
	select {
	case got := <-sidecar.deliveryCh:
		t.Errorf("INVARIANT BROKEN: ProxyHttpRequest leaked into local deliveryCh: %+v", got)
	default:
	}
}

// Hold broker delivery until all caller frames have been routed. This makes
// the open/data race deterministic: a local bypass would deliver data before
// the target has received TunnelOpen and reserved the tunnel.
func TestTunnelCallerFrames_DoNotOvertakeBrokeredOpen(t *testing.T) {
	for _, target := range []string{"sv::tcp-svc::pod-A", "ag::ws1::tcp-agent::v1"} {
		t.Run(target, func(t *testing.T) {
			router := newMockMessageRouter()
			s := newProxyTestServer(router)
			sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
			client := newProxyClient(sender, &mockStream{})
			peer := newLocalSidecarSession(s, target, 8)
			ctx := context.Background()
			const originalID = "tun-ordered"
			open := &pb.TunnelOpen{TunnelId: originalID, TargetTopic: target, Protocol: pb.TunnelOpen_TCP}
			s.routeProxyEnvelope(ctx, client, proxyEnvelope{tunnelOpen: open})
			s.routeProxyEnvelope(ctx, client, proxyEnvelope{tunnelData: &pb.TunnelData{TunnelId: originalID, Seq: 1, Data: []byte("first")}})
			s.routeProxyEnvelope(ctx, client, proxyEnvelope{tunnelData: &pb.TunnelData{TunnelId: originalID, Seq: 2, Data: []byte("second")}})
			s.routeProxyEnvelope(ctx, client, proxyEnvelope{tunnelAck: &pb.TunnelAck{TunnelId: originalID, AckSeq: 1, Credits: 42}})
			s.routeProxyEnvelope(ctx, client, proxyEnvelope{tunnelClose: &pb.TunnelClose{TunnelId: originalID, Reason: pb.TunnelClose_NORMAL}})

			select {
			case got := <-peer.deliveryCh:
				t.Fatalf("frame overtook brokered TunnelOpen: %v", got)
			default:
			}
			router.mu.Lock()
			published := append([]publishedMsg(nil), router.publishedMessages...)
			router.mu.Unlock()
			if len(published) != 5 {
				t.Fatalf("expected open, two data frames, ack and close on the same broker path; got %d", len(published))
			}

			// Release the broker backlog through the real subscription handler.
			deliver := s.createMessageHandler(peer)
			for _, msg := range published {
				if msg.topic != target {
					t.Fatalf("published to %q, want %q", msg.topic, target)
				}
				deliver(msg.payload)
			}
			gotOpen := drainOne(peer.deliveryCh).GetTunnelData()
			if gotOpen == nil || gotOpen.GetSeq() != 0 {
				t.Fatalf("first delivery must be TunnelOpen, got %v", gotOpen)
			}
			var decodedOpen pb.TunnelOpen
			if err := proto.Unmarshal(gotOpen.GetData(), &decodedOpen); err != nil {
				t.Fatal(err)
			}
			wireID := decodedOpen.GetTunnelId()
			if wireID == "" || wireID == originalID || gotOpen.GetTunnelId() != wireID || decodedOpen.GetTargetTopic() != target {
				t.Fatalf("invalid forwarded open: %v", &decodedOpen)
			}
			for i, payload := range []string{"first", "second"} {
				got := drainOne(peer.deliveryCh).GetTunnelData()
				if got.GetTunnelId() != wireID || got.GetSeq() != uint32(i+1) || string(got.GetData()) != payload {
					t.Fatalf("data delivery %d out of order: %v", i+1, got)
				}
			}
			ack := drainOne(peer.deliveryCh).GetTunnelAck()
			if ack.GetTunnelId() != wireID || ack.GetAckSeq() != 1 || ack.GetCredits() != 42 {
				t.Fatalf("unexpected ack: %v", ack)
			}
			closeMsg := drainOne(peer.deliveryCh).GetTunnelClose()
			if closeMsg == nil || closeMsg.GetTunnelId() != wireID || closeMsg.GetReason() != pb.TunnelClose_NORMAL {
				t.Fatalf("unexpected close: %v", closeMsg)
			}
		})
	}
}
