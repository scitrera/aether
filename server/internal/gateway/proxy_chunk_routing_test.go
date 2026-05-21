package gateway

// Tests for ProxyHttpBodyChunk / ProxyHttpResponse routing via the
// per-request request-pin (T21).

import (
	"context"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
	"google.golang.org/protobuf/proto"
)

// unwrapProxyDownstream decodes the published wire payload (a
// MessageEnvelope wrapping an inner DownstreamMessage marshaled by
// publishProxyEnvelope) so tests can inspect the inner frame's request_id /
// tunnel_id rewrites.
func unwrapProxyDownstream(t *testing.T, payload []byte) *pb.DownstreamMessage {
	t.Helper()
	var env pb.MessageEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		t.Fatalf("unmarshal MessageEnvelope: %v", err)
	}
	var inner pb.DownstreamMessage
	if err := proto.Unmarshal(env.Payload, &inner); err != nil {
		t.Fatalf("unmarshal DownstreamMessage: %v", err)
	}
	return &inner
}

// fetchPrimaryRequestPin looks up the primary (wireID-keyed) request pin
// installed by routeProxyHttpRequest. It walks the per-caller forward index
// (scopedPinID(caller, originalID) → wireID) to find the wireID, then fetches
// the three-tuple pin value.
func fetchPrimaryRequestPin(t *testing.T, s *GatewayServer, caller, originalID string) (wireID, pinValue string) {
	t.Helper()
	indirect, err := s.sessions.GetRequestPin(context.Background(), scopedPinID(caller, originalID))
	if err != nil {
		t.Fatalf("GetRequestPin(forward index): %v", err)
	}
	if indirect == "" {
		return "", ""
	}
	wireID = indirect
	pinValue, err = s.sessions.GetRequestPin(context.Background(), wireID)
	if err != nil {
		t.Fatalf("GetRequestPin(wireID): %v", err)
	}
	return wireID, pinValue
}

// TestRouteProxyHttpRequest_BodyChunked_InstallsPin asserts the gateway
// installs a wireID-keyed three-tuple pin (originalID|caller|service) and a
// per-caller forward index when the parent request announces body_chunked=
// true. The wireID stamped on the outbound envelope must carry the
// gateway-minted wireIDPrefix so the service-side echo lookup is
// collision-free.
func TestRouteProxyHttpRequest_BodyChunked_InstallsPin(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.sessions.(*mockSessionManager).serviceInstances = []string{"sv::memorylayer::p"}
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-chunked",
		TargetTopic: "sv::memorylayer",
		Method:      "POST",
		Path:        "/v1/upload",
		BodyChunked: true,
		TimeoutMs:   30_000,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	wireID, pinValue := fetchPrimaryRequestPin(t, s, sender.ToTopic(), "req-chunked")
	if wireID == "" {
		t.Fatalf("expected forward index to point at a wireID after chunked-request publish")
	}
	if !strings.HasPrefix(wireID, wireIDPrefix) {
		t.Errorf("forward-index value should be a wireID (prefix %q), got %q", wireIDPrefix, wireID)
	}
	if pinValue == "" {
		t.Fatalf("expected primary pin to be set under wireID %q", wireID)
	}
	originalID, caller, service := decodeRequestPin3(pinValue)
	if originalID != "req-chunked" {
		t.Errorf("pinned originalID: got %q, want %q", originalID, "req-chunked")
	}
	if service != "sv::memorylayer::p" {
		t.Errorf("pinned service: got %q, want %q", service, "sv::memorylayer::p")
	}
	if caller != sender.ToTopic() {
		t.Errorf("pinned caller: got %q, want %q", caller, sender.ToTopic())
	}
	// The published envelope must carry the wireID (the gateway rewrites
	// req.RequestId before publishing so the service correlates against a
	// unique id).
	if req.GetRequestId() != wireID {
		t.Errorf("published request_id: got %q, want %q", req.GetRequestId(), wireID)
	}
}

// seedRequestPinPair writes the production three-tuple pin pair (primary
// wireID-keyed entry + per-caller forward index) directly into the session
// store. Returns the wireID for assertions.
func seedRequestPinPair(t *testing.T, s *GatewayServer, caller, service, originalID string) string {
	t.Helper()
	wireID := newWireID()
	pinValue := encodeRequestPin3(originalID, caller, service)
	if err := s.sessions.SetRequestPin(context.Background(), wireID, pinValue, 0); err != nil {
		t.Fatalf("SetRequestPin(wireID): %v", err)
	}
	if err := s.sessions.SetRequestPin(context.Background(), scopedPinID(caller, originalID), wireID, 0); err != nil {
		t.Fatalf("SetRequestPin(forward index): %v", err)
	}
	return wireID
}

// TestRouteProxyHttpBodyChunk_RequestDirection_ForwardsToService asserts an
// is_request=true chunk is published to the pinned service topic. The chunk's
// caller-side originalID must be translated to the wireID before publish.
func TestRouteProxyHttpBodyChunk_RequestDirection_ForwardsToService(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	wireID := seedRequestPinPair(t, s, sender.ToTopic(), "sv::memorylayer::pinned", "rid-1")

	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-1",
		IsRequest: true,
		Seq:       0,
		Data:      []byte("hello world"),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpBodyChunk: chunk})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "sv::memorylayer::pinned" {
		t.Errorf("expected publish to pinned service, got %q", router.publishedMessages[0].topic)
	}
	// The forwarded chunk must carry the wireID so the service-side
	// correlation succeeds.
	inner := unwrapProxyDownstream(t, router.publishedMessages[0].payload)
	forwardedChunk := inner.GetProxyHttpBodyChunk()
	if forwardedChunk == nil {
		t.Fatalf("expected ProxyHttpBodyChunk payload, got %+v", inner)
	}
	if forwardedChunk.GetRequestId() != wireID {
		t.Errorf("forwarded chunk request_id: got %q, want %q (wireID)", forwardedChunk.GetRequestId(), wireID)
	}
}

// TestRouteProxyHttpBodyChunk_ResponseDirection_ForwardsToCaller asserts an
// is_request=false chunk (service-direction echo) is routed back to the
// pinned caller topic with the caller-side originalID restored.
func TestRouteProxyHttpBodyChunk_ResponseDirection_ForwardsToCaller(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	wireID := seedRequestPinPair(t, s, "ag::ws1::caller::v1", sender.ToTopic(), "rid-2")

	// Service echoes whatever id was on the request — that's wireID.
	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: wireID,
		IsRequest: false,
		Seq:       0,
		Data:      []byte("response data"),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpBodyChunk: chunk})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "ag::ws1::caller::v1" {
		t.Errorf("expected publish to pinned caller, got %q", router.publishedMessages[0].topic)
	}
	inner := unwrapProxyDownstream(t, router.publishedMessages[0].payload)
	forwardedChunk := inner.GetProxyHttpBodyChunk()
	if forwardedChunk == nil {
		t.Fatalf("expected ProxyHttpBodyChunk payload, got %+v", inner)
	}
	// Caller-side originalID must be restored.
	if forwardedChunk.GetRequestId() != "rid-2" {
		t.Errorf("forwarded chunk request_id: got %q, want %q (originalID)", forwardedChunk.GetRequestId(), "rid-2")
	}
}

// TestRouteProxyHttpBodyChunk_FinResponseClearsPin verifies the request pin
// is deleted on the terminal response chunk so it doesn't linger.
func TestRouteProxyHttpBodyChunk_FinResponseClearsPin(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	wireID := seedRequestPinPair(t, s, "ag::ws1::caller::v1", sender.ToTopic(), "rid-fin")

	finChunk := &pb.ProxyHttpBodyChunk{
		RequestId: wireID,
		IsRequest: false,
		Seq:       3,
		Data:      []byte("last"),
		Fin:       true,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpBodyChunk: finChunk})

	// Both pin entries (primary wireID-keyed + forward index) should be
	// cleared after the terminal response chunk.
	primary, _ := s.sessions.GetRequestPin(context.Background(), wireID)
	if primary != "" {
		t.Errorf("expected primary pin cleared after fin response chunk, got %q", primary)
	}
	forward, _ := s.sessions.GetRequestPin(context.Background(), scopedPinID("ag::ws1::caller::v1", "rid-fin"))
	if forward != "" {
		t.Errorf("expected forward-index cleared after fin response chunk, got %q", forward)
	}
}

// TestRouteProxyHttpBodyChunk_PinMissing_RequestDirection_EmitsProxyError
// asserts that when the per-request pin is missing (TTL expired or never
// installed) a request-direction chunk emits a SIDECAR_UNAVAILABLE
// ProxyHttpResponse to the caller so it doesn't hang.
func TestRouteProxyHttpBodyChunk_PinMissing_RequestDirection_EmitsProxyError(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-missing",
		IsRequest: true,
		Data:      []byte("orphan"),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpBodyChunk: chunk})

	router.mu.Lock()
	if pubs := len(router.publishedMessages); pubs != 0 {
		t.Errorf("expected no publish on missing pin, got %d", pubs)
	}
	router.mu.Unlock()

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected ProxyHttpResponse error on missing pin")
	}
	resp := stream.sent[0].GetProxyHttpResponse()
	if resp == nil || resp.Error == nil {
		t.Fatalf("expected ProxyHttpResponse with error, got %+v", stream.sent[0])
	}
	if resp.Error.Kind != pb.ProxyError_SIDECAR_UNAVAILABLE {
		t.Errorf("expected SIDECAR_UNAVAILABLE on missing pin, got %v", resp.Error.Kind)
	}
}

// TestRouteProxyHttpResponse_RoutesToPinnedCaller asserts an upstream
// ProxyHttpResponse from a sidecar lands on the pinned caller's stream and
// carries the caller-side originalID (not the wireID).
func TestRouteProxyHttpResponse_RoutesToPinnedCaller(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	wireID := seedRequestPinPair(t, s, "ag::ws1::caller::v1", sender.ToTopic(), "rid-resp")

	resp := &pb.ProxyHttpResponse{
		RequestId:  wireID,
		StatusCode: 200,
		Body:       []byte("OK"),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpResp: resp})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "ag::ws1::caller::v1" {
		t.Errorf("expected publish to pinned caller, got %q", router.publishedMessages[0].topic)
	}
	inner := unwrapProxyDownstream(t, router.publishedMessages[0].payload)
	forwardedResp := inner.GetProxyHttpResponse()
	if forwardedResp == nil {
		t.Fatalf("expected ProxyHttpResponse payload, got %+v", inner)
	}
	if forwardedResp.GetRequestId() != "rid-resp" {
		t.Errorf("forwarded response request_id: got %q, want %q (originalID)", forwardedResp.GetRequestId(), "rid-resp")
	}
	// Single-shot response → both pin entries cleared.
	primary, _ := s.sessions.GetRequestPin(context.Background(), wireID)
	if primary != "" {
		t.Errorf("expected primary pin cleared after single-shot response, got %q", primary)
	}
	forward, _ := s.sessions.GetRequestPin(context.Background(), scopedPinID("ag::ws1::caller::v1", "rid-resp"))
	if forward != "" {
		t.Errorf("expected forward-index cleared after single-shot response, got %q", forward)
	}
}

// TestRouteProxyHttpResponse_BodyChunked_KeepsPin asserts that a
// body_chunked=true response leaves the pin in place so subsequent body
// chunks find the caller.
func TestRouteProxyHttpResponse_BodyChunked_KeepsPin(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	wireID := seedRequestPinPair(t, s, "ag::ws1::caller::v1", sender.ToTopic(), "rid-resp-chunk")

	resp := &pb.ProxyHttpResponse{
		RequestId:   wireID,
		StatusCode:  200,
		BodyChunked: true,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpResp: resp})

	got, _ := s.sessions.GetRequestPin(context.Background(), wireID)
	if got == "" {
		t.Errorf("expected primary pin retained for chunked response, got empty")
	}
}

// TestRequestPinTTL_AppliesTimeoutSlack asserts the helper produces a TTL
// that is at least timeout_ms (with a small slack) for non-zero inputs and
// the default for zero.
func TestRequestPinTTL_AppliesTimeoutSlack(t *testing.T) {
	if got := requestPinTTL(0); got <= 0 {
		t.Errorf("default TTL must be positive, got %v", got)
	}
	got := requestPinTTL(10_000) // 10s
	if got.Seconds() < 10 {
		t.Errorf("expected TTL >= timeout_ms, got %v", got)
	}
}
