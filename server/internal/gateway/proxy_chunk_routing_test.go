package gateway

// Tests for ProxyHttpBodyChunk / ProxyHttpResponse routing via the
// per-request request-pin (T21).

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// TestRouteProxyHttpRequest_BodyChunked_InstallsPin asserts the gateway
// installs a request-pin (caller|service) when the parent request announces
// body_chunked=true, so follow-on chunks can find the destination.
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

	got, err := s.sessions.GetRequestPin(context.Background(), "req-chunked")
	if err != nil {
		t.Fatalf("GetRequestPin: %v", err)
	}
	if got == "" {
		t.Fatalf("expected request pin to be set after chunked-request publish")
	}
	caller, service := decodeRequestPin(got)
	if service != "sv::memorylayer::p" {
		t.Errorf("pinned service: got %q, want %q", service, "sv::memorylayer::p")
	}
	if caller != sender.ToTopic() {
		t.Errorf("pinned caller: got %q, want %q", caller, sender.ToTopic())
	}
}

// TestRouteProxyHttpBodyChunk_RequestDirection_ForwardsToService asserts an
// is_request=true chunk is published to the pinned service topic.
func TestRouteProxyHttpBodyChunk_RequestDirection_ForwardsToService(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	pinValue := encodeRequestPin(sender.ToTopic(), "sv::memorylayer::pinned")
	if err := s.sessions.SetRequestPin(context.Background(), "rid-1", pinValue, 0); err != nil {
		t.Fatalf("SetRequestPin: %v", err)
	}

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
}

// TestRouteProxyHttpBodyChunk_ResponseDirection_ForwardsToCaller asserts an
// is_request=false chunk is routed back to the pinned caller topic.
func TestRouteProxyHttpBodyChunk_ResponseDirection_ForwardsToCaller(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	pinValue := encodeRequestPin("ag::ws1::caller::v1", "sv::memorylayer::p")
	if err := s.sessions.SetRequestPin(context.Background(), "rid-2", pinValue, 0); err != nil {
		t.Fatalf("SetRequestPin: %v", err)
	}

	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-2",
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
}

// TestRouteProxyHttpBodyChunk_FinResponseClearsPin verifies the request pin
// is deleted on the terminal response chunk so it doesn't linger.
func TestRouteProxyHttpBodyChunk_FinResponseClearsPin(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	pinValue := encodeRequestPin("ag::ws1::caller::v1", "sv::memorylayer::p")
	_ = s.sessions.SetRequestPin(context.Background(), "rid-fin", pinValue, 0)

	finChunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-fin",
		IsRequest: false,
		Seq:       3,
		Data:      []byte("last"),
		Fin:       true,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpBodyChunk: finChunk})

	got, _ := s.sessions.GetRequestPin(context.Background(), "rid-fin")
	if got != "" {
		t.Errorf("expected pin cleared after fin response chunk, got %q", got)
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
// ProxyHttpResponse from a sidecar lands on the pinned caller's stream.
func TestRouteProxyHttpResponse_RoutesToPinnedCaller(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "p"}
	client := newProxyClient(sender, stream)

	pinValue := encodeRequestPin("ag::ws1::caller::v1", "sv::memorylayer::p")
	_ = s.sessions.SetRequestPin(context.Background(), "rid-resp", pinValue, 0)

	resp := &pb.ProxyHttpResponse{
		RequestId:  "rid-resp",
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
	// Single-shot response → pin cleared.
	got, _ := s.sessions.GetRequestPin(context.Background(), "rid-resp")
	if got != "" {
		t.Errorf("expected pin cleared after single-shot response, got %q", got)
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

	pinValue := encodeRequestPin("ag::ws1::caller::v1", "sv::memorylayer::p")
	_ = s.sessions.SetRequestPin(context.Background(), "rid-resp-chunk", pinValue, 0)

	resp := &pb.ProxyHttpResponse{
		RequestId:   "rid-resp-chunk",
		StatusCode:  200,
		BodyChunked: true,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpResp: resp})

	got, _ := s.sessions.GetRequestPin(context.Background(), "rid-resp-chunk")
	if got == "" {
		t.Errorf("expected pin retained for chunked response, got empty")
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
