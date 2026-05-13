package gateway

// Tests for routeProxyEnvelope() — proxy/tunnel routing primitives:
//   - Basic REST route success → audited as OpProxyHttpRouted, ProxyHttpResponse delivered.
//   - REST ACL deny path.
//   - Wildcard sv::{impl} with N=3 connected instances: chosen distribution.
//   - Tunnel pin & follow-on routing: TunnelOpen pins, TunnelData/Close use pin.
//   - Pin loss → PEER_RESET emitted to caller.
//   - Quota deny.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newProxyTestServer builds a GatewayServer wired for proxy routing tests.
// ACL is nil (no-op) by default; tests that exercise denial set it via the
// stub helper installAclDenier.
func newProxyTestServer(router *mockMessageRouter) *GatewayServer {
	s := newRoutingTestServer(router)
	// Ensure publish breaker is closed.
	s.publishBreaker = circuitbreaker.New("test-proxy-publish",
		circuitbreaker.WithMaxFailures(100))
	// Match the production default. NewGatewayServer sets this true; tests
	// that go through newTestGatewayWithMocks bypass that constructor.
	s.proxyLocalBypassEnabled = true
	return s
}

func newProxyClient(identity models.Identity, stream *mockStream) *ClientSession {
	c := newRoutingTestClient(identity, stream)
	c.SessionUUID = uuid.New()
	return c
}

// ---------------------------------------------------------------------------
// 1. Basic REST route success
// ---------------------------------------------------------------------------

func TestRouteProxyHttpRequest_Success_PublishesToConcreteTopic(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-001",
		TargetTopic: "sv::memorylayer::pod-a",
		Method:      "GET",
		Path:        "/v1/items",
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected exactly 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "sv::memorylayer::pod-a" {
		t.Errorf("expected publish to concrete sv::memorylayer::pod-a, got %q", router.publishedMessages[0].topic)
	}
}

func TestRouteProxyHttpRequest_PayloadTooLarge_ReturnsError(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxRequestBodyBytes = 16
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-toobig",
		TargetTopic: "sv::svc::x",
		Body:        make([]byte, 32),
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected ProxyHttpResponse error")
	}
	resp := stream.sent[0].GetProxyHttpResponse()
	if resp == nil || resp.Error == nil {
		t.Fatalf("expected ProxyHttpResponse with error, got %+v", stream.sent[0])
	}
	if resp.Error.Kind != pb.ProxyError_PAYLOAD_TOO_LARGE {
		t.Errorf("expected PAYLOAD_TOO_LARGE, got %v", resp.Error.Kind)
	}
}

// ---------------------------------------------------------------------------
// 2. REST ACL deny path
// ---------------------------------------------------------------------------

// TestRouteProxyHttpRequest_AgentTarget_Succeeds verifies that a concrete
// agent topic is now a valid proxy target (T34: lift service-class restriction).
// With nil ACL (implicit allow) the request must be published to the agent topic.
func TestRouteProxyHttpRequest_AgentTarget_Succeeds(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-agent-ok",
		TargetTopic: "ag::ws1::worker::v1",
		Method:      "POST",
		Path:        "/tool/invoke",
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish to agent topic, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "ag::ws1::worker::v1" {
		t.Errorf("expected publish to ag::ws1::worker::v1, got %q", router.publishedMessages[0].topic)
	}
}

// TestRouteTunnelOpen_AgentTarget_Succeeds verifies that TunnelOpen to an agent
// topic is accepted and pinned correctly (T34: lift service-class restriction).
func TestRouteTunnelOpen_AgentTarget_Succeeds(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	open := &pb.TunnelOpen{
		TunnelId:    "tun-agent",
		TargetTopic: "ag::ws1::worker::v1",
		Protocol:    pb.TunnelOpen_TCP,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelOpen: open})

	pinned, _ := s.sessions.GetTunnelPin(context.Background(), "tun-agent")
	_, service := decodeTunnelPin(pinned)
	if service != "ag::ws1::worker::v1" {
		t.Errorf("expected pin → ag::ws1::worker::v1, got %q (pin=%q)", service, pinned)
	}
	if s.tunnelCounterFor("ws1").n.Load() != 1 {
		t.Errorf("expected active tunnel count = 1, got %d", s.tunnelCounterFor("ws1").n.Load())
	}
	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Errorf("expected 1 publish for TunnelOpen to agent, got %d", pubs)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for _, m := range stream.sent {
		if c := m.GetTunnelClose(); c != nil {
			t.Errorf("unexpected TunnelClose: reason=%v detail=%q", c.Reason, c.Detail)
		}
	}
}

// ---------------------------------------------------------------------------
// 3. Wildcard resolution
// ---------------------------------------------------------------------------

func TestRouteProxyHttpRequest_Wildcard_ResolvesToConcrete(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	// Seed 3 cluster-wide instances via mock session manager.
	s.sessions.(*mockSessionManager).serviceInstances = []string{
		"sv::memorylayer::a", "sv::memorylayer::b", "sv::memorylayer::c",
	}
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// Send 60 wildcard requests; verify each lands on one of the 3 known
	// concrete topics, and that the distribution touches all three over
	// many trials. Random distribution → occasional empty bucket is
	// acceptable; we just ensure ≥2 buckets receive at least one hit.
	const N = 60
	for i := 0; i < N; i++ {
		req := &pb.ProxyHttpRequest{
			RequestId:   "req-" + string(rune('a'+i%26)),
			TargetTopic: "sv::memorylayer",
		}
		s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != N {
		t.Fatalf("expected %d publishes, got %d", N, len(router.publishedMessages))
	}
	hits := map[string]int{}
	for _, p := range router.publishedMessages {
		hits[p.topic]++
		if !strings.HasPrefix(p.topic, "sv::memorylayer::") {
			t.Errorf("publish on non-concrete topic: %q", p.topic)
		}
	}
	if len(hits) < 2 {
		t.Errorf("expected at least 2 concrete instances to receive traffic, got %d: %v", len(hits), hits)
	}
}

func TestRouteProxyHttpRequest_Wildcard_NoInstances_ReturnsSidecarUnavailable(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	// No instances seeded.
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-empty",
		TargetTopic: "sv::nobody",
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish when no instances, got %d", pubs)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected error response")
	}
	resp := stream.sent[0].GetProxyHttpResponse()
	if resp == nil || resp.Error == nil || resp.Error.Kind != pb.ProxyError_SIDECAR_UNAVAILABLE {
		t.Errorf("expected SIDECAR_UNAVAILABLE, got %+v", stream.sent[0])
	}
}

func TestRouteProxyHttpRequest_Wildcard_PrefersLocalInstance(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	// Local index has one instance. Cluster scan should NOT be consulted.
	s.identityIndex.Store("sv::memorylayer::local-pod", "session-1")
	// Seed cluster scan with different instances; if local fast-path works,
	// these should never be picked.
	s.sessions.(*mockSessionManager).serviceInstances = []string{
		"sv::memorylayer::remote-a", "sv::memorylayer::remote-b",
	}

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	for i := 0; i < 10; i++ {
		req := &pb.ProxyHttpRequest{RequestId: "r", TargetTopic: "sv::memorylayer"}
		s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	for _, p := range router.publishedMessages {
		if p.topic != "sv::memorylayer::local-pod" {
			t.Errorf("expected local-pod fast-path, got %q", p.topic)
		}
	}
}

// ---------------------------------------------------------------------------
// 4. Tunnel pin & follow-on routing
// ---------------------------------------------------------------------------

func TestRouteTunnelOpen_PinsToConcreteAndPublishes(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.sessions.(*mockSessionManager).serviceInstances = []string{"sv::tcp-svc::pod-1"}
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	open := &pb.TunnelOpen{
		TunnelId:    "tun-1",
		TargetTopic: "sv::tcp-svc",
		Protocol:    pb.TunnelOpen_TCP,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelOpen: open})

	// Pin must be set; decode caller|service form.
	pinned, _ := s.sessions.GetTunnelPin(context.Background(), "tun-1")
	_, service := decodeTunnelPin(pinned)
	if service != "sv::tcp-svc::pod-1" {
		t.Errorf("expected pin service → sv::tcp-svc::pod-1, got pin=%q (service=%q)", pinned, service)
	}

	// Counter must increment.
	if s.tunnelCounterFor("ws1").n.Load() != 1 {
		t.Errorf("expected active tunnel count = 1, got %d", s.tunnelCounterFor("ws1").n.Load())
	}

	// Should have published the open envelope.
	router.mu.Lock()
	pubs := len(router.publishedMessages)
	target := ""
	if pubs > 0 {
		target = router.publishedMessages[0].topic
	}
	router.mu.Unlock()
	if pubs != 1 {
		t.Errorf("expected 1 publish, got %d", pubs)
	}
	if target != "sv::tcp-svc::pod-1" {
		t.Errorf("expected publish to concrete pod, got %q", target)
	}
}

func TestRouteTunnelData_FollowsExistingPin(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// Pre-seed a pin and mark the pinned identity locally connected.
	_ = s.sessions.SetTunnelPin(context.Background(), "tun-2", "sv::tcp-svc::pod-X", time.Minute)
	s.identityIndex.Store("sv::tcp-svc::pod-X", "session-X")

	data := &pb.TunnelData{TunnelId: "tun-2", Seq: 1, Data: []byte("hello")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 data publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "sv::tcp-svc::pod-X" {
		t.Errorf("expected publish to pinned pod, got %q", router.publishedMessages[0].topic)
	}
}

func TestRouteTunnelClose_DeletesPinAndDecrementsCounter(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	_ = s.sessions.SetTunnelPin(context.Background(), "tun-3", "sv::svc::pod", time.Minute)
	s.tunnelCounterFor("ws1").n.Store(1)

	closeMsg := &pb.TunnelClose{TunnelId: "tun-3", Reason: pb.TunnelClose_NORMAL}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelClose: closeMsg})

	got, _ := s.sessions.GetTunnelPin(context.Background(), "tun-3")
	if got != "" {
		t.Errorf("expected pin deleted, got %q", got)
	}
	if s.tunnelCounterFor("ws1").n.Load() != 0 {
		t.Errorf("expected tunnel count = 0, got %d", s.tunnelCounterFor("ws1").n.Load())
	}
}

// ---------------------------------------------------------------------------
// 5. Pin loss → PEER_RESET
// ---------------------------------------------------------------------------

func TestRouteTunnelData_PinMissing_EmitsPeerReset(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// No pin set; expect PEER_RESET emitted to caller, no publish.
	data := &pb.TunnelData{TunnelId: "tun-missing", Seq: 1, Data: []byte("x")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish on missing pin, got %d", pubs)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected TunnelClose emitted to caller")
	}
	closeMsg := stream.sent[0].GetTunnelClose()
	if closeMsg == nil {
		t.Fatalf("expected TunnelClose, got %+v", stream.sent[0])
	}
	if closeMsg.Reason != pb.TunnelClose_PEER_RESET {
		t.Errorf("expected PEER_RESET, got %v", closeMsg.Reason)
	}
}

func TestRouteTunnelData_PinnedPrincipalDisconnected_EmitsPeerReset(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// Set a pin to a principal that is NOT in identityIndex AND not active in Redis.
	_ = s.sessions.SetTunnelPin(context.Background(), "tun-stale", "sv::svc::ghost", time.Minute)
	s.tunnelCounterFor("ws1").n.Store(1)
	// mockSessionManager.IsActive defaults to true for all queries; flip it
	// off so the followPin path treats the pinned principal as gone.
	s.sessions.(*mockSessionManager).isActiveResult = false

	data := &pb.TunnelData{TunnelId: "tun-stale", Seq: 1, Data: []byte("x")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: data})

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 || stream.sent[0].GetTunnelClose() == nil {
		t.Fatalf("expected TunnelClose emitted, got %+v", stream.sent)
	}
	if stream.sent[0].GetTunnelClose().Reason != pb.TunnelClose_PEER_RESET {
		t.Errorf("expected PEER_RESET, got %v", stream.sent[0].GetTunnelClose().Reason)
	}
	// Pin must be cleared.
	got, _ := s.sessions.GetTunnelPin(context.Background(), "tun-stale")
	if got != "" {
		t.Errorf("expected pin cleared on PEER_RESET, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// 6. Quota deny
// ---------------------------------------------------------------------------

func TestRouteTunnelOpen_QuotaExceeded_EmitsQuotaClose(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxConcurrentTunnelsPerWorkspace = 1
	s.tunnelCounterFor("ws1").n.Store(1) // already at cap
	s.sessions.(*mockSessionManager).serviceInstances = []string{"sv::svc::a"}

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	open := &pb.TunnelOpen{TunnelId: "tun-cap", TargetTopic: "sv::svc"}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelOpen: open})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish on quota deny, got %d", pubs)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 || stream.sent[0].GetTunnelClose() == nil {
		t.Fatalf("expected TunnelClose, got %+v", stream.sent)
	}
	if stream.sent[0].GetTunnelClose().Reason != pb.TunnelClose_QUOTA {
		t.Errorf("expected QUOTA, got %v", stream.sent[0].GetTunnelClose().Reason)
	}
}

func TestRouteTunnelData_PerTunnelByteCapExceeded_ClosesWithQuota(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxTunnelBytes = 8
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	_ = s.sessions.SetTunnelPin(context.Background(), "tun-bytes", "sv::svc::pod", time.Minute)
	s.identityIndex.Store("sv::svc::pod", "session-1")
	s.tunnelCounterFor("ws1").n.Store(1)

	// First frame within cap.
	first := &pb.TunnelData{TunnelId: "tun-bytes", Data: []byte("0123")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: first})
	// Second frame trips cap: total bytes=8 exactly equals cap is "exceeded"
	// per the > comparison; tweak so we cross cleanly.
	second := &pb.TunnelData{TunnelId: "tun-bytes", Data: []byte("45678")}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelData: second})

	stream.mu.Lock()
	defer stream.mu.Unlock()
	hasQuotaClose := false
	for _, m := range stream.sent {
		if c := m.GetTunnelClose(); c != nil && c.Reason == pb.TunnelClose_QUOTA {
			hasQuotaClose = true
			break
		}
	}
	if !hasQuotaClose {
		t.Errorf("expected QUOTA TunnelClose after per-tunnel byte cap, got %+v", stream.sent)
	}
}

// ---------------------------------------------------------------------------
// 7. Audit ops are emitted
// ---------------------------------------------------------------------------

// auditCapture is a lightweight stand-in for the audit logger used to verify
// that routeProxy paths emit the correct ops. We bypass auditLogger entirely
// here — auditLog() uses s.auditLogger which is nil in tests; instead we
// stage a counting hook by sniffing the metering / stream observability
// already present. For this test we simply confirm publish + reply were
// done correctly: the audit path is exercised but verifying its DB write
// belongs in audit/* tests.
//
// We DO assert that ProxyHttpResponse with non-nil error yields zero
// publishes — the audit failure path runs alongside the error reply.
func TestRouteProxyHttpRequest_FailureBranch_NoPublish(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// No instances + wildcard → SIDECAR_UNAVAILABLE → no publish, audit
	// failure (logged best-effort via auditLog → nil-safe).
	req := &pb.ProxyHttpRequest{RequestId: "f", TargetTopic: "sv::ghost"}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 0 {
		t.Errorf("expected no publish on failure path, got %d", len(router.publishedMessages))
	}
}

// ---------------------------------------------------------------------------
// 8. Wildcard health filter (mock-only sanity test).
// ---------------------------------------------------------------------------

// stubScanErr exercises the error path on cluster-wide discovery: when the
// session manager returns an error, routeProxy should reply with
// SIDECAR_UNAVAILABLE rather than publishing or panicking.
func TestRouteProxyHttpRequest_DiscoveryError_ReturnsSidecarUnavailable(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.sessions.(*mockSessionManager).serviceInstancesErr = errors.New("redis down")

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{RequestId: "d", TargetTopic: "sv::svc"}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected error response")
	}
	resp := stream.sent[0].GetProxyHttpResponse()
	if resp == nil || resp.Error == nil || resp.Error.Kind != pb.ProxyError_SIDECAR_UNAVAILABLE {
		t.Errorf("expected SIDECAR_UNAVAILABLE, got %+v", stream.sent[0])
	}
}

// ---------------------------------------------------------------------------
// 9. tunnelByteCounter idempotency
// ---------------------------------------------------------------------------

func TestTunnelByteCounter_PerTunnelIsolated(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)

	a := s.tunnelByteCounterFor("a")
	b := s.tunnelByteCounterFor("b")
	a.Add(100)
	b.Add(200)
	if a.Load() != 100 || b.Load() != 200 {
		t.Errorf("counters mixed: a=%d b=%d", a.Load(), b.Load())
	}
	s.deleteTunnelByteCounter("a")
	a2 := s.tunnelByteCounterFor("a")
	if a2.Load() != 0 {
		t.Errorf("expected fresh counter after delete, got %d", a2.Load())
	}
}

// ---------------------------------------------------------------------------
// 10. proxyEnvelopeFromUpstream dispatches by oneof
// ---------------------------------------------------------------------------

func TestProxyEnvelopeFromUpstream_AllVariants(t *testing.T) {
	cases := []struct {
		name    string
		req     *pb.UpstreamMessage
		matched bool
	}{
		{"http-req", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpRequest{ProxyHttpRequest: &pb.ProxyHttpRequest{}}}, true},
		{"http-chunk", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: &pb.ProxyHttpBodyChunk{}}}, true},
		{"http-resp", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpResponse{ProxyHttpResponse: &pb.ProxyHttpResponse{}}}, true},
		{"open", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelOpen{TunnelOpen: &pb.TunnelOpen{}}}, true},
		{"data", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelData{TunnelData: &pb.TunnelData{}}}, true},
		{"close", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelClose{TunnelClose: &pb.TunnelClose{}}}, true},
		{"unrelated", &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Send{Send: &pb.SendMessage{}}}, false},
	}
	for _, tc := range cases {
		_, ok := proxyEnvelopeFromUpstream(tc.req)
		if ok != tc.matched {
			t.Errorf("%s: expected matched=%v, got %v", tc.name, tc.matched, ok)
		}
	}
}

// ---------------------------------------------------------------------------
// 11. Wildcard healthy filter — sanity check via SessionRegistry-scope test.
//
// The healthy-TTL filter is implemented in state.SessionRegistry; here we
// exercise the gateway's pass-through use of the filter via a stub that
// returns only the "healthy" instances. The mock session manager doesn't
// honour healthRemaining, so this test is a unit-level smoke check that
// the gateway forwards the impl name correctly.
// ---------------------------------------------------------------------------

func TestFindHealthyServiceInstances_ForwardsImpl(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	called := atomic.Int32{}
	stub := &recordingSessions{mockSessionManager: s.sessions.(*mockSessionManager), onScan: func(impl string) {
		called.Add(1)
		if impl != "memorylayer" {
			t.Errorf("expected impl=memorylayer, got %q", impl)
		}
	}}
	s.sessions = stub
	stub.serviceInstances = []string{"sv::memorylayer::x"}

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)
	req := &pb.ProxyHttpRequest{RequestId: "r", TargetTopic: "sv::memorylayer"}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	if called.Load() == 0 {
		t.Error("expected FindHealthyServiceInstances to be called")
	}
}

// recordingSessions wraps mockSessionManager with a per-call hook used to
// verify that the gateway forwards the impl filter correctly.
type recordingSessions struct {
	*mockSessionManager
	onScan func(impl string)
}

func (r *recordingSessions) FindHealthyServiceInstances(ctx context.Context, impl string, ttl time.Duration) ([]string, error) {
	if r.onScan != nil {
		r.onScan(impl)
	}
	return r.mockSessionManager.FindHealthyServiceInstances(ctx, impl, ttl)
}

// ---------------------------------------------------------------------------
// 12. Hop-depth tracking (T40)
//
// Each gateway hop along a proxy/tunnel chain increments proxy_chain_depth by
// 1 before forwarding; envelopes whose inbound depth has reached the
// configured cap are rejected with ACL_DENIED. These tests verify both the
// happy-path increment and the loop-break rejection on ProxyHttpRequest and
// TunnelOpen, which together cover all chain-eligible entry points.
// ---------------------------------------------------------------------------

func TestRouteProxyHttpRequest_HopDepth_BelowCap_IncrementsAndForwards(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxChainDepth = 8
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// Inbound depth 7, cap 8 — must succeed AND outbound depth must be 8.
	req := &pb.ProxyHttpRequest{
		RequestId:       "req-7hop",
		TargetTopic:     "sv::svc::pod",
		ProxyChainDepth: 7,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish for 7-hop chain, got %d", len(router.publishedMessages))
	}
	// The forwarded envelope is the same proto pointer; the depth field on
	// `req` was mutated in place by the gateway before publish.
	if got := req.GetProxyChainDepth(); got != 8 {
		t.Errorf("expected outbound proxy_chain_depth=8 after increment, got %d", got)
	}
}

func TestRouteProxyHttpRequest_HopDepth_AtCap_RejectsWithACLDenied(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxChainDepth = 8
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// Inbound depth 9 (one past cap 8) — must be rejected. Test the boundary
	// described in the brief: "9-hop chain rejected at hop 9".
	req := &pb.ProxyHttpRequest{
		RequestId:       "req-9hop",
		TargetTopic:     "sv::svc::pod",
		ProxyChainDepth: 9,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish when chain depth exceeds cap, got %d", pubs)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected ProxyHttpResponse error")
	}
	resp := stream.sent[0].GetProxyHttpResponse()
	if resp == nil || resp.Error == nil {
		t.Fatalf("expected error response, got %+v", stream.sent[0])
	}
	if resp.Error.Kind != pb.ProxyError_ACL_DENIED {
		t.Errorf("expected ACL_DENIED, got %v", resp.Error.Kind)
	}
	if !strings.Contains(resp.Error.Message, "proxy_chain_depth_exceeded") {
		t.Errorf("expected detail to mention proxy_chain_depth_exceeded, got %q", resp.Error.Message)
	}
}

func TestRouteProxyHttpRequest_HopDepth_ExactlyAtCap_Rejected(t *testing.T) {
	// The check is `>=`, so an inbound value equal to the cap is itself a
	// rejection — the cap is the highest legal *outbound* depth.
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxChainDepth = 8
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:       "req-8hop",
		TargetTopic:     "sv::svc::pod",
		ProxyChainDepth: 8,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish at depth==cap, got %d", pubs)
	}
}

func TestRouteProxyHttpRequest_HopDepth_DefaultCap8(t *testing.T) {
	// When maxChainDepth is unset (0), getMaxChainDepth() returns 8.
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// Depth 8 against the default cap of 8 must be rejected.
	req := &pb.ProxyHttpRequest{
		RequestId:       "req-default-cap",
		TargetTopic:     "sv::svc::pod",
		ProxyChainDepth: 8,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish at default cap, got %d", pubs)
	}
}

func TestRouteTunnelOpen_HopDepth_BelowCap_IncrementsAndForwards(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxChainDepth = 8
	s.sessions.(*mockSessionManager).serviceInstances = []string{"sv::tcp::pod-1"}
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	open := &pb.TunnelOpen{
		TunnelId:        "tun-7hop",
		TargetTopic:     "sv::tcp",
		ProxyChainDepth: 7,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelOpen: open})

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish for 7-hop tunnel, got %d", len(router.publishedMessages))
	}
	if got := open.GetProxyChainDepth(); got != 8 {
		t.Errorf("expected outbound proxy_chain_depth=8 after increment, got %d", got)
	}
}

func TestRouteTunnelOpen_HopDepth_AtCap_RejectsWithError(t *testing.T) {
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	s.quotaEnforcer.maxChainDepth = 8
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	// 9-hop chain rejected at hop 9: TunnelOpen must close with ACL_DENIED
	// detail without publishing.
	open := &pb.TunnelOpen{
		TunnelId:        "tun-9hop",
		TargetTopic:     "sv::tcp::pod",
		ProxyChainDepth: 9,
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{tunnelOpen: open})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish when tunnel chain depth exceeds cap, got %d", pubs)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 || stream.sent[0].GetTunnelClose() == nil {
		t.Fatalf("expected TunnelClose, got %+v", stream.sent)
	}
	closeMsg := stream.sent[0].GetTunnelClose()
	if closeMsg.Reason != pb.TunnelClose_ERROR {
		t.Errorf("expected ERROR reason, got %v", closeMsg.Reason)
	}
	if !strings.Contains(closeMsg.Detail, "proxy_chain_depth_exceeded") {
		t.Errorf("expected detail to mention proxy_chain_depth_exceeded, got %q", closeMsg.Detail)
	}
	// Counter must NOT have been incremented for the rejected open.
	if got := s.tunnelCounterFor("ws1").n.Load(); got != 0 {
		t.Errorf("expected tunnel counter unchanged on depth rejection, got %d", got)
	}
}

func TestRouteProxyHttpRequest_HopDepth_ZeroInbound_IncrementsToOne(t *testing.T) {
	// First-hop case (legacy callers / SDKs that don't set the field): depth
	// 0 must succeed and land at the next gateway as 1.
	router := newMockMessageRouter()
	s := newProxyTestServer(router)
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newProxyClient(sender, stream)

	req := &pb.ProxyHttpRequest{
		RequestId:   "req-0hop",
		TargetTopic: "sv::svc::pod",
		// ProxyChainDepth left at zero.
	}
	s.routeProxyEnvelope(context.Background(), client, proxyEnvelope{httpReq: req})

	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 1 {
		t.Fatalf("expected 1 publish for zero-depth request, got %d", pubs)
	}
	if got := req.GetProxyChainDepth(); got != 1 {
		t.Errorf("expected depth 0→1 increment, got %d", got)
	}
}
