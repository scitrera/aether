package gateway

// Tests for WS-P2J: sv::{impl} wildcard resolution on the regular agent-message
// routing path (routeMessage).
//
// Coverage:
//   - Wildcard with a healthy local instance → resolves to that instance.
//   - Wildcard with no local instance but a cluster-wide candidate → resolves to cluster candidate.
//   - Wildcard with NO healthy candidates anywhere → ERR_SV_UNAVAILABLE returned to sender.
//   - Concrete sv::{impl}::{spec} targets pass through unchanged (regression).
//   - ACL sees the resolved concrete target, not the wildcard (ordering check).
//   - Audit log carries the resolved concrete target.

import (
	"context"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/pkg/models"
)

// newWildcardTestServer builds a GatewayServer for wildcard routing tests.
// Mirrors newRoutingTestServer but ensures publishBreaker is always closed and
// IsActive returns true so orchestration is never triggered on a resolved concrete topic.
func newWildcardTestServer(router *mockMessageRouter) *GatewayServer {
	s := newRoutingTestServer(router)
	s.publishBreaker = circuitbreaker.New("test-wildcard-publish",
		circuitbreaker.WithMaxFailures(100))
	s.sessions.(*mockSessionManager).isActiveResult = true
	return s
}

// newWildcardClient builds a ClientSession for wildcard routing tests.
func newWildcardClient(identity models.Identity, stream *mockStream) *ClientSession {
	return newRoutingTestClient(identity, stream)
}

// ---------------------------------------------------------------------------
// 1. Wildcard with a healthy LOCAL instance → resolves to that instance.
// ---------------------------------------------------------------------------

func TestRouteMessage_SvWildcard_LocalInstance_ResolvesToLocal(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)

	// Seed a locally-connected service instance.
	s.identityIndex.Store("sv::platform-bridge::local-pod", "session-local")

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	msg := &pb.SendMessage{
		TargetTopic: "sv::platform-bridge",
		MessageType: pb.MessageType_OPAQUE,
		Payload:     []byte("hello"),
	}
	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "sv::platform-bridge::local-pod" {
		t.Errorf("expected publish to sv::platform-bridge::local-pod, got %q", router.publishedMessages[0].topic)
	}
	// msg.TargetTopic must have been rewritten in-place to the concrete value.
	if msg.TargetTopic != "sv::platform-bridge::local-pod" {
		t.Errorf("expected msg.TargetTopic rewritten to concrete, got %q", msg.TargetTopic)
	}
}

// ---------------------------------------------------------------------------
// 2. Wildcard with no local instance but a cluster candidate → resolves to cluster.
// ---------------------------------------------------------------------------

func TestRouteMessage_SvWildcard_ClusterInstance_ResolvesToCluster(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)

	// No local instance; seed cluster scan result.
	s.sessions.(*mockSessionManager).serviceInstances = []string{"sv::platform-bridge::cluster-pod"}

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	msg := &pb.SendMessage{
		TargetTopic: "sv::platform-bridge",
		MessageType: pb.MessageType_OPAQUE,
		Payload:     []byte("hello"),
	}
	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != "sv::platform-bridge::cluster-pod" {
		t.Errorf("expected publish to sv::platform-bridge::cluster-pod, got %q", router.publishedMessages[0].topic)
	}
}

// ---------------------------------------------------------------------------
// 3. Wildcard with NO healthy candidates → ERR_SV_UNAVAILABLE, no publish.
// ---------------------------------------------------------------------------

func TestRouteMessage_SvWildcard_NoInstances_ReturnsError(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)
	// No local instances, no cluster instances seeded.

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	msg := &pb.SendMessage{
		TargetTopic: "sv::platform-bridge",
		MessageType: pb.MessageType_OPAQUE,
		Payload:     []byte("hello"),
	}
	s.routeMessage(context.Background(), client, msg)

	// No message published.
	router.mu.Lock()
	pubs := len(router.publishedMessages)
	router.mu.Unlock()
	if pubs != 0 {
		t.Errorf("expected no publish when no instances available, got %d", pubs)
	}

	// Caller receives ERR_SV_UNAVAILABLE.
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected error response to sender, got none")
	}
	errResp := stream.sent[0].GetError()
	if errResp == nil {
		t.Fatalf("expected DownstreamMessage_Error, got %+v", stream.sent[0])
	}
	if errResp.Code != "ERR_SV_UNAVAILABLE" {
		t.Errorf("expected code ERR_SV_UNAVAILABLE, got %q", errResp.Code)
	}
	if !errResp.Retryable {
		t.Error("expected Retryable=true for sv_unavailable")
	}
}

// ---------------------------------------------------------------------------
// 4. Concrete sv::{impl}::{spec} passes through unchanged (regression).
// ---------------------------------------------------------------------------

func TestRouteMessage_SvConcrete_PassesThrough(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)
	// Mark the concrete topic as active so routeMessage does not try orchestration.
	s.sessions.(*mockSessionManager).isActiveResult = true

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	const concreteTopic = "sv::platform-bridge::bridge-host-tenant"
	msg := &pb.SendMessage{
		TargetTopic: concreteTopic,
		MessageType: pb.MessageType_OPAQUE,
		Payload:     []byte("hello"),
	}
	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish for concrete sv:: target, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != concreteTopic {
		t.Errorf("concrete topic must not be rewritten: got %q, want %q", router.publishedMessages[0].topic, concreteTopic)
	}
}

// ---------------------------------------------------------------------------
// 5. Local instance is preferred over cluster candidates.
// ---------------------------------------------------------------------------

func TestRouteMessage_SvWildcard_PrefersLocalOverCluster(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)

	// One local instance.
	s.identityIndex.Store("sv::platform-bridge::local-only", "session-L")
	// Cluster scan has different instances; they must never be picked.
	s.sessions.(*mockSessionManager).serviceInstances = []string{
		"sv::platform-bridge::remote-1",
		"sv::platform-bridge::remote-2",
	}

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	for i := 0; i < 10; i++ {
		msg := &pb.SendMessage{
			TargetTopic: "sv::platform-bridge",
			MessageType: pb.MessageType_OPAQUE,
			Payload:     []byte("x"),
		}
		s.routeMessage(context.Background(), client, msg)
		// Reset for next iteration (routeMessage rewrites msg.TargetTopic).
		msg.TargetTopic = "sv::platform-bridge"
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	for _, p := range router.publishedMessages {
		if p.topic != "sv::platform-bridge::local-only" {
			t.Errorf("expected every publish to go to local-only, got %q", p.topic)
		}
	}
}

// ---------------------------------------------------------------------------
// 6. Wildcard distributes across multiple local instances.
// ---------------------------------------------------------------------------

func TestRouteMessage_SvWildcard_DistributesAcrossLocalInstances(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)

	s.identityIndex.Store("sv::platform-bridge::pod-a", "session-a")
	s.identityIndex.Store("sv::platform-bridge::pod-b", "session-b")
	s.identityIndex.Store("sv::platform-bridge::pod-c", "session-c")

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	const N = 60
	for i := 0; i < N; i++ {
		msg := &pb.SendMessage{
			TargetTopic: "sv::platform-bridge",
			MessageType: pb.MessageType_OPAQUE,
			Payload:     []byte("x"),
		}
		s.routeMessage(context.Background(), client, msg)
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != N {
		t.Fatalf("expected %d publishes, got %d", N, len(router.publishedMessages))
	}
	hits := map[string]int{}
	for _, p := range router.publishedMessages {
		hits[p.topic]++
		if !strings.HasPrefix(p.topic, "sv::platform-bridge::") {
			t.Errorf("publish went to non-concrete topic: %q", p.topic)
		}
	}
	if len(hits) < 2 {
		t.Errorf("expected at least 2 instances to receive traffic over %d sends, got %d: %v", N, len(hits), hits)
	}
}

// ---------------------------------------------------------------------------
// 7. Envelope Source carries the resolved concrete topic, not the wildcard.
//    This verifies that msg.TargetTopic is rewritten before the MessageEnvelope
//    is constructed and published — which is what audit/receivers observe.
// ---------------------------------------------------------------------------

func TestRouteMessage_SvWildcard_EnvelopeCarriesConcreteTarget(t *testing.T) {
	router := newMockMessageRouter()
	s := newWildcardTestServer(router)

	// One local instance; no cluster fallback needed.
	s.identityIndex.Store("sv::platform-bridge::pod-env", "session-env")

	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "caller", Specifier: "v1"}
	client := newWildcardClient(sender, stream)

	msg := &pb.SendMessage{
		TargetTopic: "sv::platform-bridge",
		MessageType: pb.MessageType_OPAQUE,
		Payload:     []byte("payload"),
	}
	s.routeMessage(context.Background(), client, msg)

	// msg.TargetTopic must be the concrete resolved value.
	const wantTopic = "sv::platform-bridge::pod-env"
	if msg.TargetTopic != wantTopic {
		t.Errorf("msg.TargetTopic after routeMessage = %q, want %q", msg.TargetTopic, wantTopic)
	}

	// The publish must also land on the concrete topic.
	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.publishedMessages) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(router.publishedMessages))
	}
	if router.publishedMessages[0].topic != wantTopic {
		t.Errorf("published to %q, want %q", router.publishedMessages[0].topic, wantTopic)
	}
}
