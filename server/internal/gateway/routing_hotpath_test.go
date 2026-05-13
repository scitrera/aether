package gateway

// Tests for routeMessage() hot path: rate limiting, permission enforcement,
// successful routing, and error response on publish failure.
//
// All tests in this file use mock dependencies - no external services required.

import (
	"context"
	"errors"
	"sync"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/pkg/models"
	"golang.org/x/time/rate"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newRoutingTestServer builds a minimal GatewayServer suitable for
// routeMessage tests. ACL is nil so checkMessageSend is a no-op.
func newRoutingTestServer(router *mockMessageRouter) *GatewayServer {
	s := newTestGatewayWithMocks(
		newMockSessionManager(),
		router,
		newMockKVReadWriter(),
		newMockCheckpointManager(),
	)
	// Use a circuit breaker that is always closed (will pass calls through).
	s.publishBreaker = circuitbreaker.New("test-publish",
		circuitbreaker.WithMaxFailures(100),
	)
	// sessions.IsActive must return true to avoid triggering orchestration.
	s.sessions.(*mockSessionManager).isActiveResult = true
	return s
}

// newRoutingTestClient builds a ClientSession for routeMessage tests.
// The mockStream captures any downstream messages sent back to the client.
func newRoutingTestClient(identity models.Identity, stream *mockStream) *ClientSession {
	return &ClientSession{
		ID:            "route-test-session",
		Identity:      identity,
		Stream:        stream,
		subscriptions: make(map[string]func()),
		// rateLimiter left nil → no rate limiting by default
	}
}

// ---------------------------------------------------------------------------
// Rate limiting tests
// ---------------------------------------------------------------------------

func TestRouteMessage_RateLimited_SendsRateLimitedError(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{
		Type:      models.PrincipalAgent,
		Workspace: "ws1",
	}
	client := newRoutingTestClient(identity, stream)

	// Configure a rate limiter that has zero tokens - every Allow() call returns false.
	client.rateLimiter = rate.NewLimiter(rate.Limit(0), 0)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
	}

	s.routeMessage(context.Background(), client, msg)

	// Client should receive exactly one error response.
	if stream.sentCount() == 0 {
		t.Fatal("expected an error response to be sent to client, got none")
	}
	stream.mu.Lock()
	sent := stream.sent[0]
	stream.mu.Unlock()

	errResp := sent.GetError()
	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload, got nil")
	}
	if errResp.Code != "ERR_RATE_LIMITED" {
		t.Errorf("expected error code ERR_RATE_LIMITED, got %q", errResp.Code)
	}
	if !errResp.Retryable {
		t.Error("expected Retryable=true for rate limit error")
	}
}

func TestRouteMessage_RateLimited_MessageNotPublished(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)
	client.rateLimiter = rate.NewLimiter(rate.Limit(0), 0)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 0 {
		t.Errorf("expected no messages published when rate limited, got %d", published)
	}
}

func TestRouteMessage_NotRateLimited_MessageDelivered(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)
	// Very high rate - never blocks.
	client.rateLimiter = rate.NewLimiter(rate.Limit(1e6), 1000)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("hello"),
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 1 {
		t.Errorf("expected 1 published message, got %d", published)
	}
}

// ---------------------------------------------------------------------------
// Permission enforcement tests
// ---------------------------------------------------------------------------

func TestRouteMessage_UserSendsToEventTopic_PermissionDeniedError(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalUser, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "event::ws1",
		MessageType: pb.MessageType_EVENT,
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for permission violation")
	}
	stream.mu.Lock()
	sent := stream.sent[0]
	stream.mu.Unlock()

	errResp := sent.GetError()
	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload, got nil")
	}
	if errResp.Code != "ERR_PERMISSION_DENIED" {
		t.Errorf("expected ERR_PERMISSION_DENIED, got %q", errResp.Code)
	}
}

func TestRouteMessage_UserSendsToEventTopic_MessageNotPublished(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalUser, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "event::ws1",
		MessageType: pb.MessageType_EVENT,
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 0 {
		t.Errorf("expected 0 published messages after permission denial, got %d", published)
	}
}

func TestRouteMessage_MetricsBridgeSendsAnyTopic_PermissionDenied(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalMetricsBridge, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "metric::ws1",
		MessageType: pb.MessageType_METRIC,
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for MetricsBridge send attempt")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil || errResp.Code != "ERR_PERMISSION_DENIED" {
		t.Errorf("expected ERR_PERMISSION_DENIED for receive-only MetricsBridge, got %v", errResp)
	}
}

func TestRouteMessage_AgentSendsToOwnWorkspace_Succeeds(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "prod",
		Implementation: "worker",
		Specifier:      "v1",
	}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::prod::other::v2",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("ping"),
	}

	s.routeMessage(context.Background(), client, msg)

	// No error should be sent.
	stream.mu.Lock()
	var hasError bool
	for _, m := range stream.sent {
		if m.GetError() != nil {
			hasError = true
		}
	}
	stream.mu.Unlock()

	if hasError {
		t.Error("expected no error response for agent sending within own workspace")
	}

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 1 {
		t.Errorf("expected 1 published message, got %d", published)
	}
}

func TestRouteMessage_AgentSendsToCrossWorkspace_TransportAllows(t *testing.T) {
	// Cross-workspace transport-layer block was removed — see
	// routing.go::enforceTopicPermissions. With s.acl=nil in this
	// fixture (no ACL wired), the cross-workspace send is no longer
	// rejected here; in a real deployment the ACL layer is what
	// enforces the workspace gate and would deny when the sender
	// lacks grants on the target workspace. ACL-layer enforcement is
	// covered by the integration tests in routing_task_authority_test.go.
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws2::impl::spec",
		MessageType: pb.MessageType_CHAT,
	}

	s.routeMessage(context.Background(), client, msg)

	stream.mu.Lock()
	var hasError bool
	for _, m := range stream.sent {
		if m.GetError() != nil {
			hasError = true
		}
	}
	stream.mu.Unlock()
	if hasError {
		t.Error("expected no transport-level error for cross-workspace send (ACL gates downstream)")
	}

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()
	if published != 1 {
		t.Errorf("expected 1 published message (ACL not wired in this fixture, so the send goes through), got %d", published)
	}
}

// ---------------------------------------------------------------------------
// Invalid topic format
// ---------------------------------------------------------------------------

func TestRouteMessage_InvalidTopicFormat_SendsInvalidTopicError(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "not-a-valid-topic",
		MessageType: pb.MessageType_CHAT,
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for invalid topic")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil || errResp.Code != "ERR_INVALID_TOPIC" {
		t.Errorf("expected ERR_INVALID_TOPIC, got %v", errResp)
	}
}

func TestRouteMessage_EmptyTopic_SendsInvalidTopicError(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "",
		MessageType: pb.MessageType_CHAT,
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for empty topic")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil || errResp.Code != "ERR_INVALID_TOPIC" {
		t.Errorf("expected ERR_INVALID_TOPIC for empty topic, got %v", errResp)
	}
}

// ---------------------------------------------------------------------------
// Publish failure - error response sent to client (recently added behaviour)
// ---------------------------------------------------------------------------

// errorPublishRouter is a MessageRouter whose Publish always returns an error.
type errorPublishRouter struct {
	mu                     sync.Mutex
	subscribedTopics       []string
	exclusiveSubscriptions map[string]string
	err                    error
}

func (r *errorPublishRouter) Publish(_ context.Context, _ string, _ []byte) error {
	return r.err
}

func (r *errorPublishRouter) Subscribe(topic string, _ func([]byte)) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscribedTopics = append(r.subscribedTopics, topic)
	return func() {}, nil
}

func (r *errorPublishRouter) SubscribeExclusive(topic string, consumer string, _ func([]byte)) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.exclusiveSubscriptions == nil {
		r.exclusiveSubscriptions = make(map[string]string)
	}
	r.exclusiveSubscriptions[topic] = consumer
	return func() {}, nil
}

func (r *errorPublishRouter) SubscribeExclusiveFromNow(topic string, consumer string, h func([]byte)) (func(), error) {
	return r.SubscribeExclusive(topic, consumer, h)
}

func (r *errorPublishRouter) SubscribeExclusiveFromTimestamp(topic string, consumer string, _ int64, h func([]byte)) (func(), error) {
	return r.SubscribeExclusive(topic, consumer, h)
}

func TestRouteMessage_PublishFails_SendsPublishFailedErrorToClient(t *testing.T) {
	errRouter := &errorPublishRouter{
		err: errors.New("broker unavailable"),
	}

	sessions := newMockSessionManager()
	sessions.isActiveResult = true

	s := &GatewayServer{
		sessions:      sessions,
		router:        errRouter,
		kv:            newMockKVReadWriter(),
		checkpoints:   newMockCheckpointManager(),
		gatewayID:     "test-gateway",
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
		publishBreaker: circuitbreaker.New("test-pub",
			circuitbreaker.WithMaxFailures(100),
		),
	}

	stream := &mockStream{}
	identity := models.Identity{
		Type:      models.PrincipalAgent,
		Workspace: "ws1",
	}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("data"),
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response sent to client after publish failure, got none")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload after publish failure, got nil")
	}
	if errResp.Code != "ERR_PUBLISH_FAILED" {
		t.Errorf("expected ERR_PUBLISH_FAILED, got %q", errResp.Code)
	}
	if !errResp.Retryable {
		t.Error("expected Retryable=true for publish failure error")
	}
}

func TestRouteMessage_PublishSucceeds_NoErrorSentToClient(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("hello world"),
	}

	s.routeMessage(context.Background(), client, msg)

	stream.mu.Lock()
	var hasError bool
	for _, m := range stream.sent {
		if m.GetError() != nil {
			hasError = true
		}
	}
	stream.mu.Unlock()

	if hasError {
		t.Error("expected no error response when publish succeeds")
	}
}

func TestRouteMessage_PublishSucceeds_TopicAndPayloadForwarded(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	stream := &mockStream{}

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("test-payload"),
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	msgs := router.publishedMessages
	router.mu.Unlock()

	if len(msgs) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(msgs))
	}
	if msgs[0].topic != "ag::ws1::impl::spec" {
		t.Errorf("expected topic ag.ws1.impl.spec, got %q", msgs[0].topic)
	}
	if len(msgs[0].payload) == 0 {
		t.Error("expected non-empty marshalled payload")
	}
}
