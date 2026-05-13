package gateway

// Tests for routeMessage() covering:
//   - Payload exceeds max size → ERR_PAYLOAD_TOO_LARGE sent, message not published
//   - Payload exactly at max size → allowed through
//   - Circuit breaker open → ERR_CIRCUIT_OPEN sent to client
//   - QuotaEnforcer.getMaxMessagePayloadSize default and custom values

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Payload size limit
// ---------------------------------------------------------------------------

func TestRouteMessage_PayloadTooLarge_SendsPayloadTooLargeError(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	// Lower the limit to 10 bytes so the test doesn't allocate 1MB.
	s.quotaEnforcer.maxMessagePayloadSize = 10

	stream := &mockStream{}
	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     bytes.Repeat([]byte("x"), 11), // 11 bytes > 10 byte limit
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for oversized payload")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload")
	}
	if errResp.Code != "ERR_PAYLOAD_TOO_LARGE" {
		t.Errorf("expected ERR_PAYLOAD_TOO_LARGE, got %q", errResp.Code)
	}
}

func TestRouteMessage_PayloadTooLarge_MessageNotPublished(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	s.quotaEnforcer.maxMessagePayloadSize = 10

	stream := &mockStream{}
	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     bytes.Repeat([]byte("x"), 100),
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 0 {
		t.Errorf("expected 0 published messages for oversized payload, got %d", published)
	}
}

func TestRouteMessage_PayloadExactlyAtLimit_MessagePublished(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	// Set limit to exactly 20 bytes.
	s.quotaEnforcer.maxMessagePayloadSize = 20

	stream := &mockStream{}
	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     bytes.Repeat([]byte("y"), 20), // exactly at limit
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 1 {
		t.Errorf("expected 1 published message at exact payload limit, got %d", published)
	}

	// No error should have been sent to the client.
	stream.mu.Lock()
	var hasErr bool
	for _, m := range stream.sent {
		if m.GetError() != nil {
			hasErr = true
		}
	}
	stream.mu.Unlock()
	if hasErr {
		t.Error("expected no error response when payload is exactly at limit")
	}
}

func TestRouteMessage_ZeroPayload_AllowedThrough(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)

	stream := &mockStream{}
	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     nil, // zero-length
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	if published != 1 {
		t.Errorf("expected 1 published message for zero-length payload, got %d", published)
	}
}

// ---------------------------------------------------------------------------
// Circuit breaker open path
// ---------------------------------------------------------------------------

// openCircuitRouter is a MessageRouter whose Publish always returns an error,
// allowing us to trip the circuit breaker open.
type openCircuitRouter struct {
	errorPublishRouter
}

func newOpenCircuitBreaker() *circuitbreaker.CircuitBreaker {
	// maxFailures=1 means the circuit opens after a single failure.
	cb := circuitbreaker.New("test-open-cb", circuitbreaker.WithMaxFailures(1))
	// Trip it open by executing one failing call.
	_ = cb.Execute(func() error {
		return circuitbreaker.ErrCircuitOpen // any non-nil error trips it
	})
	return cb
}

func TestRouteMessage_CircuitBreakerOpen_SendsCircuitOpenError(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	// Replace the circuit breaker with one that is already open.
	s.publishBreaker = newOpenCircuitBreaker()

	stream := &mockStream{}
	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("data"),
	}

	s.routeMessage(context.Background(), client, msg)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response when circuit breaker is open")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload for open circuit breaker")
	}
	if errResp.Code != "ERR_CIRCUIT_OPEN" {
		t.Errorf("expected ERR_CIRCUIT_OPEN, got %q", errResp.Code)
	}
	if !errResp.Retryable {
		t.Error("expected Retryable=true for circuit open error")
	}
}

func TestRouteMessage_CircuitBreakerOpen_MessageNotPublishedToRouter(t *testing.T) {
	router := newMockMessageRouter()
	s := newRoutingTestServer(router)
	s.publishBreaker = newOpenCircuitBreaker()

	stream := &mockStream{}
	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	client := newRoutingTestClient(identity, stream)

	msg := &pb.SendMessage{
		TargetTopic: "ag::ws1::impl::spec",
		MessageType: pb.MessageType_CHAT,
		Payload:     []byte("data"),
	}

	s.routeMessage(context.Background(), client, msg)

	router.mu.Lock()
	published := len(router.publishedMessages)
	router.mu.Unlock()

	// The circuit breaker intercepts the call before reaching the router.
	if published != 0 {
		t.Errorf("expected 0 published messages when circuit breaker is open, got %d", published)
	}
}

// ---------------------------------------------------------------------------
// QuotaEnforcer payload size helpers
// ---------------------------------------------------------------------------

func TestQuotaEnforcer_GetMaxMessagePayloadSize_DefaultIs1MB(t *testing.T) {
	qe := newQuotaEnforcer(100, 200)
	expected := 1024 * 1024
	if got := qe.getMaxMessagePayloadSize(); got != expected {
		t.Errorf("expected default maxMessagePayloadSize=%d, got %d", expected, got)
	}
}

func TestQuotaEnforcer_GetMaxMessagePayloadSize_CustomValueOverridesDefault(t *testing.T) {
	qe := newQuotaEnforcer(100, 200)
	qe.maxMessagePayloadSize = 512
	if got := qe.getMaxMessagePayloadSize(); got != 512 {
		t.Errorf("expected maxMessagePayloadSize=512, got %d", got)
	}
}

func TestQuotaEnforcer_GetMaxTaskPayloadSize_DefaultIs512KB(t *testing.T) {
	qe := newQuotaEnforcer(100, 200)
	expected := 512 * 1024
	if got := qe.getMaxTaskPayloadSize(); got != expected {
		t.Errorf("expected default maxTaskPayloadSize=%d, got %d", expected, got)
	}
}

func TestQuotaEnforcer_GetMaxTaskPayloadSize_CustomValueOverridesDefault(t *testing.T) {
	qe := newQuotaEnforcer(100, 200)
	qe.maxTaskPayloadSize = 256
	if got := qe.getMaxTaskPayloadSize(); got != 256 {
		t.Errorf("expected maxTaskPayloadSize=256, got %d", got)
	}
}
