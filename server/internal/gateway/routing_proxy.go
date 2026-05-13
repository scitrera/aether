package gateway

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
	"google.golang.org/protobuf/proto"
)

// proxy/tunnel routing primitives.
//
// Plain SendMessage routes through routeMessage; proxy and tunnel envelopes
// route through routeProxyEnvelope (and its tunnel-pin-aware companions). The
// two paths share the same ACL/authority/audit primitives but differ in:
//
//   - Wildcard resolution: ProxyHttpRequest and TunnelOpen accept the bare
//     `sv::{impl}` form; that wildcard is resolved to a single concrete
//     `sv::{impl}::{specifier}` instance before delivery and audit, so
//     downstream observers see an unambiguous address.
//   - Tunnel stickiness: TunnelOpen pins {tunnel_id → concrete topic} in
//     Redis. Subsequent TunnelData/Close lookups read the pin instead of
//     re-resolving the wildcard. Pins refresh on each data/ack frame.
//   - Permission gate: the ACL + proxy_path / tunnel_target resource scopes
//     are the sole gate. Any principal type may be a proxy/tunnel target.

// healthyLockMin is the lock-TTL floor below which an instance is excluded
// from the wildcard candidate set: a holder under the floor is unlikely to
// refresh its lock before expiry, so handing them tunnel work risks an
// immediate orphan.
const healthyLockMin = 5 * time.Second

// defaultTunnelPinTTL is the floor for tunnel-pin TTLs when TunnelOpen does
// not set idle_timeout_ms.
const defaultTunnelPinTTL = 5 * time.Minute

// activeTunnelCounter holds the gateway-wide live-tunnel counter for a
// single workspace. Incremented on TunnelOpen success, decremented on
// TunnelClose / pin loss. A bounded counter is sufficient as a cheap
// admission gate; this is not a persistent quota across gateway restarts.
type activeTunnelCounter struct{ n atomic.Int64 }

func (s *GatewayServer) tunnelCounterFor(workspace string) *activeTunnelCounter {
	if v, ok := s.activeTunnels.Load(workspace); ok {
		return v.(*activeTunnelCounter)
	}
	c := &activeTunnelCounter{}
	actual, _ := s.activeTunnels.LoadOrStore(workspace, c)
	return actual.(*activeTunnelCounter)
}

// proxyEnvelope is the discriminated union we route. Exactly one of the
// fields is non-nil, mirroring the upstream oneof.
type proxyEnvelope struct {
	httpReq       *pb.ProxyHttpRequest
	httpBodyChunk *pb.ProxyHttpBodyChunk
	httpResp      *pb.ProxyHttpResponse
	tunnelOpen    *pb.TunnelOpen
	tunnelData    *pb.TunnelData
	tunnelAck     *pb.TunnelAck
	tunnelClose   *pb.TunnelClose
}

// routeProxyEnvelope is the entry point for upstream proxy/tunnel frames.
// It dispatches by oneof variant and shares ACL/audit/quota primitives with
// routeMessage. Nothing here mutates routeMessage's hot path.
func (s *GatewayServer) routeProxyEnvelope(ctx context.Context, client *ClientSession, env proxyEnvelope) {
	client.identityMu.RLock()
	sender := client.Identity
	client.identityMu.RUnlock()

	switch {
	case env.httpReq != nil:
		s.routeProxyHttpRequest(ctx, client, sender, env.httpReq)
	case env.httpBodyChunk != nil:
		s.routeProxyHttpBodyChunk(ctx, client, sender, env.httpBodyChunk)
	case env.httpResp != nil:
		s.routeProxyHttpResponse(ctx, client, sender, env.httpResp)
	case env.tunnelOpen != nil:
		s.routeTunnelOpen(ctx, client, sender, env.tunnelOpen)
	case env.tunnelData != nil:
		s.routeTunnelData(ctx, client, sender, env.tunnelData)
	case env.tunnelAck != nil:
		s.routeTunnelAck(ctx, client, sender, env.tunnelAck)
	case env.tunnelClose != nil:
		s.routeTunnelClose(ctx, client, sender, env.tunnelClose)
	default:
		logging.Logger.Warn().Str("identity", sender.String()).Msg("routeProxyEnvelope: no payload set")
	}
}

// resolveProxyTarget resolves a proxy/tunnel target to a concrete topic.
// It accepts two forms:
//
//   - sv::{impl} (bare wildcard) — resolved to one healthy connected service
//     instance via local index then cluster-wide Redis scan.
//   - Any other concrete topic (sv::{impl}::{spec}, ag::*, tu::*, etc.) —
//     returned as-is; no wildcard expansion is performed.
//
// Any principal type is a valid target; the ACL gate (proxyACLCheck) is the
// sole permission control. Returns the concrete topic string and an error if
// wildcard resolution finds no healthy candidate.
func (s *GatewayServer) resolveProxyTarget(ctx context.Context, target string) (concreteTopic string, err error) {
	// Attempt sv:: wildcard parse first. If it parses as a bare sv::{impl}
	// wildcard, resolve to a concrete instance. If it parses as a concrete
	// sv::{impl}::{spec}, return it directly. If ParseSendTarget rejects the
	// input (not an sv:: topic), treat it as an already-concrete topic string.
	impl, specifier, isWildcard, parseErr := models.ParseSendTarget(target)
	if parseErr == nil {
		if !isWildcard {
			return models.ServiceTopic(impl, specifier)
		}
		// Wildcard: prefer a locally-connected sv::{impl}::* instance.
		if local := s.findLocalServiceInstances(impl); len(local) > 0 {
			pick := local[rand.IntN(len(local))]
			return pick, nil
		}
		// Fallback: cluster-wide via the session registry's lock keyspace.
		candidates, scanErr := s.sessions.FindHealthyServiceInstances(ctx, impl, healthyLockMin)
		if scanErr != nil {
			return "", fmt.Errorf("service instance discovery failed: %w", scanErr)
		}
		if len(candidates) == 0 {
			return "", fmt.Errorf("no healthy sv::%s instances available", impl)
		}
		pick := candidates[rand.IntN(len(candidates))]
		return pick, nil
	}

	// Non-sv:: target: use as-is (concrete agent, task, etc.).
	return target, nil
}

// findLocalServiceInstances scans identityIndex for connected sv::{impl}::*
// keys and returns their identity strings. Cheap O(n) scan; n bounded by the
// number of locally-connected services for this gateway, typically 1–10.
func (s *GatewayServer) findLocalServiceInstances(impl string) []string {
	prefix := "sv" + models.IdentitySep + impl + models.IdentitySep
	var out []string
	s.identityIndex.Range(func(key, _ any) bool {
		ks, ok := key.(string)
		if !ok {
			return true
		}
		if strings.HasPrefix(ks, prefix) {
			out = append(out, ks)
		}
		return true
	})
	return out
}

// proxyACLCheck reuses checkMessageSendWithAuthority / checkMessageSend,
// matching the same actor-direct-then-OBO order plain messages use.
func (s *GatewayServer) proxyACLCheck(ctx context.Context, client *ClientSession, sender models.Identity, target string, authz *pb.AuthorizationContext) (*acl.ResolvedAuthority, error) {
	resolved, err := s.resolveAuthorizationContext(ctx, client, sender, authz)
	if err != nil {
		return nil, err
	}
	if resolved != nil {
		return resolved, s.checkMessageSendWithAuthority(ctx, sender, target, client.SessionUUID, resolved)
	}
	return nil, s.checkMessageSend(ctx, sender, target)
}

// proxyFrameSourceMarker is the “MessageEnvelope.Source“ value used to
// flag a wire envelope as a pre-marshalled DownstreamMessage carrying a
// proxy/tunnel control-plane frame (ProxyHttpRequest, ProxyHttpResponse,
// TunnelOpen/Close, ProxyError, …).
//
// The per-session subscription handler (subscription.go::createMessageHandler)
// can never reliably distinguish a marshalled MessageEnvelope from a
// marshalled DownstreamMessage by bytes alone — both are valid proto3 and
// silently parse against either schema. Without an explicit marker, every
// proxy frame published through publishProxyEnvelope was being unmarshalled
// as a MessageEnvelope, re-wrapped in a generic IncomingMessage, and
// dispatched to the SDK's OnMessage handler instead of the typed handler
// (OnProxyHttpRequest etc.). The terminator's request handler never fired,
// the cowork-side caller's response future never resolved, and the call
// hung until its (often swallowed) timeout fired.
//
// Wrapping the inner DownstreamMessage in a MessageEnvelope and using this
// sentinel Source lets the consumer look once at Source, unwrap on match,
// and Deliver the inner DownstreamMessage as-is so the SDK's typed dispatch
// fires correctly. The sentinel is not a valid Aether identity topic
// (no canonical principal type uses an underscore-prefixed identifier) so
// it cannot collide with a legitimate SendMessage source.
const proxyFrameSourceMarker = "_aether/proxy-frame"

// publishProxyEnvelope marshals a DownstreamMessage and publishes it to the
// concrete service topic via the standard router publish breaker.
func (s *GatewayServer) publishProxyEnvelope(ctx context.Context, target string, msg *pb.DownstreamMessage) error {
	inner, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal proxy envelope: %w", err)
	}
	// Wrap the DownstreamMessage in a MessageEnvelope so the per-session
	// subscription handler (subscription.go::createMessageHandler) can
	// distinguish it from a regular SendMessage envelope and unwrap+deliver
	// the inner DownstreamMessage directly. See proxyFrameSourceMarker
	// docstring for the full rationale.
	envelope := &pb.MessageEnvelope{
		Source:      proxyFrameSourceMarker,
		Payload:     inner,
		MessageType: pb.MessageType_OPAQUE,
		TimestampMs: time.Now().UnixMilli(),
	}
	bytes, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal proxy frame wrapper: %w", err)
	}
	return s.publishBreaker.Execute(func() error {
		return s.router.Publish(ctx, target, bytes)
	})
}

// deliverDataPlaneLocal attempts the single-node bypass for data-plane
// envelopes (TunnelData, TunnelAck, ProxyHttpBodyChunk). When the target
// identity is connected to this gateway instance and the proxy local bypass
// is enabled, the message is enqueued directly on the target's delivery
// channel and the function returns true. Returns false on:
//   - bypass disabled by config / env override (caller falls back to RMQ),
//   - target not locally connected (caller falls back to RMQ),
//   - target session not addressable from activeStreams (caller falls back
//     to RMQ),
//   - target's delivery buffer full (caller falls back to RMQ to avoid
//     stalling routing — same backpressure-tolerant behavior as the RMQ
//     fan-out path).
//
// Control-plane envelopes (TunnelOpen, TunnelClose, ProxyHttpRequest header,
// ProxyHttpResponse header, ProxyError) MUST NOT use this helper: they are
// the only path that emits audit events for routed proxy traffic, so they
// always take the RMQ route. Callers ensure that invariant.
func (s *GatewayServer) deliverDataPlaneLocal(targetIdentity, envelopeType string, downstream *pb.DownstreamMessage) bool {
	if !s.proxyLocalBypassEnabled {
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "disabled").Inc()
		return false
	}
	sessionAny, ok := s.identityIndex.Load(targetIdentity)
	if !ok {
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "rmq_fallback").Inc()
		return false
	}
	sessionID, ok := sessionAny.(string)
	if !ok {
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "rmq_fallback").Inc()
		return false
	}
	clientAny, ok := s.activeStreams.Load(sessionID)
	if !ok {
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "rmq_fallback").Inc()
		return false
	}
	client, ok := clientAny.(*ClientSession)
	if !ok || client.deliveryCh == nil {
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "rmq_fallback").Inc()
		return false
	}
	// Non-blocking enqueue. On a full buffer we deliberately fall through to
	// the RMQ path rather than emitting a BACKPRESSURE error: the RMQ fan-out
	// is already prepared to absorb a slow reader without taking the caller
	// out of routing. This mirrors the trade-off documented in T31.
	select {
	case client.deliveryCh <- downstream:
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "hit").Inc()
		return true
	default:
		proxyLocalBypassTotal.WithLabelValues(envelopeType, "full_buffer").Inc()
		return false
	}
}

// sendProxyHttpError delivers a ProxyHttpResponse with a populated error to
// the originating caller. Used both for ACL/quota denials and for routing
// failures (no available sidecar etc.). request_id is preserved so the
// caller can correlate.
func sendProxyHttpError(client *ClientSession, requestID string, kind pb.ProxyError_Kind, message string) {
	resp := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpResponse{
			ProxyHttpResponse: &pb.ProxyHttpResponse{
				RequestId: requestID,
				Error: &pb.ProxyError{
					Kind:    kind,
					Message: message,
				},
			},
		},
	}
	if err := client.SafeSend(resp); err != nil {
		logging.Logger.Warn().Err(err).Str("request_id", requestID).Msg("failed to send ProxyHttpResponse error")
	}
}

// sendTunnelClose delivers a TunnelClose downstream to the caller. Used when
// pin loss or quota denial means we can't route a follow-on frame.
func sendTunnelClose(client *ClientSession, tunnelID string, reason pb.TunnelClose_Reason, detail string) {
	msg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelClose{
			TunnelClose: &pb.TunnelClose{
				TunnelId: tunnelID,
				Reason:   reason,
				Detail:   detail,
			},
		},
	}
	if err := client.SafeSend(msg); err != nil {
		logging.Logger.Warn().Err(err).Str("tunnel_id", tunnelID).Msg("failed to send TunnelClose")
	}
}

// =============================================================================
// ProxyHttpRequest
// =============================================================================

func (s *GatewayServer) routeProxyHttpRequest(ctx context.Context, client *ClientSession, sender models.Identity, req *pb.ProxyHttpRequest) {
	requestID := req.GetRequestId()
	target := req.GetTargetTopic()

	// 0. Body size cap.
	maxBody := s.quotaEnforcer.getMaxRequestBodyBytes()
	if len(req.Body) > maxBody {
		s.auditProxyHttpFailure(ctx, sender, target, requestID, client.SessionUUID, nil,
			fmt.Sprintf("body size %d exceeds max %d", len(req.Body), maxBody))
		sendProxyHttpError(client, requestID, pb.ProxyError_PAYLOAD_TOO_LARGE,
			fmt.Sprintf("request body size %d exceeds maximum of %d bytes", len(req.Body), maxBody))
		return
	}

	// 0b. Hop-depth check (T40). Reject when the inbound depth has already
	// reached the cap; otherwise increment so the downstream sidecar sees one
	// more hop along the chain. This breaks loops like agent → sandbox-A →
	// agent → sandbox-A → ... where every iteration would otherwise pass ACL.
	maxDepth := s.quotaEnforcer.getMaxChainDepth()
	if req.GetProxyChainDepth() >= maxDepth {
		detail := fmt.Sprintf("proxy_chain_depth_exceeded: inbound=%d cap=%d", req.GetProxyChainDepth(), maxDepth)
		s.auditProxyHttpFailure(ctx, sender, target, requestID, client.SessionUUID, nil, detail)
		sendProxyHttpError(client, requestID, pb.ProxyError_ACL_DENIED, detail)
		return
	}
	req.ProxyChainDepth = req.GetProxyChainDepth() + 1

	// 1. Resolve wildcard → concrete; rewrite envelope target so audit/receiver
	//    see an unambiguous address. sv::{impl} wildcards are expanded to a
	//    single healthy instance; all other target forms are used as-is.
	concrete, err := s.resolveProxyTarget(ctx, target)
	if err != nil {
		s.auditProxyHttpFailure(ctx, sender, target, requestID, client.SessionUUID, nil, err.Error())
		sendProxyHttpError(client, requestID, pb.ProxyError_SIDECAR_UNAVAILABLE, err.Error())
		return
	}
	req.TargetTopic = concrete

	// 2. ACL check — same primitives as plain SendMessage.
	resolvedAuthority, err := s.proxyACLCheck(ctx, client, sender, concrete, req.GetAuthorization())
	if err != nil {
		s.auditProxyHttpFailure(ctx, sender, concrete, requestID, client.SessionUUID, resolvedAuthority, err.Error())
		sendProxyHttpError(client, requestID, pb.ProxyError_ACL_DENIED, err.Error())
		return
	}

	// 2.5. Stamp the validated grant onto the AuthorizationContext so the
	//      terminator (audience-side) can mint extended X-Auth-* headers
	//      without issuing its own ResolveAuthorityRequest. The gateway has
	//      already validated against the calling delegate (cowork agent
	//      etc.); the terminator (e.g. MemoryLayer) is the audience and would
	//      otherwise fail acl.ResolveAuthority's actor==delegate check when
	//      it tried to re-resolve. By inlining the resolution here we
	//      eliminate that mismatch and a redundant RPC round-trip per
	//      request. See AuthorizationContext.resolved doc in api/proto/aether.proto.
	if resolvedAuthority != nil && req.Authorization != nil && resolvedAuthority.Grant != nil {
		req.Authorization.Resolved = grantToResolvedAuthorityInfo(resolvedAuthority.Grant)
	}

	// 3. Stamp the originating caller into the envelope so the receiving
	//    sidecar can populate X-Auth-* headers without a separate lookup.
	//    (T3 introduces the helper; here we just propagate the actor topic.)
	if req.Headers == nil {
		req.Headers = make(map[string]string)
	}
	req.Headers["x-aether-actor-topic"] = sender.ToTopic()

	// 4. Install request-pin so follow-on body chunks and the response can
	//    route to the correct counterparty without re-resolving wildcards.
	//    Pin lifetime ≥ timeout_ms + slack so chunks arriving late still find
	//    the binding; refreshed on each chunk that touches the gateway.
	pinTTL := requestPinTTL(req.GetTimeoutMs())
	pinValue := encodeRequestPin(sender.ToTopic(), concrete)
	if pinErr := s.sessions.SetRequestPin(ctx, requestID, pinValue, pinTTL); pinErr != nil {
		// Non-fatal: a missing pin only breaks chunked uploads/responses, not
		// inline bodies. Log and continue so the inline path stays available.
		logging.Logger.Warn().Err(pinErr).Str("request_id", requestID).Msg("routeProxyHttpRequest: SetRequestPin failed (non-fatal for inline bodies)")
	}

	// 5. Publish to the concrete target topic.
	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpRequest{ProxyHttpRequest: req},
	}
	if err := s.publishProxyEnvelope(ctx, concrete, downstream); err != nil {
		_ = s.sessions.DeleteRequestPin(ctx, requestID)
		s.auditProxyHttpFailure(ctx, sender, concrete, requestID, client.SessionUUID, resolvedAuthority, err.Error())
		sendProxyHttpError(client, requestID, pb.ProxyError_SIDECAR_UNAVAILABLE,
			fmt.Sprintf("failed to deliver request: %v", err))
		return
	}

	// Inline (non-chunked) requests with no chunked response are
	// single-shot; the pin is only needed if the service emits chunked
	// response frames. We keep it for the pin's TTL — well under a minute —
	// to support both directions; cleanup happens on fin or pin expiry.
	s.auditProxyHttpSuccess(ctx, sender, concrete, requestID, client.SessionUUID, resolvedAuthority, len(req.Body))
}

// defaultRequestPinTTL is the floor for request-pin TTLs when ProxyHttpRequest
// does not set timeout_ms. Sized to comfortably cover most HTTP request
// lifecycles while still expiring stale pins.
const defaultRequestPinTTL = 60 * time.Second

// requestPinTTL converts a ProxyHttpRequest.timeout_ms into a Redis TTL,
// adding a 30s slack so chunks arriving near the deadline still find the
// binding.
func requestPinTTL(timeoutMs int64) time.Duration {
	if timeoutMs <= 0 {
		return defaultRequestPinTTL
	}
	return time.Duration(timeoutMs)*time.Millisecond + 30*time.Second
}

// requestPinSep separates caller and service portions of a request-pin value.
// Mirrors tunnelPinSep so the pin shape is consistent across primitives.
const requestPinSep = "|"

// encodeRequestPin returns the pin value used for SetRequestPin: caller and
// service identities joined by requestPinSep.
func encodeRequestPin(caller, service string) string {
	if caller == "" {
		return service
	}
	return caller + requestPinSep + service
}

// decodeRequestPin splits a pin value into caller and service. Legacy values
// without a separator are treated as service-only.
func decodeRequestPin(value string) (caller, service string) {
	if idx := strings.Index(value, requestPinSep); idx >= 0 {
		return value[:idx], value[idx+1:]
	}
	return "", value
}

// routeProxyHttpBodyChunk forwards body-chunk continuations using the
// request-pin established by routeProxyHttpRequest. Direction is determined
// by chunk.is_request: true = caller→service, false = service→caller. The
// parent request already passed the ACL gate, so chunks ride that decision.
//
// On pin loss (TTL expired or never installed) we emit a ProxyError reply
// to the most-likely waiting peer. Pins are refreshed on each chunk so an
// in-flight upload doesn't time out mid-stream; on fin we clear the pin in
// the request direction (the response side will refresh it again if needed).
func (s *GatewayServer) routeProxyHttpBodyChunk(ctx context.Context, client *ClientSession, sender models.Identity, chunk *pb.ProxyHttpBodyChunk) {
	requestID := chunk.GetRequestId()
	if requestID == "" {
		return
	}

	pinValue, err := s.sessions.GetRequestPin(ctx, requestID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("request_id", requestID).Msg("ProxyHttpBodyChunk pin lookup failed")
		if chunk.GetIsRequest() {
			sendProxyHttpError(client, requestID, pb.ProxyError_SIDECAR_UNAVAILABLE, "request pin lookup failed")
		}
		return
	}
	if pinValue == "" {
		// Pin expired or absent: caller-direction chunks have nowhere to go;
		// reply with a transport error so the caller can fail fast.
		if chunk.GetIsRequest() {
			sendProxyHttpError(client, requestID, pb.ProxyError_SIDECAR_UNAVAILABLE, "request pin expired or unknown")
		}
		// Service-direction chunks with no pin are dropped silently — the
		// caller has either disconnected or already received a fin.
		return
	}
	caller, service := decodeRequestPin(pinValue)

	var dest string
	if chunk.GetIsRequest() {
		// caller → service. Sender must be the caller (pin's caller side).
		// Defence-in-depth: if sender doesn't match the pinned caller we
		// still deliver — the pin is the source of truth.
		dest = service
	} else {
		dest = caller
	}
	if dest == "" {
		return
	}

	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: chunk},
	}
	// Single-node fast path: body chunks are bytes-only payload that follow a
	// ProxyHttpRequest header (already audited via the RMQ path). Audit fires
	// on the request/response headers, not per chunk, so the bypass is safe.
	if s.deliverDataPlaneLocal(dest, "proxy_http_body_chunk", downstream) {
		// Mirror the RMQ-success post-actions: refresh / fin handling below.
	} else if pubErr := s.publishProxyEnvelope(ctx, dest, downstream); pubErr != nil {
		logging.Logger.Warn().Err(pubErr).Str("request_id", requestID).Str("target", dest).Msg("ProxyHttpBodyChunk forward failed")
		if chunk.GetIsRequest() {
			sendProxyHttpError(client, requestID, pb.ProxyError_SIDECAR_UNAVAILABLE, fmt.Sprintf("failed to deliver chunk: %v", pubErr))
			_ = s.sessions.DeleteRequestPin(ctx, requestID)
		}
		return
	}

	// Refresh TTL so a still-streaming request doesn't lose its pin.
	if rErr := s.sessions.RefreshRequestPin(ctx, requestID, requestPinTTL(0)); rErr != nil {
		logging.Logger.Debug().Err(rErr).Str("request_id", requestID).Msg("ProxyHttpBodyChunk pin refresh failed (non-fatal)")
	}

	// On final response chunk, clear the pin: no more frames will use it.
	// Final request chunks don't clear the pin yet — the response is still
	// pending and we reuse the same pin for the response path.
	if chunk.GetFin() && !chunk.GetIsRequest() {
		_ = s.sessions.DeleteRequestPin(ctx, requestID)
	}
}

// routeProxyHttpResponse forwards a service-emitted ProxyHttpResponse back to
// the originating caller. The request-pin records who that caller is; on
// terminal responses (no body_chunked or carrying an error) the pin is
// cleared. When body_chunked=true we leave the pin in place so subsequent
// ProxyHttpBodyChunk frames can find the caller.
func (s *GatewayServer) routeProxyHttpResponse(ctx context.Context, client *ClientSession, sender models.Identity, resp *pb.ProxyHttpResponse) {
	requestID := resp.GetRequestId()
	if requestID == "" {
		return
	}

	pinValue, err := s.sessions.GetRequestPin(ctx, requestID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("request_id", requestID).Msg("ProxyHttpResponse pin lookup failed")
		return
	}
	if pinValue == "" {
		// Caller has either gone away or the request never registered a pin
		// (e.g. error path). Drop quietly.
		logging.Logger.Debug().Str("request_id", requestID).Msg("ProxyHttpResponse: no pin")
		return
	}
	caller, _ := decodeRequestPin(pinValue)
	if caller == "" {
		return
	}

	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpResponse{ProxyHttpResponse: resp},
	}
	if pubErr := s.publishProxyEnvelope(ctx, caller, downstream); pubErr != nil {
		logging.Logger.Warn().Err(pubErr).Str("request_id", requestID).Str("target", caller).Msg("ProxyHttpResponse forward failed")
		_ = s.sessions.DeleteRequestPin(ctx, requestID)
		return
	}

	// Terminate the pin when the response is single-shot or carries an
	// error; chunked-body responses keep the pin alive until the final chunk
	// arrives (handled in routeProxyHttpBodyChunk).
	if resp.GetError() != nil || !resp.GetBodyChunked() {
		_ = s.sessions.DeleteRequestPin(ctx, requestID)
		return
	}
	// Chunked response in progress — refresh the pin so the chunks find it.
	if rErr := s.sessions.RefreshRequestPin(ctx, requestID, requestPinTTL(0)); rErr != nil {
		logging.Logger.Debug().Err(rErr).Str("request_id", requestID).Msg("ProxyHttpResponse pin refresh failed (non-fatal)")
	}
	_ = sender
	_ = client
}

// =============================================================================
// TunnelOpen / TunnelData / TunnelClose
// =============================================================================

func (s *GatewayServer) routeTunnelOpen(ctx context.Context, client *ClientSession, sender models.Identity, open *pb.TunnelOpen) {
	tunnelID := open.GetTunnelId()
	target := open.GetTargetTopic()

	if tunnelID == "" {
		sendTunnelClose(client, "", pb.TunnelClose_ERROR, "tunnel_id is required")
		return
	}

	// 0. Quota: cap concurrent tunnels per workspace.
	cap := s.quotaEnforcer.getMaxConcurrentTunnelsPerWorkspace()
	counter := s.tunnelCounterFor(sender.Workspace)
	if cur := counter.n.Load(); cur >= int64(cap) {
		s.auditTunnelOpenFailure(ctx, sender, target, tunnelID, client.SessionUUID, nil,
			fmt.Sprintf("workspace tunnel cap %d reached", cap))
		sendTunnelClose(client, tunnelID, pb.TunnelClose_QUOTA,
			fmt.Sprintf("workspace tunnel cap %d reached", cap))
		return
	}

	// 0b. Hop-depth check (T40). Symmetric with ProxyHttpRequest: reject when
	// inbound depth >= cap, otherwise increment before forwarding so loops
	// like agent → sandbox → agent → sandbox terminate.
	maxDepth := s.quotaEnforcer.getMaxChainDepth()
	if open.GetProxyChainDepth() >= maxDepth {
		detail := fmt.Sprintf("proxy_chain_depth_exceeded: inbound=%d cap=%d", open.GetProxyChainDepth(), maxDepth)
		s.auditTunnelOpenFailure(ctx, sender, target, tunnelID, client.SessionUUID, nil, detail)
		sendTunnelClose(client, tunnelID, pb.TunnelClose_ERROR, "ACL_DENIED: "+detail)
		return
	}
	open.ProxyChainDepth = open.GetProxyChainDepth() + 1

	// 1. Resolve wildcard → concrete and rewrite envelope. sv::{impl} wildcards
	//    are expanded; all other target forms are used as-is.
	concrete, err := s.resolveProxyTarget(ctx, target)
	if err != nil {
		s.auditTunnelOpenFailure(ctx, sender, target, tunnelID, client.SessionUUID, nil, err.Error())
		sendTunnelClose(client, tunnelID, pb.TunnelClose_ERROR, "SIDECAR_UNAVAILABLE: "+err.Error())
		return
	}
	open.TargetTopic = concrete

	// 2. ACL.
	resolvedAuthority, err := s.proxyACLCheck(ctx, client, sender, concrete, open.GetAuthorization())
	if err != nil {
		s.auditTunnelOpenFailure(ctx, sender, concrete, tunnelID, client.SessionUUID, resolvedAuthority, err.Error())
		sendTunnelClose(client, tunnelID, pb.TunnelClose_ERROR, "ACL_DENIED: "+err.Error())
		return
	}

	// 3. Pin tunnel_id → caller|concrete with TTL ≥ idle_timeout_ms.
	// Encoding both sides lets routeTunnelAck deliver acks to the correct
	// counterparty regardless of which direction the credit grant flows.
	pinTTL := tunnelPinTTL(open.GetIdleTimeoutMs())
	pinValue := encodeTunnelPin(sender.ToTopic(), concrete)
	if pinErr := s.sessions.SetTunnelPin(ctx, tunnelID, pinValue, pinTTL); pinErr != nil {
		s.auditTunnelOpenFailure(ctx, sender, concrete, tunnelID, client.SessionUUID, resolvedAuthority,
			fmt.Sprintf("failed to record tunnel pin: %v", pinErr))
		sendTunnelClose(client, tunnelID, pb.TunnelClose_ERROR, "internal: pin failed")
		return
	}

	// 4. Publish. The downstream oneof has no TunnelOpen variant; sidecars
	// receive the open as the seq=0 TunnelData frame whose payload is the
	// marshaled TunnelOpen envelope (target rewritten to the concrete topic).
	openBytes, marshErr := proto.Marshal(open)
	if marshErr != nil {
		_ = s.sessions.DeleteTunnelPin(ctx, tunnelID)
		s.auditTunnelOpenFailure(ctx, sender, concrete, tunnelID, client.SessionUUID, resolvedAuthority,
			fmt.Sprintf("marshal TunnelOpen: %v", marshErr))
		sendTunnelClose(client, tunnelID, pb.TunnelClose_ERROR, "internal: marshal failed")
		return
	}
	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelData{
			TunnelData: &pb.TunnelData{
				TunnelId: tunnelID,
				Seq:      0,
				Data:     openBytes,
			},
		},
	}
	if err := s.publishProxyEnvelope(ctx, concrete, downstream); err != nil {
		_ = s.sessions.DeleteTunnelPin(ctx, tunnelID)
		s.auditTunnelOpenFailure(ctx, sender, concrete, tunnelID, client.SessionUUID, resolvedAuthority, err.Error())
		sendTunnelClose(client, tunnelID, pb.TunnelClose_ERROR, "SIDECAR_UNAVAILABLE: "+err.Error())
		return
	}

	counter.n.Add(1)
	s.auditTunnelOpened(ctx, sender, concrete, tunnelID, client.SessionUUID, resolvedAuthority)
}

// tunnelPinTTL converts a TunnelOpen.idle_timeout_ms into a Redis TTL,
// honouring the spec's "TTL >= idle_timeout_ms" requirement and falling back
// to a 5-minute default when idle timeout is unset.
func tunnelPinTTL(idleMs int64) time.Duration {
	if idleMs <= 0 {
		return defaultTunnelPinTTL
	}
	// Round up: spec says TTL >= idle. Add 30s safety buffer to absorb
	// clock skew and refresh latency on the data path.
	return time.Duration(idleMs)*time.Millisecond + 30*time.Second
}

// followPin returns the concrete service identity bound to tunnelID,
// emitting a PEER_RESET to the caller and clearing local bookkeeping when
// the pin is missing or the pinned principal is no longer connected. Returns
// "" when the caller MUST stop processing this frame.
func (s *GatewayServer) followPin(ctx context.Context, client *ClientSession, tunnelID, workspace string) string {
	pinValue, err := s.sessions.GetTunnelPin(ctx, tunnelID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("tunnel_id", tunnelID).Msg("tunnel pin lookup failed")
		sendTunnelClose(client, tunnelID, pb.TunnelClose_PEER_RESET, "pin lookup failed")
		return ""
	}
	if pinValue == "" {
		sendTunnelClose(client, tunnelID, pb.TunnelClose_PEER_RESET, "tunnel pin expired or unknown")
		s.tunnelCounterFor(workspace).n.Add(-1)
		return ""
	}
	_, concrete := decodeTunnelPin(pinValue)
	if concrete == "" {
		sendTunnelClose(client, tunnelID, pb.TunnelClose_PEER_RESET, "tunnel pin malformed")
		return ""
	}
	// Verify the pinned principal is still reachable. If neither local nor
	// cluster-wide active, emit PEER_RESET and clear the pin.
	if _, locallyConnected := s.identityIndex.Load(concrete); !locallyConnected {
		active, _ := s.sessions.IsActive(ctx, concrete)
		if !active {
			_ = s.sessions.DeleteTunnelPin(ctx, tunnelID)
			s.tunnelCounterFor(workspace).n.Add(-1)
			sendTunnelClose(client, tunnelID, pb.TunnelClose_PEER_RESET, "pinned sidecar disconnected")
			return ""
		}
	}
	return concrete
}

func (s *GatewayServer) routeTunnelData(ctx context.Context, client *ClientSession, sender models.Identity, data *pb.TunnelData) {
	tunnelID := data.GetTunnelId()
	concrete := s.followPin(ctx, client, tunnelID, sender.Workspace)
	if concrete == "" {
		return
	}
	// Quota: per-tunnel byte cap. Tracked in a small in-memory map; we
	// don't persist these counters across gateway restarts.
	if cap := s.quotaEnforcer.getMaxTunnelBytes(); cap > 0 {
		counter := s.tunnelByteCounterFor(tunnelID)
		if total := counter.Add(int64(len(data.Data))); total > cap {
			_ = s.sessions.DeleteTunnelPin(ctx, tunnelID)
			s.tunnelCounterFor(sender.Workspace).n.Add(-1)
			s.deleteTunnelByteCounter(tunnelID)
			sendTunnelClose(client, tunnelID, pb.TunnelClose_QUOTA,
				fmt.Sprintf("per-tunnel byte cap %d exceeded", cap))
			return
		}
	}
	// Refresh the pin so an in-flight tunnel doesn't time out mid-stream.
	if err := s.sessions.RefreshTunnelPin(ctx, tunnelID, defaultTunnelPinTTL); err != nil {
		logging.Logger.Debug().Err(err).Str("tunnel_id", tunnelID).Msg("tunnel pin refresh failed (non-fatal)")
	}

	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelData{TunnelData: data},
	}
	// Single-node fast path: when the pinned sidecar is connected to this
	// gateway, deliver directly. TunnelData is a data-plane envelope and is
	// not audited per-frame, so bypassing RMQ does not lose observability.
	if s.deliverDataPlaneLocal(concrete, "tunnel_data", downstream) {
		return
	}
	if err := s.publishProxyEnvelope(ctx, concrete, downstream); err != nil {
		logging.Logger.Warn().Err(err).Str("tunnel_id", tunnelID).Str("target", concrete).Msg("failed to forward TunnelData")
		// Don't auto-close on a single publish failure — caller may retry.
	}
}

// tunnelPinSep separates the caller and service portions of a tunnel pin
// value. The pin value is `caller_identity|service_identity`; legacy single-
// value pins (no separator) are interpreted as service-only with no caller.
const tunnelPinSep = "|"

// encodeTunnelPin returns the pin value used for SetTunnelPin: caller and
// service identities joined by tunnelPinSep. Empty caller produces a
// service-only value, preserving legacy compatibility.
func encodeTunnelPin(caller, service string) string {
	if caller == "" {
		return service
	}
	return caller + tunnelPinSep + service
}

// decodeTunnelPin splits a pin value into caller and service. Legacy values
// without a separator are treated as service-only.
func decodeTunnelPin(value string) (caller, service string) {
	if idx := strings.Index(value, tunnelPinSep); idx >= 0 {
		return value[:idx], value[idx+1:]
	}
	return "", value
}

// routeTunnelAck forwards an upstream-bound TunnelAck from one peer (typically
// the sidecar) to the *other* peer (the original caller) as a downstream
// TunnelAck. The pin records both caller and service identities, so we deliver
// to whichever side is NOT the sender.
//
// Acks are flow-control hints, not session-fatal: a missing pin or
// disconnected counterparty is logged at debug and dropped silently. We do
// NOT emit PEER_RESET on missing-pin for an Ack (that would over-react to
// transient lost-credits).
func (s *GatewayServer) routeTunnelAck(ctx context.Context, client *ClientSession, sender models.Identity, ack *pb.TunnelAck) {
	tunnelID := ack.GetTunnelId()
	if tunnelID == "" {
		return
	}
	pinValue, err := s.sessions.GetTunnelPin(ctx, tunnelID)
	if err != nil {
		logging.Logger.Debug().Err(err).Str("tunnel_id", tunnelID).Msg("ack: tunnel pin lookup failed")
		return
	}
	if pinValue == "" {
		// Pin expired or unknown — ack has nowhere to land. Drop quietly.
		return
	}
	caller, service := decodeTunnelPin(pinValue)

	// Pick the destination: whichever side is NOT the sender. If the pin
	// lacks a caller (legacy/compat or pre-Open ack race), only forward
	// caller→service direction.
	senderTopic := sender.ToTopic()
	var destTopic string
	switch senderTopic {
	case service:
		destTopic = caller
	case caller:
		destTopic = service
	default:
		// Sender isn't a known peer for this tunnel. Drop.
		logging.Logger.Debug().Str("tunnel_id", tunnelID).Str("sender", senderTopic).
			Str("caller", caller).Str("service", service).Msg("ack: sender not a known tunnel peer")
		return
	}
	if destTopic == "" {
		// Counterparty not tracked yet. Drop quietly — credits will be re-
		// granted when the next peer-side ack arrives.
		return
	}

	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelAck{TunnelAck: ack},
	}
	// Single-node fast path: TunnelAck is a flow-control hint, not audited.
	// If the destination peer is locally connected, deliver directly.
	if s.deliverDataPlaneLocal(destTopic, "tunnel_ack", downstream) {
		return
	}
	if err := s.publishProxyEnvelope(ctx, destTopic, downstream); err != nil {
		logging.Logger.Debug().Err(err).Str("tunnel_id", tunnelID).Str("target", destTopic).
			Msg("routeTunnelAck: forward failed (non-fatal)")
	}
}

func (s *GatewayServer) routeTunnelClose(ctx context.Context, client *ClientSession, sender models.Identity, closeMsg *pb.TunnelClose) {
	tunnelID := closeMsg.GetTunnelId()
	pinValue, err := s.sessions.GetTunnelPin(ctx, tunnelID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("tunnel_id", tunnelID).Msg("close: tunnel pin lookup failed")
	}
	_, concrete := decodeTunnelPin(pinValue)

	// Best-effort delivery to the pinned sidecar so it can release backend
	// resources. Skip when we have no pin (likely already cleaned up).
	if concrete != "" {
		downstream := &pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelClose{TunnelClose: closeMsg},
		}
		if pubErr := s.publishProxyEnvelope(ctx, concrete, downstream); pubErr != nil {
			logging.Logger.Debug().Err(pubErr).Str("tunnel_id", tunnelID).Msg("close-frame forward failed (non-fatal)")
		}
	}

	_ = s.sessions.DeleteTunnelPin(ctx, tunnelID)
	s.deleteTunnelByteCounter(tunnelID)
	s.tunnelCounterFor(sender.Workspace).n.Add(-1)

	s.auditTunnelClosed(ctx, sender, concrete, tunnelID, client.SessionUUID, closeMsg.GetReason(), closeMsg.GetDetail())
}

// =============================================================================
// Per-tunnel byte counters
// =============================================================================

func (s *GatewayServer) tunnelByteCounterFor(tunnelID string) *atomic.Int64 {
	if v, ok := s.tunnelByteCounters.Load(tunnelID); ok {
		return v.(*atomic.Int64)
	}
	c := new(atomic.Int64)
	actual, _ := s.tunnelByteCounters.LoadOrStore(tunnelID, c)
	return actual.(*atomic.Int64)
}

func (s *GatewayServer) deleteTunnelByteCounter(tunnelID string) {
	s.tunnelByteCounters.Delete(tunnelID)
}

// =============================================================================
// Audit helpers
// =============================================================================

func (s *GatewayServer) auditProxyHttpSuccess(ctx context.Context, sender models.Identity, target, requestID string, sessionID uuid.UUID, authority *acl.ResolvedAuthority, bodySize int) {
	event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpProxyHttpRouted, target, sender.Workspace, sessionID, true, "", map[string]interface{}{
		"from":       sender.ToTopic(),
		"to":         target,
		"request_id": requestID,
		"body_size":  bodySize,
	})
	applyResolvedAuthorityToAuditEvent(event, authority)
	s.auditLog(ctx, event)
}

func (s *GatewayServer) auditProxyHttpFailure(ctx context.Context, sender models.Identity, target, requestID string, sessionID uuid.UUID, authority *acl.ResolvedAuthority, reason string) {
	event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpProxyHttpFailed, target, sender.Workspace, sessionID, false, reason, map[string]interface{}{
		"from":       sender.ToTopic(),
		"to":         target,
		"request_id": requestID,
	})
	applyResolvedAuthorityToAuditEvent(event, authority)
	s.auditLog(ctx, event)
}

func (s *GatewayServer) auditTunnelOpened(ctx context.Context, sender models.Identity, target, tunnelID string, sessionID uuid.UUID, authority *acl.ResolvedAuthority) {
	event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpTunnelOpened, target, sender.Workspace, sessionID, true, "", map[string]interface{}{
		"from":      sender.ToTopic(),
		"to":        target,
		"tunnel_id": tunnelID,
	})
	applyResolvedAuthorityToAuditEvent(event, authority)
	s.auditLog(ctx, event)
}

func (s *GatewayServer) auditTunnelOpenFailure(ctx context.Context, sender models.Identity, target, tunnelID string, sessionID uuid.UUID, authority *acl.ResolvedAuthority, reason string) {
	event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpTunnelOpenFailed, target, sender.Workspace, sessionID, false, reason, map[string]interface{}{
		"from":      sender.ToTopic(),
		"to":        target,
		"tunnel_id": tunnelID,
	})
	applyResolvedAuthorityToAuditEvent(event, authority)
	s.auditLog(ctx, event)
}

func (s *GatewayServer) auditTunnelClosed(ctx context.Context, sender models.Identity, target, tunnelID string, sessionID uuid.UUID, reason pb.TunnelClose_Reason, detail string) {
	event := audit.NewMessageEvent(string(sender.Type), sender.String(), audit.OpTunnelClosed, target, sender.Workspace, sessionID, true, "", map[string]interface{}{
		"from":      sender.ToTopic(),
		"to":        target,
		"tunnel_id": tunnelID,
		"reason":    reason.String(),
		"detail":    detail,
	})
	s.auditLog(ctx, event)
}

// proxyEnvelopeFromUpstream returns a proxyEnvelope from a recognised
// UpstreamMessage variant, or zero value if the variant isn't one of the
// proxy/tunnel oneofs.
func proxyEnvelopeFromUpstream(req *pb.UpstreamMessage) (proxyEnvelope, bool) {
	switch p := req.Payload.(type) {
	case *pb.UpstreamMessage_ProxyHttpRequest:
		return proxyEnvelope{httpReq: p.ProxyHttpRequest}, true
	case *pb.UpstreamMessage_ProxyHttpBodyChunk:
		return proxyEnvelope{httpBodyChunk: p.ProxyHttpBodyChunk}, true
	case *pb.UpstreamMessage_ProxyHttpResponse:
		return proxyEnvelope{httpResp: p.ProxyHttpResponse}, true
	case *pb.UpstreamMessage_TunnelOpen:
		return proxyEnvelope{tunnelOpen: p.TunnelOpen}, true
	case *pb.UpstreamMessage_TunnelData:
		return proxyEnvelope{tunnelData: p.TunnelData}, true
	case *pb.UpstreamMessage_TunnelAck:
		return proxyEnvelope{tunnelAck: p.TunnelAck}, true
	case *pb.UpstreamMessage_TunnelClose:
		return proxyEnvelope{tunnelClose: p.TunnelClose}, true
	}
	return proxyEnvelope{}, false
}
