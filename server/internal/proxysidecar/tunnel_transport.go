package proxysidecar

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
)

// sendUpstreamTimeout caps how long a single SendProxyHttp* call will
// block waiting for queue space. Sized to comfortably outlast the
// gateway's normal request lifecycle while still failing fast when the
// upstream connection is genuinely wedged so callers can release their
// HTTP-backend connections instead of pinning resources indefinitely.
const sendUpstreamTimeout = 30 * time.Second

// serviceClientTransport is the production tunnelTransport implementation
// used by the Terminator. It ships TunnelData/Ack/Close frames upstream
// through the embedded ServiceClient's request queue; the gateway's
// routeTunnelAck / routeTunnelClose / proxy-envelope dispatch then forwards
// them to the original caller.
//
// All Send* methods route through ServiceClient.SendWithPriority so that
// per-envelope priority drives the SDK's CoDel-managed admission queue.
// The priority taxonomy is defined in sdk/go/aether/priority.go:
//
//   - TunnelClose                       → PriorityControl
//   - TunnelAck                         → PriorityResponseHeader
//   - TunnelData                        → PriorityResponseChunk
//   - ProxyHttpResponse (success)       → PriorityResponseHeader
//   - ProxyHttpResponse (error variant) → PriorityControl
//   - ProxyHttpBodyChunk (fin=true)     → PriorityResponseHeader (terminal)
//   - ProxyHttpBodyChunk (other)        → PriorityResponseChunk
type serviceClientTransport struct {
	// client now wraps an AgentClient (agent identity); all Send* methods
	// call BaseClient methods, so only the field type changed.
	client *aether.AgentClient
}

// SendTunnelData ships a TunnelData frame upstream. TunnelData carries the
// bulk of bytes in an open tunnel — classify as PriorityResponseChunk so it
// can be shed under sustained load before higher-priority control envelopes.
func (s *serviceClientTransport) SendTunnelData(d *pb.TunnelData) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendUpstreamTimeout)
	defer cancel()
	return s.client.SendWithPriority(ctx, aether.PriorityResponseChunk, &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelData{TunnelData: d},
	})
}

// SendTunnelAck ships a TunnelAck frame upstream. Acks gate the caller's
// outbound flow-control window; treat them like response headers
// (PriorityResponseHeader) so they slip past bulk-data shedding.
func (s *serviceClientTransport) SendTunnelAck(a *pb.TunnelAck) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendUpstreamTimeout)
	defer cancel()
	return s.client.SendWithPriority(ctx, aether.PriorityResponseHeader, &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelAck{TunnelAck: a},
	})
}

// SendTunnelClose ships a TunnelClose upstream. Close frames are session-
// critical: dropping a close leaks tunnel state on both ends. Use
// PriorityControl so the admission queue admits it ahead of bulk traffic.
func (s *serviceClientTransport) SendTunnelClose(c *pb.TunnelClose) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendUpstreamTimeout)
	defer cancel()
	return s.client.SendWithPriority(ctx, aether.PriorityControl, &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelClose{TunnelClose: c},
	})
}

// SendProxyHttpResponse ships a ProxyHttpResponse upstream so the gateway
// can route it back to the originating caller via the request-pin.
//
// Priority selection: error responses (non-zero ProxyError on the message)
// promote to PriorityControl so the caller observes the failure even when
// the upstream queue is saturated. Successful response headers ride
// PriorityResponseHeader.
func (s *serviceClientTransport) SendProxyHttpResponse(r *pb.ProxyHttpResponse) error {
	prio := aether.PriorityResponseHeader
	if r.GetError() != nil {
		prio = aether.PriorityControl
	}
	if log.Debug().Enabled() {
		log.Debug().
			Str("dir", "terminator-out").
			Str("op", "ProxyHttpResponse").
			Str("request_id", r.GetRequestId()).
			Int32("status", r.GetStatusCode()).
			Int("body_bytes", len(r.GetBody())).
			Bool("body_chunked", r.GetBodyChunked()).
			Int("priority", int(prio)).
			Msg("terminator: envelope")
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendUpstreamTimeout)
	defer cancel()
	return s.client.SendWithPriority(ctx, prio, &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpResponse{ProxyHttpResponse: r},
	})
}

// SendProxyHttpBodyChunk ships a ProxyHttpBodyChunk upstream. The terminator
// emits these (with is_request=false) when a backend response exceeds the
// inline body cap and must be streamed.
//
// Priority selection: terminal chunks (fin=true) close the stream — losing
// the terminal corrupts the response, so promote to PriorityResponseHeader.
// Mid-stream chunks ride PriorityResponseChunk and are eligible for
// shedding when sustained latency exceeds CoDel target.
func (s *serviceClientTransport) SendProxyHttpBodyChunk(c *pb.ProxyHttpBodyChunk) error {
	prio := aether.PriorityResponseChunk
	if c.GetFin() {
		prio = aether.PriorityResponseHeader
	}
	if log.Debug().Enabled() {
		log.Debug().
			Str("dir", "terminator-out").
			Str("op", "ProxyHttpBodyChunk").
			Str("request_id", c.GetRequestId()).
			Uint32("seq", c.GetSeq()).
			Bool("fin", c.GetFin()).
			Int("data_bytes", len(c.GetData())).
			Int("priority", int(prio)).
			Msg("terminator: envelope")
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendUpstreamTimeout)
	defer cancel()
	return s.client.SendWithPriority(ctx, prio, &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: c},
	})
}

// Compile-time check that serviceClientTransport satisfies the tunnelTransport
// interface declared in tunnel_tcp.go.
var _ tunnelTransport = (*serviceClientTransport)(nil)
