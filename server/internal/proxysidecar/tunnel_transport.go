package proxysidecar

import (
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
)

// serviceClientTransport is the production tunnelTransport implementation
// used by the Terminator. It ships TunnelData/Ack/Close frames upstream
// through the embedded ServiceClient's request queue; the gateway's
// routeTunnelAck / routeTunnelClose / proxy-envelope dispatch then forwards
// them to the original caller.
type serviceClientTransport struct {
	client *aether.ServiceClient
}

func (s *serviceClientTransport) SendTunnelData(d *pb.TunnelData) error {
	return s.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelData{TunnelData: d},
	})
}

func (s *serviceClientTransport) SendTunnelAck(a *pb.TunnelAck) error {
	return s.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelAck{TunnelAck: a},
	})
}

func (s *serviceClientTransport) SendTunnelClose(c *pb.TunnelClose) error {
	return s.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelClose{TunnelClose: c},
	})
}

// SendProxyHttpResponse ships a ProxyHttpResponse upstream so the gateway
// can route it back to the originating caller via the request-pin.
func (s *serviceClientTransport) SendProxyHttpResponse(r *pb.ProxyHttpResponse) error {
	return s.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpResponse{ProxyHttpResponse: r},
	})
}

// SendProxyHttpBodyChunk ships a ProxyHttpBodyChunk upstream. The terminator
// emits these (with is_request=false) when a backend response exceeds the
// inline body cap and must be streamed.
func (s *serviceClientTransport) SendProxyHttpBodyChunk(c *pb.ProxyHttpBodyChunk) error {
	return s.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: c},
	})
}

// Compile-time check that serviceClientTransport satisfies the tunnelTransport
// interface declared in tunnel_tcp.go.
var _ tunnelTransport = (*serviceClientTransport)(nil)
