package aether

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// metadataCapturingGateway is a minimal in-process AetherGateway server that
// records the gRPC metadata seen on the inbound Connect stream. It exists to
// prove ClientOptions.Metadata is emitted as transport-level headers (e.g. the
// aggregator's x-aether-tenant pairing hint) before any frame is processed.
type metadataCapturingGateway struct {
	pb.UnimplementedAetherGatewayServer

	mu   sync.Mutex
	md   metadata.MD
	seen chan struct{}
}

func (g *metadataCapturingGateway) Connect(stream grpc.BidiStreamingServer[pb.UpstreamMessage, pb.DownstreamMessage]) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	g.mu.Lock()
	g.md = md.Copy()
	g.mu.Unlock()
	close(g.seen)

	// Drain inbound frames until the client tears the stream down. We never
	// send a ConnectionAck — the test only needs the init frame to have been
	// flushed, which Connect's establishStream does synchronously.
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

func TestServiceClient_ConnectEmitsStreamMetadata(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	gw := &metadataCapturingGateway{seen: make(chan struct{})}
	srv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(srv, gw)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	client, err := NewServiceClient(ServiceOptions{
		ClientOptions: ClientOptions{
			ServerAddr: lis.Addr().String(),
			Metadata:   map[string]string{"x-aether-tenant": "acme"},
		},
		Implementation: "sandbox-provider",
		Specifier:      "default",
	})
	if err != nil {
		t.Fatalf("NewServiceClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	select {
	case <-gw.seen:
	case <-ctx.Done():
		t.Fatal("timed out waiting for server to observe Connect metadata")
	}

	gw.mu.Lock()
	got := gw.md.Get("x-aether-tenant")
	gw.mu.Unlock()
	if len(got) != 1 || got[0] != "acme" {
		t.Fatalf("x-aether-tenant header = %v, want [acme]", got)
	}
}

// TestServiceClient_ConnectNoMetadataIsBackwardCompatible asserts that when no
// Metadata is supplied, Connect still succeeds and emits no x-aether-tenant
// header (zero-value behavior is unchanged).
func TestServiceClient_ConnectNoMetadataIsBackwardCompatible(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	gw := &metadataCapturingGateway{seen: make(chan struct{})}
	srv := grpc.NewServer()
	pb.RegisterAetherGatewayServer(srv, gw)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	client, err := NewServiceClient(ServiceOptions{
		ClientOptions:  ClientOptions{ServerAddr: lis.Addr().String()},
		Implementation: "sandbox-provider",
		Specifier:      "default",
	})
	if err != nil {
		t.Fatalf("NewServiceClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	select {
	case <-gw.seen:
	case <-ctx.Done():
		t.Fatal("timed out waiting for server to observe Connect")
	}

	gw.mu.Lock()
	got := gw.md.Get("x-aether-tenant")
	gw.mu.Unlock()
	if len(got) != 0 {
		t.Fatalf("x-aether-tenant header = %v, want none", got)
	}
}
