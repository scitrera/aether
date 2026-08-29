package aether

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type reconnectRunGateway struct {
	pb.UnimplementedAetherGatewayServer

	disconnectWithSignal bool
	connections          atomic.Int32
}

func (g *reconnectRunGateway) Connect(stream grpc.BidiStreamingServer[pb.UpstreamMessage, pb.DownstreamMessage]) error {
	connection := g.connections.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(newMockConnectionAck("reconnect-run", connection > 1)); err != nil {
		return err
	}

	if connection == 1 {
		if g.disconnectWithSignal {
			if err := stream.Send(&pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Signal{
				Signal: &pb.Signal{Type: pb.Signal_GRACEFUL_DISCONNECT, Reason: "cycle connection"},
			}}); err != nil {
				return err
			}
			<-stream.Context().Done()
			return nil
		}
		return status.Error(codes.Unavailable, "cycle connection")
	}

	if err := stream.Send(newMockIncomingMessage("test-source", []byte("after reconnect"))); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func TestBaseClientRunContinuesAfterSuccessfulReconnect(t *testing.T) {
	tests := []struct {
		name                 string
		disconnectWithSignal bool
	}{
		{name: "receive error"},
		{name: "graceful disconnect signal", disconnectWithSignal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer listener.Close()

			gateway := &reconnectRunGateway{disconnectWithSignal: tt.disconnectWithSignal}
			server := grpc.NewServer()
			pb.RegisterAetherGatewayServer(server, gateway)
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()

			client, err := NewAgentClient(AgentOptions{
				ClientOptions: ClientOptions{
					ServerAddr: listener.Addr().String(),
					Connection: ConnectionOptions{
						AutoReconnect:     true,
						MaxRetries:        3,
						InitialBackoff:    time.Millisecond,
						MaxBackoff:        5 * time.Millisecond,
						BackoffMultiplier: 1,
						ConnectTimeout:    time.Second,
					},
				},
				Workspace:      "test-workspace",
				Implementation: "reconnect-run",
				Specifier:      tt.name,
			})
			if err != nil {
				t.Fatalf("NewAgentClient: %v", err)
			}
			defer client.Close()

			received := make(chan string, 1)
			client.OnMessage(func(_ context.Context, message *Message) error {
				received <- string(message.Payload)
				return nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.Connect(ctx); err != nil {
				t.Fatalf("Connect: %v", err)
			}

			done := make(chan error, 1)
			go func() { done <- client.Run(ctx) }()

			select {
			case payload := <-received:
				if payload != "after reconnect" {
					t.Fatalf("payload = %q, want after reconnect", payload)
				}
			case err := <-done:
				t.Fatalf("Run exited before receiving from reconnected stream: %v", err)
			case <-ctx.Done():
				t.Fatal("timed out waiting for message after reconnect")
			}

			if got := gateway.connections.Load(); got < 2 {
				t.Fatalf("connections = %d, want at least 2", got)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Run did not exit after context cancellation")
			}
		})
	}
}
