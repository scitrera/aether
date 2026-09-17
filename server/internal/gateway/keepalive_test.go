package gateway

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type keepaliveEchoServer struct {
	pb.UnimplementedAetherGatewayServer
}

func (keepaliveEchoServer) Connect(stream pb.AetherGateway_ConnectServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.Send(&pb.DownstreamMessage{}); err != nil {
			return err
		}
	}
}

func TestConnectionRotationPreservesActiveStream(t *testing.T) {
	for _, forcedGrace := range []bool{false, true} {
		name := "graceful_default"
		if forcedGrace {
			name = "forced_grace_negative_control"
		}
		t.Run(name, func(t *testing.T) {
			params := StreamKeepaliveParameters()
			// Accelerate the real transport's age timer, including its jitter.
			params.MaxConnectionAge = 100 * time.Millisecond
			if forcedGrace {
				params.MaxConnectionAgeGrace = 10 * time.Millisecond
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(grpc.KeepaliveParams(params))
			pb.RegisterAetherGatewayServer(server, keepaliveEchoServer{})
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&pb.UpstreamMessage{}); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(400 * time.Millisecond)
			sendErr := stream.Send(&pb.UpstreamMessage{})
			_, receiveErr := stream.Recv()
			if forcedGrace {
				if receiveErr == nil {
					t.Fatal("negative control did not terminate the aged stream")
				}
				return
			}
			if sendErr != nil || receiveErr != nil {
				t.Fatalf("active stream failed after rotation: send=%v receive=%v", sendErr, receiveErr)
			}
		})
	}
}
