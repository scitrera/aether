package aether

import (
	"bytes"
	"context"
	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"testing"
	"time"
)

type imagePayloadEchoServer struct {
	pb.UnimplementedAetherGatewayServer
}

func (imagePayloadEchoServer) Connect(stream pb.AetherGateway_ConnectServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	return stream.Send(&pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Msg{Msg: &pb.IncomingMessage{Payload: msg.GetSend().GetPayload()}}})
}
func TestClientReceivesImageAboveDefaultGRPCLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(9*1024*1024), grpc.MaxSendMsgSize(16*1024*1024))
	pb.RegisterAetherGatewayServer(server, imagePayloadEchoServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	base, err := NewBaseClient(BaseClientConfig{ServerAddr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	opts, err := base.buildDialOptions()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(listener.Addr().String(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	for _, legacy := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var callOpts []grpc.CallOption
		if legacy {
			callOpts = append(callOpts, grpc.MaxCallRecvMsgSize(4*1024*1024))
		}
		stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx, callOpts...)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("x"), 7*1024*1024)
		err = stream.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Send{Send: &pb.SendMessage{Payload: payload}}})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		response, err := stream.Recv()
		cancel()
		if legacy {
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("legacy negative control: %v", err)
			}
		} else if err != nil || !bytes.Equal(response.GetMsg().GetPayload(), payload) {
			t.Fatalf("image echo mismatch: %v", err)
		}
	}
}
