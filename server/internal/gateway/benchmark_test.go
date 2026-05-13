package gateway

import (
	"context"
	"sync"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc/metadata"
)

// benchStream implements the gRPC stream interface for benchmarks.
// Named differently from mockStream in connect_test.go to avoid redeclaration.
type benchStream struct {
	mu   sync.Mutex
	msgs []*pb.DownstreamMessage
}

func (m *benchStream) Send(msg *pb.DownstreamMessage) error {
	m.mu.Lock()
	m.msgs = append(m.msgs, msg)
	m.mu.Unlock()
	return nil
}

func (m *benchStream) Recv() (*pb.UpstreamMessage, error) { return nil, nil }
func (m *benchStream) SetHeader(metadata.MD) error        { return nil }
func (m *benchStream) SendHeader(metadata.MD) error       { return nil }
func (m *benchStream) SetTrailer(metadata.MD)             {}
func (m *benchStream) Context() context.Context           { return context.Background() }
func (m *benchStream) SendMsg(v interface{}) error        { return nil }
func (m *benchStream) RecvMsg(v interface{}) error        { return nil }

func newBenchClientSession() (*ClientSession, *benchStream) {
	ms := &benchStream{}
	cs := &ClientSession{
		ID:            "bench-session",
		Stream:        ms,
		deliveryCh:    make(chan *pb.DownstreamMessage, deliveryBufferSize),
		subscriptions: make(map[string]func()),
	}
	return cs, ms
}

// ---------------------------------------------------------------------------
// BenchmarkSafeSend – message serialisation through SafeSend with mock stream
// ---------------------------------------------------------------------------

func BenchmarkSafeSend(b *testing.B) {
	b.ReportAllocs()

	client, _ := newBenchClientSession()
	msg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Signal{
			Signal: &pb.Signal{
				Type:   pb.Signal_GRACEFUL_DISCONNECT,
				Reason: "benchmark signal payload",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.SafeSend(msg)
	}
}

func BenchmarkSafeSend_Concurrent(b *testing.B) {
	b.ReportAllocs()

	client, _ := newBenchClientSession()
	msg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Signal{
			Signal: &pb.Signal{
				Type:   pb.Signal_GRACEFUL_DISCONNECT,
				Reason: "concurrent benchmark signal payload",
			},
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = client.SafeSend(msg)
		}
	})
}

// ---------------------------------------------------------------------------
// BenchmarkDeliver – non-blocking channel enqueue in Deliver
// ---------------------------------------------------------------------------

func BenchmarkDeliver(b *testing.B) {
	b.ReportAllocs()

	client := &ClientSession{
		ID:         "bench-deliver",
		deliveryCh: make(chan *pb.DownstreamMessage, b.N+1024),
	}
	msg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Signal{
			Signal: &pb.Signal{Type: pb.Signal_GRACEFUL_DISCONNECT},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Deliver(msg)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkValidateTopicFormat – topic validation on the routing hot path
// ---------------------------------------------------------------------------

func BenchmarkValidateTopicFormat_Valid(b *testing.B) {
	b.ReportAllocs()
	topic := "ag::production::worker::v1"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateTopicFormat(topic)
	}
}

func BenchmarkValidateTopicFormat_Invalid(b *testing.B) {
	b.ReportAllocs()
	topic := "invalid.topic.prefix"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateTopicFormat(topic)
	}
}
