package nats

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func pickFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func newTempServer(t *testing.T, cfg Config) *EmbeddedServer {
	t.Helper()
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	if cfg.ListenHost == "" {
		cfg.ListenHost = "127.0.0.1"
	}
	if cfg.ClientPort == 0 {
		cfg.ClientPort = -1
	}
	if cfg.ClusterPort == 0 {
		cfg.ClusterPort = -1
	}
	es := &EmbeddedServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := es.Start(ctx, cfg); err != nil {
		t.Fatalf("start embedded server: %v", err)
	}
	return es
}

func TestEmbeddedServer_SingleNode_StartStop(t *testing.T) {
	before := runtime.NumGoroutine()

	es := newTempServer(t, Config{})
	conn := es.Conn()
	if !conn.IsConnected() {
		t.Fatalf("expected client to be connected")
	}

	sub, err := conn.SubscribeSync("test.subject")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := conn.Publish("test.subject", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("next msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("unexpected payload: %q", msg.Data)
	}
	_ = sub.Unsubscribe()

	es.Stop()

	// Give goroutines a brief grace period to wind down.
	for i := 0; i < 20; i++ {
		if runtime.NumGoroutine() <= before+5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Fatalf("possible goroutine leak: before=%d after=%d", before, after)
	}
}

func TestEmbeddedServer_JetStream_StreamCreate(t *testing.T) {
	es := newTempServer(t, Config{})
	t.Cleanup(es.Stop)

	js := es.JetStream()
	if js == nil {
		t.Fatal("expected non-nil jetstream context")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "test_stream",
		Subjects: []string{"test.>"},
		Replicas: es.ReplicasForHA(),
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	if _, err := js.Publish(ctx, "test.one", []byte("payload-1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}

	msg, err := cons.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(msg.Data()) != "payload-1" {
		t.Fatalf("unexpected payload: %q", msg.Data())
	}
}

func TestEmbeddedServer_ReplicasForHA(t *testing.T) {
	cases := []struct {
		name   string
		mode   HAMode
		peers  int
		expect int
	}{
		{"single-node-auto", HAModeAuto, 0, 1},
		{"single-node-sync", HAModeSync, 0, 1},
		{"single-node-async", HAModeAsync, 0, 1},
		{"two-node-auto", HAModeAuto, 1, 2},
		{"two-node-sync", HAModeSync, 1, 2},
		{"two-node-async", HAModeAsync, 1, 1},
		{"three-node-auto", HAModeAuto, 2, 3},
		{"five-node-auto", HAModeAuto, 4, 3},
		{"three-node-sync", HAModeSync, 2, 2},
		{"three-node-async", HAModeAsync, 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := replicasFor(tc.mode, tc.peers)
			if got != tc.expect {
				t.Fatalf("replicasFor(%v, %d) = %d, want %d", tc.mode, tc.peers, got, tc.expect)
			}
		})
	}
}

func TestEmbeddedServer_TwoNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping two-node cluster test in -short mode")
	}

	// Pick two ephemeral cluster ports up-front so the peers can reference
	// each other before either has started.
	portA, portB := pickFreePort(t), pickFreePort(t)
	if portA == portB {
		t.Skip("could not allocate distinct ephemeral ports")
	}

	cfgA := Config{
		ClusterName: "aetherlite-test",
		NodeName:    "node-a",
		ListenHost:  "127.0.0.1",
		ClientPort:  -1,
		ClusterPort: portA,
		Peers:       []string{fmt.Sprintf("nats://127.0.0.1:%d", portB)},
	}
	cfgB := Config{
		ClusterName: "aetherlite-test",
		NodeName:    "node-b",
		ListenHost:  "127.0.0.1",
		ClientPort:  -1,
		ClusterPort: portB,
		Peers:       []string{fmt.Sprintf("nats://127.0.0.1:%d", portA)},
	}

	esA := newTempServer(t, cfgA)
	t.Cleanup(esA.Stop)
	esB := newTempServer(t, cfgB)
	t.Cleanup(esB.Stop)

	// Wait briefly for routes to establish.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if esA.Conn().IsConnected() && esB.Conn().IsConnected() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := make(chan string, 1)
	sub, err := esB.Conn().Subscribe("cluster.ping", func(m *natsgo.Msg) {
		got <- string(m.Data)
	})
	if err != nil {
		t.Fatalf("subscribe on B: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// Wait for the subscription interest to propagate via gossip.
	time.Sleep(500 * time.Millisecond)

	if err := esA.Conn().Publish("cluster.ping", []byte("ping")); err != nil {
		t.Fatalf("publish on A: %v", err)
	}
	if err := esA.Conn().Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}

	select {
	case payload := <-got:
		if payload != "ping" {
			t.Fatalf("unexpected payload: %q", payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for cross-cluster message")
	}
}

func TestEmbeddedServer_Stop_Idempotent(t *testing.T) {
	es := newTempServer(t, Config{})
	es.Stop()
	es.Stop()
}
