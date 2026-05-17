package router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natssrv "github.com/scitrera/aether/internal/cluster/nats"
)

// newTestJetStreamRouter spins up a single-node embedded NATS server and
// returns a ready JetStreamRouter plus a cleanup function.
func newTestJetStreamRouter(t *testing.T) (*JetStreamRouter, func()) {
	t.Helper()

	es := &natssrv.EmbeddedServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg := natssrv.Config{
		DataDir:     t.TempDir(),
		ListenHost:  "127.0.0.1",
		ClientPort:  -1,
		ClusterPort: -1,
	}
	if err := es.Start(ctx, cfg); err != nil {
		t.Fatalf("start embedded NATS: %v", err)
	}

	r, err := NewJetStreamRouter(es.JetStream(), es.ReplicasForHA(), nil)
	if err != nil {
		es.Stop()
		t.Fatalf("NewJetStreamRouter: %v", err)
	}

	return r, func() { es.Stop() }
}

// collect subscribes to topic and collects up to n messages, returning them
// in order. Times out after timeout if fewer than n arrive.
func collect(t *testing.T, r *JetStreamRouter, topic string, n int, timeout time.Duration) [][]byte {
	t.Helper()
	var (
		mu   sync.Mutex
		msgs [][]byte
		done = make(chan struct{})
	)
	var once sync.Once

	unsub, err := r.Subscribe(topic, func(data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		mu.Lock()
		msgs = append(msgs, cp)
		if len(msgs) >= n {
			once.Do(func() { close(done) })
		}
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe(%q): %v", topic, err)
	}
	t.Cleanup(unsub)

	select {
	case <-done:
	case <-time.After(timeout):
		t.Errorf("timeout waiting for %d messages on %q", n, topic)
	}

	mu.Lock()
	defer mu.Unlock()
	return msgs
}

// TestJetStreamRouter_PublishSubscribe_RoundTrip verifies that a message
// published to an aether topic is received with the original payload, and that
// the handler does NOT see the NATS-escaped subject (codec is applied correctly).
func TestJetStreamRouter_PublishSubscribe_RoundTrip(t *testing.T) {
	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	const topic = "ag::ws::com.example.agent::v1"
	const payload = "hello-roundtrip"

	received := make(chan []byte, 1)
	unsub, err := r.Subscribe(topic, func(data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		received <- cp
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsub()

	// Small delay to let the consumer bind before publishing.
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	if err := r.Publish(ctx, topic, []byte(payload)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-received:
		if string(got) != payload {
			t.Errorf("payload mismatch: got %q, want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for published message")
	}
}

// TestJetStreamRouter_SubscribeExclusive_OffsetResume verifies that a durable
// consumer resumes from its stored offset after being cancelled and recreated.
// Design: publish batch1 messages, subscribe, drain all batch1, cancel, then
// publish batch2, re-subscribe, verify only batch2 is received. This avoids
// abandoned in-flight messages which would require waiting for AckWait to expire.
func TestJetStreamRouter_SubscribeExclusive_OffsetResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping offset-resume test in -short mode")
	}

	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	const topic = "tu::ws::com.example.task::resume-test"
	const consumerName = "foo"
	const batch1 = 5
	const batch2 = 5

	ctx := context.Background()

	// Publish only the first batch before the first subscription.
	for i := 0; i < batch1; i++ {
		if err := r.Publish(ctx, topic, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Publish batch1[%d]: %v", i, err)
		}
	}

	// First subscription: receive all batch1 messages, then cancel.
	var received1 []string
	var mu1 sync.Mutex
	done1 := make(chan struct{})
	var once1 sync.Once

	unsub1, err := r.SubscribeExclusive(topic, consumerName, func(data []byte) {
		mu1.Lock()
		received1 = append(received1, string(data))
		if len(received1) >= batch1 {
			once1.Do(func() { close(done1) })
		}
		mu1.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeExclusive (first): %v", err)
	}

	select {
	case <-done1:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first batch")
	}
	// Brief pause to ensure all acks are sent before cancelling.
	time.Sleep(100 * time.Millisecond)
	unsub1()

	// Publish the second batch after the first subscription is cancelled.
	for i := batch1; i < batch1+batch2; i++ {
		if err := r.Publish(ctx, topic, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Publish batch2[%d]: %v", i, err)
		}
	}

	// Second subscription with same consumer name: should receive only batch2.
	var received2 []string
	var mu2 sync.Mutex
	done2 := make(chan struct{})
	var once2 sync.Once

	unsub2, err := r.SubscribeExclusive(topic, consumerName, func(data []byte) {
		mu2.Lock()
		received2 = append(received2, string(data))
		if len(received2) >= batch2 {
			once2.Do(func() { close(done2) })
		}
		mu2.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeExclusive (second): %v", err)
	}
	defer unsub2()

	select {
	case <-done2:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for second batch")
	}

	// Allow a little time to ensure no extra messages arrive.
	time.Sleep(200 * time.Millisecond)

	mu2.Lock()
	got2 := len(received2)
	mu2.Unlock()

	if got2 != batch2 {
		t.Errorf("second subscription: got %d messages, want %d", got2, batch2)
	}
}

// TestJetStreamRouter_SubscribeExclusiveFromNow verifies that only messages
// published after the subscription was created are delivered.
func TestJetStreamRouter_SubscribeExclusiveFromNow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping from-now test in -short mode")
	}

	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	const topic = "ta::ws::impl::from-now-test"
	const consumerName = "from-now-consumer"
	ctx := context.Background()

	// Publish 5 messages BEFORE subscribing — these must NOT be received.
	for i := 0; i < 5; i++ {
		if err := r.Publish(ctx, topic, []byte(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatalf("pre-subscribe Publish %d: %v", i, err)
		}
	}

	// Small delay to ensure the messages are committed.
	time.Sleep(100 * time.Millisecond)

	var count atomic.Int64
	done := make(chan struct{})
	var once sync.Once

	unsub, err := r.SubscribeExclusiveFromNow(topic, consumerName, func(data []byte) {
		count.Add(1)
		if count.Load() >= 5 {
			once.Do(func() { close(done) })
		}
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromNow: %v", err)
	}
	defer unsub()

	// Small delay to let consumer bind.
	time.Sleep(100 * time.Millisecond)

	// Publish 5 messages AFTER subscribing — these MUST be received.
	for i := 0; i < 5; i++ {
		if err := r.Publish(ctx, topic, []byte(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatalf("post-subscribe Publish %d: %v", i, err)
		}
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for post-subscribe messages")
	}

	// Wait briefly and ensure no extra (pre-subscribe) messages arrive.
	time.Sleep(200 * time.Millisecond)

	if got := count.Load(); got != 5 {
		t.Errorf("received %d messages, want exactly 5 (post-subscribe)", got)
	}
}

// TestJetStreamRouter_SubscribeExclusiveFromTimestamp verifies timestamp-based
// delivery: only the message published after the recorded timestamp is received.
func TestJetStreamRouter_SubscribeExclusiveFromTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping from-timestamp test in -short mode")
	}

	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	const topic = "tb::ws::impl"
	const consumerName = "ts-consumer"
	ctx := context.Background()

	// Publish first message.
	if err := r.Publish(ctx, topic, []byte("first")); err != nil {
		t.Fatalf("Publish first: %v", err)
	}

	// Wait to create a clear time boundary.
	time.Sleep(200 * time.Millisecond)
	tsMs := time.Now().UnixMilli()
	time.Sleep(100 * time.Millisecond)

	// Publish second message after the timestamp.
	if err := r.Publish(ctx, topic, []byte("second")); err != nil {
		t.Fatalf("Publish second: %v", err)
	}

	received := make(chan string, 10)
	unsub, err := r.SubscribeExclusiveFromTimestamp(topic, consumerName, tsMs, func(data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		received <- string(cp)
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromTimestamp: %v", err)
	}
	defer unsub()

	select {
	case msg := <-received:
		if msg != "second" {
			t.Errorf("expected %q, got %q", "second", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for timestamped message")
	}

	// Ensure "first" did not arrive.
	select {
	case extra := <-received:
		t.Errorf("unexpected extra message: %q", extra)
	case <-time.After(200 * time.Millisecond):
		// Good — no extra messages.
	}
}

// TestJetStreamRouter_DurableConsumer_WithAetherIdentityName_Succeeds verifies
// that a consumerName containing characters NATS rejects in durable names
// (':', '@') is escaped via natscodec.EscapeForConsumerName and the subscribe
// + publish round-trip succeeds. Regression test for the bug where raw aether
// identity strings caused `nats: invalid consumer name` errors.
func TestJetStreamRouter_DurableConsumer_WithAetherIdentityName_Succeeds(t *testing.T) {
	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	const topic = "us::user-42::win-1"
	const consumerName = "us::user@example.com::win-1"
	const payload = "identity-name-payload"

	received := make(chan []byte, 1)
	unsub, err := r.SubscribeExclusive(topic, consumerName, func(data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		received <- cp
	})
	if err != nil {
		t.Fatalf("SubscribeExclusive with aether-identity consumer name: %v", err)
	}
	defer unsub()

	time.Sleep(50 * time.Millisecond)

	if err := r.Publish(context.Background(), topic, []byte(payload)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-received:
		if string(got) != payload {
			t.Errorf("payload mismatch: got %q, want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message via durable consumer with aether identity name")
	}
}

// TestJetStreamRouter_DurableConsumer_ReconnectResumes proves that an escaped
// aether-identity consumerName is stable across reconnects: publishing batch1
// + draining all of it + publishing batch2 + re-subscribing with the SAME
// raw aether consumer name must deliver only batch2 (i.e. JetStream stored
// the consumer offset under the same escaped durable name).
func TestJetStreamRouter_DurableConsumer_ReconnectResumes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping reconnect-resume test in -short mode")
	}

	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	const topic = "tu::ws::com.example.task::reconnect-test"
	// Aether identity form — contains ':' which NATS rejects as a raw consumer
	// name. The router must escape it deterministically so reconnects map to
	// the same durable consumer.
	const consumerName = "us::a::b"
	const batch1 = 2
	const batch2 = 3

	ctx := context.Background()

	// Publish batch1 BEFORE subscribing.
	for i := 0; i < batch1; i++ {
		if err := r.Publish(ctx, topic, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Publish batch1[%d]: %v", i, err)
		}
	}

	// First subscription: drain all of batch1, then cancel.
	var (
		mu1       sync.Mutex
		received1 []string
		done1     = make(chan struct{})
		once1     sync.Once
	)
	unsub1, err := r.SubscribeExclusive(topic, consumerName, func(data []byte) {
		mu1.Lock()
		defer mu1.Unlock()
		received1 = append(received1, string(data))
		if len(received1) >= batch1 {
			once1.Do(func() { close(done1) })
		}
	})
	if err != nil {
		t.Fatalf("first SubscribeExclusive: %v", err)
	}

	select {
	case <-done1:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first batch (2 messages)")
	}
	// Give NATS a moment to record the ack offset before cancelling.
	time.Sleep(150 * time.Millisecond)
	unsub1()

	// Publish batch2 AFTER the first subscription is cancelled.
	for i := batch1; i < batch1+batch2; i++ {
		if err := r.Publish(ctx, topic, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Publish batch2[%d]: %v", i, err)
		}
	}

	// Second subscription: same consumerName → must resume and only see batch2.
	var (
		mu2       sync.Mutex
		received2 []string
		done2     = make(chan struct{})
		once2     sync.Once
	)
	unsub2, err := r.SubscribeExclusive(topic, consumerName, func(data []byte) {
		mu2.Lock()
		defer mu2.Unlock()
		received2 = append(received2, string(data))
		if len(received2) >= batch2 {
			once2.Do(func() { close(done2) })
		}
	})
	if err != nil {
		t.Fatalf("second SubscribeExclusive: %v", err)
	}
	defer unsub2()

	select {
	case <-done2:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for remaining messages on reconnect")
	}

	// Briefly wait to ensure no extra (already-acked) messages leak in.
	time.Sleep(200 * time.Millisecond)

	mu2.Lock()
	got2 := append([]string(nil), received2...)
	mu2.Unlock()

	if len(got2) != batch2 {
		t.Errorf("reconnect received %d messages, want exactly %d", len(got2), batch2)
	}
	// None of the already-acked batch1 messages must reappear on the resumed
	// durable consumer — that's the offset-resumption guarantee.
	for _, m := range got2 {
		if m == "msg-0" || m == "msg-1" {
			t.Errorf("reconnect delivered already-acked message %q — offset NOT resumed", m)
		}
	}
}

// TestJetStreamRouter_UnknownPrefix_Error verifies that publishing to a topic
// with no registered stream prefix returns an error.
func TestJetStreamRouter_UnknownPrefix_Error(t *testing.T) {
	r, cleanup := newTestJetStreamRouter(t)
	defer cleanup()

	err := r.Publish(context.Background(), "nonsense::foo", []byte("data"))
	if err == nil {
		t.Fatal("expected error publishing to unknown prefix, got nil")
	}
}
