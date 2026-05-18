package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/router"
)

// TestClusterIntegration_MessageRouting_CrossNode publishes a message via the
// JetStreamRouter on node-0 and asserts that durable subscribers on node-1 AND
// node-2 (with distinct consumer names so JetStream treats them as independent
// subscriptions) both observe the message. This is the multi-node fan-out
// invariant that LimitsPolicy streams provide.
func TestClusterIntegration_MessageRouting_CrossNode(t *testing.T) {
	c := setupCluster3(t)

	// Use the JetStream context for each node's router. Replicas=3 ensures
	// the stream actually replicates across the cluster (otherwise a
	// single-replica stream pinned to one node would short-circuit the
	// cross-node assertion).
	rA, err := router.NewJetStreamRouter(c.Node(0).JetStream(), 3, nil)
	if err != nil {
		t.Fatalf("router on node-a: %v", err)
	}
	rB, err := router.NewJetStreamRouter(c.Node(1).JetStream(), 3, nil)
	if err != nil {
		t.Fatalf("router on node-b: %v", err)
	}
	rC, err := router.NewJetStreamRouter(c.Node(2).JetStream(), 3, nil)
	if err != nil {
		t.Fatalf("router on node-c: %v", err)
	}

	// Topic shape mirrors what the agent principal would publish under:
	//   ag::{workspace}::{impl}::{spec}
	const topic = "ag::ws::com.example.svc::v1"

	// Each subscriber uses a unique durable name so JetStream creates
	// independent consumers (otherwise WorkQueuePolicy-style sharing would
	// load-balance instead of fan out). The "ag" stream is LimitsPolicy so
	// independent durables each get their own copy.
	gotB := make(chan []byte, 1)
	gotC := make(chan []byte, 1)

	cancelB, err := rB.SubscribeExclusiveFromNow(topic, "ag-cluster-sub-b", func(p []byte) {
		// Non-blocking send: tests use buffered channels and only expect
		// one message.
		select {
		case gotB <- p:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe on B: %v", err)
	}
	defer cancelB()

	cancelC, err := rC.SubscribeExclusiveFromNow(topic, "ag-cluster-sub-c", func(p []byte) {
		select {
		case gotC <- p:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe on C: %v", err)
	}
	defer cancelC()

	// Allow consumer creation to propagate to all replicas. NATS push
	// consumers start delivering immediately once registered with the
	// stream leader, but on a freshly-created 3-replica stream there is a
	// small window where the consumer may not yet be visible from the
	// publish path.
	time.Sleep(500 * time.Millisecond)

	payload := []byte("cross-node-ping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rA.Publish(ctx, topic, payload); err != nil {
		t.Fatalf("publish on A: %v", err)
	}

	// Both subscribers must receive within a generous timeout.
	timeout := time.After(3 * time.Second)
	receivedB, receivedC := false, false
	for !receivedB || !receivedC {
		select {
		case got := <-gotB:
			if string(got) != string(payload) {
				t.Fatalf("B got %q, want %q", got, payload)
			}
			receivedB = true
		case got := <-gotC:
			if string(got) != string(payload) {
				t.Fatalf("C got %q, want %q", got, payload)
			}
			receivedC = true
		case <-timeout:
			t.Fatalf("timeout: receivedB=%v receivedC=%v", receivedB, receivedC)
		}
	}
}

// TestClusterIntegration_MessageRouting_OffsetAdvance publishes a batch of
// messages, durably consumes them all, tears the subscriber down, then
// publishes a SECOND batch on the same topic and re-subscribes with the same
// durable name. The second subscription must see exactly the new messages —
// not redeliveries of the first batch. This verifies that the JetStream
// durable consumer's offset/ack model preserves consumption progress across
// reconnect, which is the core property that makes durable consumers safe to
// restart.
//
// We deliberately do NOT structure this as "publish 100, consume 50, cancel,
// consume the other 50": the JetStream router's SubscribeExclusive ACKs after
// the handler returns and the iterator pre-fetches messages from the server,
// so a mid-stream cancel races against pre-fetched-but-unprocessed messages
// in a way that depends on the consumer's AckWait (30s default) timer. Two-
// batch sequencing avoids the race entirely.
func TestClusterIntegration_MessageRouting_OffsetAdvance(t *testing.T) {
	c := setupCluster3(t)

	rA, err := router.NewJetStreamRouter(c.Node(0).JetStream(), 3, nil)
	if err != nil {
		t.Fatalf("router on node-a: %v", err)
	}
	rB, err := router.NewJetStreamRouter(c.Node(1).JetStream(), 3, nil)
	if err != nil {
		t.Fatalf("router on node-b: %v", err)
	}

	const topic = "ag::ws::offset.test::v1"
	const firstBatch = 30
	const secondBatch = 20
	const durableName = "ag-cluster-offset-test"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Phase 1: publish first batch.
	for i := 0; i < firstBatch; i++ {
		if err := rA.Publish(ctx, topic, []byte(fmt.Sprintf("first-%03d", i))); err != nil {
			t.Fatalf("phase-1 publish %d: %v", i, err)
		}
	}

	// Phase 1: durable consume all `firstBatch` messages.
	firstMsgs := newCollectingChannel(firstBatch)
	cancelB1, err := rB.SubscribeExclusive(topic, durableName, func(p []byte) {
		firstMsgs.send(string(p))
	})
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	if !firstMsgs.waitForN(firstBatch, 15*time.Second) {
		cancelB1()
		t.Fatalf("phase-1 timeout: received %d of %d", firstMsgs.received(), firstBatch)
	}
	cancelB1()

	// Allow acks to settle and the durable consumer's state to commit to
	// the stream's meta-store before re-subscribing.
	time.Sleep(500 * time.Millisecond)

	// Phase 2: publish second batch.
	for i := 0; i < secondBatch; i++ {
		if err := rA.Publish(ctx, topic, []byte(fmt.Sprintf("second-%03d", i))); err != nil {
			t.Fatalf("phase-2 publish %d: %v", i, err)
		}
	}

	// Phase 2: re-subscribe with the same durable name. The server-stored
	// offset should already be past the first batch, so only the second
	// batch should arrive. We give it 5s to be sure no first-batch
	// redeliveries sneak in.
	secondMsgs := newCollectingChannel(firstBatch + secondBatch)
	cancelB2, err := rB.SubscribeExclusive(topic, durableName, func(p []byte) {
		secondMsgs.send(string(p))
	})
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}
	defer cancelB2()

	if !secondMsgs.waitForN(secondBatch, 10*time.Second) {
		t.Fatalf("phase-2 timeout: received %d of %d", secondMsgs.received(), secondBatch)
	}

	// Extra dwell: ensure no first-batch redeliveries arrive AFTER hitting
	// the expected secondBatch count. 1s is plenty — if the server were
	// going to redeliver, it would do so within tens of ms once the
	// consumer reconnects.
	time.Sleep(1 * time.Second)

	got := secondMsgs.snapshot()
	if len(got) > secondBatch {
		// We saw more than the second batch — first-batch messages
		// must have leaked through.
		extras := make([]string, 0, len(got)-secondBatch)
		for _, m := range got {
			if !startsWith(m, "second-") {
				extras = append(extras, m)
			}
		}
		t.Fatalf("durable offset did not advance: phase-2 saw %d messages (expected %d). leaked: %v",
			len(got), secondBatch, extras)
	}
	for _, m := range got {
		if !startsWith(m, "second-") {
			t.Errorf("phase-2 received unexpected first-batch message %q (durable offset regression)", m)
		}
	}
}

// collectingChannel is a tiny thread-safe receiver helper used by the offset-
// advance test. Splitting it out keeps the test body focused on the message-
// routing semantics under test.
type collectingChannel struct {
	mu   sync.Mutex
	msgs []string
	cap  int
	ch   chan struct{}
}

func newCollectingChannel(capHint int) *collectingChannel {
	return &collectingChannel{
		msgs: make([]string, 0, capHint),
		cap:  capHint,
		ch:   make(chan struct{}, 1),
	}
}

func (c *collectingChannel) send(m string) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
}

func (c *collectingChannel) received() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func (c *collectingChannel) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.msgs))
	copy(out, c.msgs)
	return out
}

func (c *collectingChannel) waitForN(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.received() >= n {
			return true
		}
		select {
		case <-c.ch:
		case <-time.After(50 * time.Millisecond):
		}
	}
	return c.received() >= n
}

// startsWith reports whether s begins with prefix.
func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
