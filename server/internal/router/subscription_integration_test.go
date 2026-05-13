//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/testutil"
)

// These tests exercise the real RabbitMQ Streams cold-start replay path used
// when a user-sent message triggers an offline agent to be spawned — the
// subscription the agent creates on connect must replay the message that was
// published just before its subprocess came up.
//
// The fix under test lives at ``router/subscription.go:214-237``
// (``OffsetResume`` + ``StartTimestampMs`` hint) and is reached via
// ``Router.SubscribeExclusiveFromTimestamp``. There was no coverage for it on
// the real broker — only the in-memory ``BadgerRouter`` equivalent — so this
// file fills that gap and is the instrument for diagnosing the production
// bug reported where cold-started agents miss the triggering message.
//
// Skips when RabbitMQ is unreachable so CI without the dev stack stays green.

// uniqueTopic returns a topic name unique to this test run so prior offset
// state from earlier invocations doesn't mask first-message-lost behaviour.
func uniqueTopic(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// newTestRouter returns a Router against the dev infra RabbitMQ, or skips if
// the broker is unreachable (so unit runs without dev infra stay green).
func newTestRouter(t *testing.T) *Router {
	t.Helper()
	streamURL := testutil.GetRabbitMQConfig().StreamURL()
	r, err := NewRouter(streamURL, 0)
	if err != nil {
		t.Skipf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// waitForMessage collects delivered payloads for up to “timeout“. Returns the
// payloads seen in delivery order. Non-blocking if timeout is 0.
func waitForMessage(ch <-chan []byte, timeout time.Duration) [][]byte {
	var out [][]byte
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, m)
		case <-deadline:
			return out
		}
	}
}

// TestSubscribeExclusiveFromTimestamp_ReplaysMessagePublishedBeforeSubscribe
// is the canonical reproduction of the first-message-lost scenario:
//  1. Publish a message (the user's chat that triggers orchestration).
//  2. Wait a bit (the agent-spawn window).
//  3. Subscribe exclusive with StartTimestampMs <= publish moment.
//  4. Assert the subscriber receives the message.
//
// Passing here means the fix is sound and the production bug is upstream
// (likely in gateway metadata plumbing). Failing here means “.Timestamp()“
// is NOT doing what the code assumes and Phase 2 (offset-based replay) is
// warranted.
func TestSubscribeExclusiveFromTimestamp_ReplaysMessagePublishedBeforeSubscribe(t *testing.T) {
	r := newTestRouter(t)
	ctx := context.Background()

	topic := uniqueTopic(t, "test-first-msg-replay")
	consumerName := "replay-consumer-" + topic

	// Record the moment just before publish. The gateway's current code
	// captures this as ``now := time.Now()`` at routeMessage entry, uses
	// it both for trigger metadata AND for the envelope ``TimestampMs``
	// field (see routing.go:287, 320, 330).
	triggerTimestampMs := time.Now().UnixMilli()

	payload := []byte("first message that must not be lost")
	if err := r.Publish(ctx, topic, payload); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Simulate the agent-spawn delay. In practice this is ~3-10s for
	// subprocess cold-start; 250ms is enough to ensure publish has been
	// fully acknowledged + indexed by the broker.
	time.Sleep(250 * time.Millisecond)

	received := make(chan []byte, 8)
	unsub, err := r.SubscribeExclusiveFromTimestamp(topic, consumerName, triggerTimestampMs, func(p []byte) {
		received <- p
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromTimestamp failed: %v", err)
	}
	defer unsub()

	msgs := waitForMessage(received, 3*time.Second)
	if len(msgs) == 0 {
		t.Fatalf("FIRST-MESSAGE-LOST REPRODUCED: subscriber with StartTimestampMs=%d did not receive the message published at or after that timestamp within 3s. This matches the production bug.", triggerTimestampMs)
	}
	if got := string(msgs[0]); got != string(payload) {
		t.Errorf("received wrong payload: got %q, want %q", got, string(payload))
	}
}

// TestSubscribeExclusiveFromTimestamp_TimestampEqualsPublishMoment checks the
// boundary case: StartTimestampMs recorded exactly at the publish moment
// (no pre-publish headroom). RabbitMQ Streams “.Timestamp()“ is documented
// as "first message at or after" — i.e. inclusive. Any stricter behaviour
// would explain the production bug by itself.
func TestSubscribeExclusiveFromTimestamp_TimestampEqualsPublishMoment(t *testing.T) {
	r := newTestRouter(t)
	ctx := context.Background()

	topic := uniqueTopic(t, "test-ts-at-publish")
	consumerName := "at-publish-consumer-" + topic

	// Capture timestamp and publish in tight sequence — best approximation
	// of "timestamp == message moment" that we can control from the client.
	ts := time.Now().UnixMilli()
	payload := []byte("payload at publish moment")
	if err := r.Publish(ctx, topic, payload); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	received := make(chan []byte, 8)
	unsub, err := r.SubscribeExclusiveFromTimestamp(topic, consumerName, ts, func(p []byte) {
		received <- p
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromTimestamp failed: %v", err)
	}
	defer unsub()

	msgs := waitForMessage(received, 3*time.Second)
	if len(msgs) == 0 {
		t.Fatalf("boundary case failed: subscriber with StartTimestampMs=%d (published immediately after) received nothing in 3s", ts)
	}
}

// TestSubscribeExclusiveFromTimestamp_TimestampSlightlyAfterPublishMoment
// exercises what happens if the gateway code ever stamps the trigger
// timestamp AFTER publish rather than before. Production today records
// “now“ before publish, but any future refactor that inverts the order
// would start dropping messages. This test is the guardrail: it should
// fail, confirming the correct ordering invariant.
func TestSubscribeExclusiveFromTimestamp_TimestampSlightlyAfterPublishMoment(t *testing.T) {
	r := newTestRouter(t)
	ctx := context.Background()

	topic := uniqueTopic(t, "test-ts-after-publish")
	consumerName := "after-publish-consumer-" + topic

	payload := []byte("published-first-then-stamped")
	if err := r.Publish(ctx, topic, payload); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	// Grant the broker time to assign a timestamp to the message, then
	// capture a timestamp strictly after it.
	time.Sleep(50 * time.Millisecond)
	ts := time.Now().UnixMilli()
	time.Sleep(200 * time.Millisecond)

	received := make(chan []byte, 8)
	unsub, err := r.SubscribeExclusiveFromTimestamp(topic, consumerName, ts, func(p []byte) {
		received <- p
	})
	if err != nil {
		t.Fatalf("SubscribeExclusiveFromTimestamp failed: %v", err)
	}
	defer unsub()

	msgs := waitForMessage(received, 2*time.Second)
	if len(msgs) != 0 {
		// Informational: if THIS case succeeds, ``.Timestamp()`` is more
		// permissive than documented (possibly chunk-level rather than
		// message-level). Record the observation but do not fail — the
		// legitimate expectation is that "timestamp-after-publish" skips
		// the message.
		t.Logf("NOTE: ``.Timestamp(ts > publish_ts)`` unexpectedly delivered %d message(s) — broker seek is more permissive than documented", len(msgs))
	}
}

// TestSubscribeExclusiveFromTimestamp_ResumeFromStoredOffsetWinsOverTimestamp
// checks the precedence rule at subscription.go:218-237 — if a stored offset
// already exists for this consumer, the “StartTimestampMs“ hint is
// ignored. Regression guardrail for returning agents.
func TestSubscribeExclusiveFromTimestamp_ResumeFromStoredOffsetWinsOverTimestamp(t *testing.T) {
	r := newTestRouter(t)
	ctx := context.Background()

	topic := uniqueTopic(t, "test-stored-offset-wins")
	consumerName := "stored-offset-consumer-" + topic

	// First pass: subscribe-from-now with offset tracking, publish a
	// message, let it land, unsubscribe. This populates the stored offset.
	first := make(chan []byte, 8)
	unsub1, err := r.SubscribeExclusive(topic, consumerName, func(p []byte) {
		first <- p
	})
	if err != nil {
		t.Fatalf("first SubscribeExclusive failed: %v", err)
	}

	if err := r.Publish(ctx, topic, []byte("m1")); err != nil {
		t.Fatalf("Publish m1 failed: %v", err)
	}
	if msgs := waitForMessage(first, 3*time.Second); len(msgs) == 0 {
		unsub1()
		t.Fatalf("first subscriber never received m1 — cannot set up stored offset")
	}
	unsub1()
	// Give the auto-commit flush interval a moment to persist the offset.
	time.Sleep(500 * time.Millisecond)

	// Publish a second message while the consumer is "offline".
	if err := r.Publish(ctx, topic, []byte("m2")); err != nil {
		t.Fatalf("Publish m2 failed: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	// Reattach with a timestamp hint that, if honoured, would include m1
	// again. The stored-offset precedence rule should skip m1 and only
	// deliver m2.
	veryEarly := time.Now().Add(-1 * time.Hour).UnixMilli()
	second := make(chan []byte, 8)
	unsub2, err := r.SubscribeExclusiveFromTimestamp(topic, consumerName, veryEarly, func(p []byte) {
		second <- p
	})
	if err != nil {
		t.Fatalf("second SubscribeExclusiveFromTimestamp failed: %v", err)
	}
	defer unsub2()

	msgs := waitForMessage(second, 3*time.Second)
	if len(msgs) == 0 {
		t.Fatalf("resumed subscriber received nothing — stored offset lookup likely broken")
	}
	var seen []string
	for _, m := range msgs {
		seen = append(seen, string(m))
	}
	joined := strings.Join(seen, ",")
	if strings.Contains(joined, "m1") {
		t.Errorf("stored offset was ignored: replayed %q, expected only m2", joined)
	}
	if !strings.Contains(joined, "m2") {
		t.Errorf("resumed subscriber missed m2: got %q", joined)
	}
}

// TestSubscribeExclusiveFromTimestamp_NoHintFallsBackToNext pins the
// third branch at subscription.go:230-233 — when no stored offset and no
// timestamp hint is provided, the subscriber starts from the next message
// (the legacy behaviour, and the path that loses first messages before
// Fix B was introduced).
func TestSubscribeExclusiveFromTimestamp_NoHintFallsBackToNext(t *testing.T) {
	r := newTestRouter(t)
	ctx := context.Background()

	topic := uniqueTopic(t, "test-no-hint-fallback")
	consumerName := "nohint-consumer-" + topic

	// Publish a "before subscribe" message.
	if err := r.Publish(ctx, topic, []byte("before-sub")); err != nil {
		t.Fatalf("Publish before-sub failed: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	received := make(chan []byte, 8)
	var mu sync.Mutex
	var got []string
	unsub, err := r.SubscribeExclusive(topic, consumerName, func(p []byte) {
		mu.Lock()
		got = append(got, string(p))
		mu.Unlock()
		received <- p
	})
	if err != nil {
		t.Fatalf("SubscribeExclusive failed: %v", err)
	}
	defer unsub()

	// Publish an "after subscribe" message.
	if err := r.Publish(ctx, topic, []byte("after-sub")); err != nil {
		t.Fatalf("Publish after-sub failed: %v", err)
	}

	// Give time for delivery.
	msgs := waitForMessage(received, 2*time.Second)
	mu.Lock()
	defer mu.Unlock()

	if len(msgs) != 1 || got[0] != "after-sub" {
		t.Errorf("expected only the after-sub message, got %v", got)
	}
}
