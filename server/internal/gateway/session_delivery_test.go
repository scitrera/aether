package gateway

// Tests for ClientSession.Deliver() / DeliverWithPriority() and startDeliveryLoop():
//   - Deliver enqueues messages when buffer has space
//   - Deliver drops messages (no block) when buffer is full
//   - startDeliveryLoop forwards messages from channel to stream via SafeSend
//   - startDeliveryLoop drains buffered messages after context cancellation
//   - startDeliveryLoop exits cleanly when context is cancelled and buffer empty
//   - DeliverWithPriority sheds lower priority before control when Semaphore-bound

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/sdk/go/aether"
	bp "github.com/scitrera/go-backpressure"
)

// newDeliveryClient creates a ClientSession with a delivery channel of the
// given buffer size, wired to the provided mockStream. No Semaphore — uses
// the legacy non-blocking select-default behavior for backwards-compat tests.
func newDeliveryClient(stream *mockStream, bufSize int) *ClientSession {
	return &ClientSession{
		ID: "delivery-test-session",
		Identity: models.Identity{
			Type:      models.PrincipalAgent,
			Workspace: "ws1",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
		deliveryCh:    make(chan *pb.DownstreamMessage, bufSize),
	}
}

// newDeliveryClientWithSem creates a ClientSession backed by a Semaphore
// with the supplied capacity and CoDel target/interval. Used by tests that
// exercise the priority-shed path.
func newDeliveryClientWithSem(stream *mockStream, bufSize, semCap int, target, interval time.Duration) *ClientSession {
	sem := bp.NewSemaphore(
		deliveryPriorityCount,
		semCap,
		bp.SemaphoreShortTimeout(target),
		bp.SemaphoreLongTimeout(interval),
	)
	return &ClientSession{
		ID: "delivery-prio-test-session",
		Identity: models.Identity{
			Type:      models.PrincipalAgent,
			Workspace: "ws1",
		},
		Stream:                stream,
		subscriptions:         make(map[string]func()),
		deliveryCh:            make(chan *pb.DownstreamMessage, bufSize),
		deliverySem:           sem,
		deliverAcquireTimeout: 200 * time.Millisecond,
	}
}

// ---------------------------------------------------------------------------
// Deliver – buffer with space
// ---------------------------------------------------------------------------

func TestDeliver_BufferNotFull_MessageEnqueued(t *testing.T) {
	stream := &mockStream{}
	client := newDeliveryClient(stream, 10)

	msg := &pb.DownstreamMessage{}
	client.Deliver(msg)

	if len(client.deliveryCh) != 1 {
		t.Errorf("expected 1 message in delivery channel, got %d", len(client.deliveryCh))
	}
}

func TestDeliver_MultipleMessages_AllEnqueued(t *testing.T) {
	stream := &mockStream{}
	client := newDeliveryClient(stream, 10)

	for i := 0; i < 5; i++ {
		client.Deliver(&pb.DownstreamMessage{})
	}

	if len(client.deliveryCh) != 5 {
		t.Errorf("expected 5 messages in delivery channel, got %d", len(client.deliveryCh))
	}
}

// ---------------------------------------------------------------------------
// Deliver – buffer full (drop without blocking)
// ---------------------------------------------------------------------------

func TestDeliver_BufferFull_MessageDroppedWithoutBlocking(t *testing.T) {
	stream := &mockStream{}
	// Buffer of size 1: fill it, then try one more.
	client := newDeliveryClient(stream, 1)

	// Fill the buffer.
	client.Deliver(&pb.DownstreamMessage{})

	// This should not block and should drop the message.
	done := make(chan struct{})
	go func() {
		client.Deliver(&pb.DownstreamMessage{})
		close(done)
	}()

	select {
	case <-done:
		// Good – Deliver returned without blocking.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Deliver blocked on full buffer; expected non-blocking drop")
	}

	// Buffer should still contain only the original 1 message.
	if len(client.deliveryCh) != 1 {
		t.Errorf("expected 1 message in buffer after drop, got %d", len(client.deliveryCh))
	}
}

func TestDeliver_BufferFull_CallerNotBlocked_ConcurrencySafe(t *testing.T) {
	// Confirm that many concurrent Deliver calls on a size-0 buffer never deadlock.
	stream := &mockStream{}
	client := newDeliveryClient(stream, 0)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			client.Deliver(&pb.DownstreamMessage{})
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed without blocking.
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent Deliver calls blocked; expected non-blocking drops on zero-size buffer")
	}
}

// ---------------------------------------------------------------------------
// startDeliveryLoop – forwards messages to stream
// ---------------------------------------------------------------------------

func TestStartDeliveryLoop_MessagesForwardedToStream(t *testing.T) {
	stream := &mockStream{}
	client := newDeliveryClient(stream, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.startDeliveryLoop(ctx)

	// Deliver a few messages and let the loop drain them.
	const n = 5
	for i := 0; i < n; i++ {
		client.Deliver(&pb.DownstreamMessage{})
	}

	// Poll until all messages arrive or timeout.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream.sentCount() == n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if stream.sentCount() != n {
		t.Errorf("expected %d messages forwarded to stream, got %d", n, stream.sentCount())
	}
}

// ---------------------------------------------------------------------------
// startDeliveryLoop – drains buffer after context cancellation
// ---------------------------------------------------------------------------

func TestStartDeliveryLoop_ContextCancelled_DrainedBeforeExit(t *testing.T) {
	stream := &mockStream{}
	client := newDeliveryClient(stream, 20)

	ctx, cancel := context.WithCancel(context.Background())

	client.startDeliveryLoop(ctx)

	// Enqueue messages BEFORE cancelling so they sit in the buffer at cancel time.
	const n = 3
	for i := 0; i < n; i++ {
		client.Deliver(&pb.DownstreamMessage{})
	}

	// Allow the loop goroutine to start draining, then cancel.
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Give the drain goroutine time to flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream.sentCount() >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if stream.sentCount() < n {
		t.Errorf("expected at least %d messages drained after cancel, got %d", n, stream.sentCount())
	}
}

// ---------------------------------------------------------------------------
// startDeliveryLoop – exits cleanly when context cancelled with empty buffer
// ---------------------------------------------------------------------------

func TestStartDeliveryLoop_EmptyBufferOnCancel_NoMessagesSent(t *testing.T) {
	stream := &mockStream{}
	client := newDeliveryClient(stream, 10)

	ctx, cancel := context.WithCancel(context.Background())
	client.startDeliveryLoop(ctx)

	// Cancel immediately with nothing in the buffer.
	cancel()

	// Give the loop goroutine time to exit.
	time.Sleep(50 * time.Millisecond)

	if stream.sentCount() != 0 {
		t.Errorf("expected 0 messages sent when buffer empty at cancel, got %d", stream.sentCount())
	}
}

// ---------------------------------------------------------------------------
// DeliverWithPriority – sheds lower priorities first under saturation
// ---------------------------------------------------------------------------

// countBackpressureNotices returns the number of BACKPRESSURE error notices
// observed on the staging channel without draining other messages.
func countBackpressureNotices(ch <-chan *pb.DownstreamMessage, deadline time.Time) int {
	notices := 0
	for time.Now().Before(deadline) {
		select {
		case msg := <-ch:
			if errPayload, ok := msg.Payload.(*pb.DownstreamMessage_Error); ok {
				if errPayload.Error.GetCode() == "BACKPRESSURE" {
					notices++
				}
			}
		case <-time.After(20 * time.Millisecond):
			return notices
		}
	}
	return notices
}

// TestDeliverWithPriority_ShedsLowerFirst configures a small-capacity
// Semaphore, holds a high-priority slot to keep the gate near saturation,
// then floods best-effort traffic. The expectation: control-priority sends
// admit cleanly while best-effort sends get shed and emit a BACKPRESSURE
// notice on the same session.
func TestDeliverWithPriority_ShedsLowerFirst(t *testing.T) {
	stream := &mockStream{}
	// Aggressive CoDel parameters so the test doesn't have to run for a
	// real-world second-scale interval. Capacity=1: every send must drain
	// before the next can admit.
	client := newDeliveryClientWithSem(stream, 4, 1,
		5*time.Millisecond, 10*time.Millisecond)
	defer client.closeDeliverySemaphore()

	// Hold the only Semaphore slot at the highest priority for a long enough
	// window that subsequent best-effort acquires either queue and shed or
	// time out via the per-Deliver acquire timeout.
	holdAcquired := make(chan struct{})
	holdRelease := make(chan struct{})
	go func() {
		if err := client.deliverySem.Acquire(context.Background(), aether.PriorityControl, 1); err != nil {
			t.Errorf("hold acquire failed: %v", err)
			close(holdAcquired)
			return
		}
		close(holdAcquired)
		<-holdRelease
		client.deliverySem.Release(1)
	}()
	<-holdAcquired

	// Hammer best-effort with several concurrent sends. With capacity=1
	// and the high-prio holder still in flight, these should accumulate
	// in the CoDel queue and be shed within the long-timeout window.
	const bestEffortBlast = 12
	var wg sync.WaitGroup
	wg.Add(bestEffortBlast)
	for i := 0; i < bestEffortBlast; i++ {
		go func() {
			defer wg.Done()
			client.DeliverWithPriority(context.Background(), aether.PriorityBestEffort, &pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_Signal{
					Signal: &pb.Signal{Type: pb.Signal_GRACEFUL_DISCONNECT, Reason: "best-effort blast"},
				},
			})
		}()
	}
	wg.Wait()

	// At least some best-effort sends must have been shed and emitted a
	// BACKPRESSURE notice on the staging channel.
	deadline := time.Now().Add(500 * time.Millisecond)
	notices := countBackpressureNotices(client.deliveryCh, deadline)
	if notices == 0 {
		t.Fatalf("expected at least one BACKPRESSURE notice from shed best-effort sends, got 0")
	}

	// Release the high-prio holder so subsequent control sends can admit.
	close(holdRelease)

	// Give the Semaphore a brief moment to reflect the release in its
	// internal queues before the next admission attempt.
	time.Sleep(20 * time.Millisecond)

	// A control-priority Deliver must succeed cleanly once the holder is
	// gone. Drain the staging channel and assert the control envelope made
	// it through.
	controlMsg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Signal{
			Signal: &pb.Signal{Type: pb.Signal_GRACEFUL_DISCONNECT, Reason: "control envelope"},
		},
	}
	client.DeliverWithPriority(context.Background(), aether.PriorityControl, controlMsg)

	// Drain the staging channel for up to 250ms and look for the control
	// envelope's signature reason.
	foundControl := false
	drainDeadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(drainDeadline) {
		select {
		case msg := <-client.deliveryCh:
			if sig, ok := msg.Payload.(*pb.DownstreamMessage_Signal); ok {
				if sig.Signal.GetReason() == "control envelope" {
					foundControl = true
				}
			}
		case <-time.After(10 * time.Millisecond):
		}
		if foundControl {
			break
		}
	}
	if !foundControl {
		t.Errorf("expected control envelope to admit cleanly after holder released")
	}
}
