package gateway

// Tests for ClientSession.Deliver() and startDeliveryLoop():
//   - Deliver enqueues messages when buffer has space
//   - Deliver drops messages (no block) when buffer is full
//   - startDeliveryLoop forwards messages from channel to stream via SafeSend
//   - startDeliveryLoop drains buffered messages after context cancellation
//   - startDeliveryLoop exits cleanly when context is cancelled and buffer empty

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// newDeliveryClient creates a ClientSession with a delivery channel of the
// given buffer size, wired to the provided mockStream.
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
