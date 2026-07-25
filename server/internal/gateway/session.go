package gateway

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/logging"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/sdk/go/aether"
	bp "github.com/scitrera/go-backpressure"
	"golang.org/x/time/rate"
)

// defaultDeliveryBufferSize is the default number of downstream messages buffered per client.
// If the buffer fills up (slow client), incoming messages are dropped with a warning.
// Override via WithDeliveryBufferSize option on NewGatewayServer or gateway.delivery_buffer_size config.
const defaultDeliveryBufferSize = 256

// deliveryBufferSize is kept for backward compatibility with existing test helpers.
// Production code uses s.deliveryBufferSize (set from config or WithDeliveryBufferSize option).
const deliveryBufferSize = defaultDeliveryBufferSize

// Default per-client delivery-backpressure parameters. Override via
// WithDeliveryBackpressure(capacity, target, interval).
const (
	// defaultDeliveryBackpressureCapacity is the number of concurrent
	// admissions allowed into the per-session delivery path. The token is
	// held only for the duration of the push onto deliveryCh, so capacity
	// effectively measures admission concurrency rather than queue depth.
	defaultDeliveryBackpressureCapacity = 16

	// defaultDeliveryBackpressureTarget is the CoDel target latency. Sheds
	// when sustained acquire latency exceeds this.
	defaultDeliveryBackpressureTarget = 50 * time.Millisecond

	// defaultDeliveryBackpressureInterval is the CoDel sampling interval.
	defaultDeliveryBackpressureInterval = 100 * time.Millisecond

	// defaultDeliverAcquireTimeout caps the admission wait per Deliver call
	// so a wedged downstream consumer can't block the publisher forever.
	// Matches the SDK-side sendUpstreamTimeout used in the sidecar.
	defaultDeliverAcquireTimeout = 30 * time.Second
)

// deliveryPriorityCount is the number of priorities used by the per-session
// delivery Semaphore. Must be >= the highest aether.Priority constant used by
// callers + 1. See sdk/go/aether/priority.go.
const deliveryPriorityCount = 5

// ClientSession represents an active client connection to the gateway.
type ClientSession struct {
	ID               string
	SessionUUID      uuid.UUID
	Identity         models.Identity
	AssociatedTaskID string
	Stream           pb.AetherGateway_ConnectServer
	Cancel           context.CancelFunc // Used to forcibly disconnect the session
	ConnectedAt      time.Time          // When this session was established

	// sendMu protects concurrent Stream.Send() calls.
	// gRPC streams are not safe for concurrent writes.
	sendMu sync.Mutex

	// identityMu protects concurrent reads/writes of Identity fields (e.g. Workspace during SwitchWorkspace).
	identityMu sync.RWMutex

	// Subscription management for multi-topic subscriptions
	subscriptionsMu sync.RWMutex
	subscriptions   map[string]func() // topic -> unsubscribe function

	// Per-client message rate limiter
	rateLimiter *rate.Limiter

	// activePoolTasks tracks the number of pool tasks currently assigned to this client.
	// Used by power-of-two-choices load balancing for pool task assignment.
	activePoolTasks atomic.Int64

	// serverInitiatedDisconnect is set true by code paths where the gateway
	// tells the worker to leave (drain/shutdown via doStop, admin force-kick
	// via DisconnectSession). cleanupSession reads this to decide whether the
	// associated task should transition: if true, task is left in its current
	// state (the worker is expected to reconnect — possibly to another
	// gateway in the fleet — and pick up where it left off).
	serverInitiatedDisconnect atomic.Bool

	// orchestratorProfiles caches the profile names registered by this orchestrator at
	// connect time. Used by the orchestratorIndex for O(1) lookup and clean removal
	// without needing a DB query on disconnect. Only set for PrincipalOrchestrator sessions.
	orchestratorProfiles []string

	// deliveryCh buffers outbound messages so that a slow client cannot block
	// the shared fan-out goroutine. Messages are drained by startDeliveryLoop.
	// With the priority-aware admission queue this is a narrow staging buffer:
	// deliverySem (below) is the actual back-pressure gate; deliveryCh just
	// hands the admitted message to the delivery loop.
	deliveryCh chan *pb.DownstreamMessage

	// deliverySem is a CoDel-managed Semaphore that admits Deliver* callers
	// by priority and sheds low-priority traffic first when the gRPC writer
	// can't keep up. Nil-safe: when nil, Deliver* fall back to the legacy
	// non-blocking select-default behavior so existing test scaffolding that
	// constructs ClientSession by hand keeps working.
	deliverySem *bp.Semaphore

	// deliverAcquireTimeout caps the Semaphore.Acquire wait per Deliver call,
	// so a wedged consumer can't pin a Deliver caller forever. Zero falls
	// back to defaultDeliverAcquireTimeout.
	deliverAcquireTimeout time.Duration

	// sessionCtx is the session-lifetime context. Used to derive admission
	// deadlines for Deliver* calls (so admission unblocks promptly when the
	// session is closing) and for the delivery-loop. May be nil for unit
	// tests that hand-roll ClientSession.
	sessionCtx context.Context

	// activeExtensions captures the set of extension URIs (mapped to their
	// negotiated version, "" when unpinned) that the gateway agreed to on
	// the InitConnection handshake. Set by Connect()'s extension
	// negotiation step (Phase 6) before any user message is processed and
	// thereafter read-only — concurrent message handlers can read without
	// a lock. nil/empty when the client declared no extensions or the
	// server has nothing in KnownExtensions.
	activeExtensions map[string]string
}

// SafeSend sends a downstream message with mutex protection.
// gRPC streams are not safe for concurrent writes; this method
// serializes all sends to prevent data corruption.
func (c *ClientSession) SafeSend(msg *pb.DownstreamMessage) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.Stream.Send(msg)
}

// startDeliveryLoop drains c.deliveryCh and writes each message to the gRPC
// stream via SafeSend. It exits when ctx is cancelled (session disconnect) and
// drains any remaining buffered messages before returning.
func (c *ClientSession) startDeliveryLoop(ctx context.Context) {
	go func() {
		// Snapshot identity string for logging (avoids lock on hot path).
		c.identityMu.RLock()
		identStr := c.Identity.String()
		c.identityMu.RUnlock()

		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("identity", identStr).Msg("recovered from panic in delivery loop")
			}
		}()

		for {
			select {
			case msg := <-c.deliveryCh:
				if err := c.SafeSend(msg); err != nil {
					logging.Logger.Error().Err(err).Str("identity", identStr).Msg("delivery loop: error sending message")
				}
			case <-ctx.Done():
				// Drain any messages already in the buffer before exiting.
				for {
					select {
					case msg := <-c.deliveryCh:
						if err := c.SafeSend(msg); err != nil {
							logging.Logger.Debug().Err(err).Str("identity", identStr).Msg("delivery loop: error sending buffered message on shutdown")
							return
						}
					default:
						return
					}
				}
			}
		}
	}()
}

// Deliver enqueues a message for delivery to the client at the default
// PriorityRequest. It is a thin wrapper around DeliverWithPriority; callers
// that have a more specific priority signal (response header, control,
// chunk, best-effort) should call DeliverWithPriority directly so the
// CoDel-driven Semaphore sheds the right traffic first under sustained load.
//
// On admission failure (CoDel shed, ctx deadline, or full staging buffer
// with no Semaphore) the message is dropped, a warning is logged, and a
// BACKPRESSURE notice is emitted on the same session so the client can
// distinguish "your stream lost a frame" from a clean disconnect.
func (c *ClientSession) Deliver(msg *pb.DownstreamMessage) {
	c.DeliverWithPriority(c.deriveDeliverCtx(), aether.PriorityRequest, msg)
}

// DeliverWithPriority is the primary delivery path. It acquires one token
// from the per-session deliverySem at the given priority, then pushes the
// envelope onto the staging buffer drained by startDeliveryLoop. The token
// is released as soon as the envelope is handed off; Semaphore capacity
// therefore measures admission concurrency, not queue depth.
//
// On Acquire failure (CoDel-driven shed because sustained acquire latency
// exceeded target, or because ctx expired before admission), the BACKPRESSURE
// notice path fires so the receiving SDK can surface a retryable signal
// rather than seeing a silently truncated stream. When deliverySem is nil
// (hand-rolled test ClientSession) we fall back to a non-blocking enqueue
// matching the legacy Deliver semantics so test scaffolding keeps working.
func (c *ClientSession) DeliverWithPriority(ctx context.Context, prio bp.Priority, msg *pb.DownstreamMessage) {
	if c.deliverySem == nil {
		// Legacy / hand-rolled session (test scaffolding). Mirror the
		// historical non-blocking select-default behaviour.
		select {
		case c.deliveryCh <- msg:
		default:
			c.emitBackpressureNotice()
		}
		return
	}

	timeout := c.deliverAcquireTimeout
	if timeout <= 0 {
		timeout = defaultDeliverAcquireTimeout
	}

	parent := ctx
	if parent == nil {
		parent = context.Background()
	}
	acquireCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	if err := c.deliverySem.Acquire(acquireCtx, prio, 1); err != nil {
		c.handleDeliverShed(prio, err)
		return
	}
	// Non-blocking enqueue with immediate token release. Holding the
	// Semaphore token while waiting on a blocked deliveryCh push (the
	// previous shape) caused tokens to wedge whenever the gRPC consumer
	// fell behind: held tokens exhausted the Semaphore, every subsequent
	// Acquire CoDel-shed, and the BACKPRESSURE notice hit the same full
	// channel and got silently dropped. Releasing immediately after the
	// push attempt keeps the Semaphore healthy and gives CoDel a clean
	// dwell-time signal driven by Acquire contention.
	pushed := false
	select {
	case c.deliveryCh <- msg:
		pushed = true
	default:
		// Staging buffer full despite admission — treat as shed.
	}
	c.deliverySem.Release(1)
	if !pushed {
		c.handleDeliverShed(prio, nil)
	}
}

// deriveDeliverCtx returns the parent context used by Deliver's
// admission-deadline derivation. We prefer the session-lifetime ctx so
// admission unblocks promptly when the session is shutting down, but fall
// back to a background ctx for hand-rolled test sessions that never wire
// sessionCtx.
func (c *ClientSession) deriveDeliverCtx() context.Context {
	if c.sessionCtx != nil {
		return c.sessionCtx
	}
	return context.Background()
}

// handleDeliverShed logs the shed and emits a BACKPRESSURE notice on the
// session. Used by both the Semaphore-driven and staging-buffer paths so
// the observable behaviour is identical.
func (c *ClientSession) handleDeliverShed(prio bp.Priority, cause error) {
	c.identityMu.RLock()
	identStr := c.Identity.String()
	c.identityMu.RUnlock()
	// Don't spam at warn level for genuine context cancellation (session
	// teardown) — that path is expected and already audited elsewhere.
	if cause != nil && errors.Is(cause, context.Canceled) {
		logging.Logger.Debug().Err(cause).Str("identity", identStr).Int("priority", int(prio)).Msg("delivery shed: session context cancelled")
	} else {
		logging.Logger.Warn().Err(cause).Str("identity", identStr).Int("priority", int(prio)).Msg("delivery shed by backpressure; dropping message")
	}
	c.emitBackpressureNotice()
}

// emitBackpressureNotice writes a single BACKPRESSURE DownstreamMessage_Error
// onto the staging buffer if there is room. This preserves the original
// drop-on-full semantics for the notice itself: a severely backed-up consumer
// will silently lose the notice rather than block the publisher.
func (c *ClientSession) emitBackpressureNotice() {
	backpressureNotice := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: &pb.ErrorResponse{
				Code:    "BACKPRESSURE",
				Message: "delivery buffer full — messages are being dropped; consider reducing send rate or processing messages faster",
			},
		},
	}
	select {
	case c.deliveryCh <- backpressureNotice:
	default:
		// Buffer still full — client is severely behind, notice also dropped.
	}
}

// closeDeliverySemaphore releases the Semaphore's background reaper. Safe to
// call on a session that never had a Semaphore.
func (c *ClientSession) closeDeliverySemaphore() {
	if c.deliverySem != nil {
		c.deliverySem.Close()
	}
}

// AddSubscription adds a topic subscription and its unsubscribe function
func (c *ClientSession) AddSubscription(topic string, unsubscribe func()) {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	if c.subscriptions == nil {
		c.subscriptions = make(map[string]func())
	}
	c.subscriptions[topic] = unsubscribe
}

// RemoveSubscription unsubscribes from a topic
func (c *ClientSession) RemoveSubscription(topic string) {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	if unsubscribe, exists := c.subscriptions[topic]; exists {
		unsubscribe()
		delete(c.subscriptions, topic)
	}
}

// UnsubscribeAll unsubscribes from all topics
func (c *ClientSession) UnsubscribeAll() {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	for topic, unsubscribe := range c.subscriptions {
		unsubscribe()
		delete(c.subscriptions, topic)
	}
}

// HasSubscription checks if a topic subscription exists
func (c *ClientSession) HasSubscription(topic string) bool {
	c.subscriptionsMu.RLock()
	defer c.subscriptionsMu.RUnlock()
	_, exists := c.subscriptions[topic]
	return exists
}

// connectionState holds shared state passed between Connect() helper methods.
type connectionState struct {
	identity         models.Identity
	sessionID        string
	sessionCtx       context.Context
	sessionCancel    context.CancelFunc
	associatedTaskID string
	resumed          bool
	// Session-lifetime fields populated by acquireSessionLock from the
	// session registry's ConnectResult, so the connect handler can echo
	// them on ConnectionAck and the connection-established audit row.
	initialConnectionUnixMs int64
	reconnectionCount       int32
	client                  *ClientSession
}
