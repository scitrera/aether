package gateway

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
	"golang.org/x/time/rate"
)

// defaultDeliveryBufferSize is the default number of downstream messages buffered per client.
// If the buffer fills up (slow client), incoming messages are dropped with a warning.
// Override via WithDeliveryBufferSize option on NewGatewayServer or gateway.delivery_buffer_size config.
const defaultDeliveryBufferSize = 256

// deliveryBufferSize is kept for backward compatibility with existing test helpers.
// Production code uses s.deliveryBufferSize (set from config or WithDeliveryBufferSize option).
const deliveryBufferSize = defaultDeliveryBufferSize

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
	deliveryCh chan *pb.DownstreamMessage

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

// Deliver enqueues a message for delivery to the client. If the delivery buffer
// is full the message is dropped, a warning is logged, and a backpressure signal
// is sent to the client to avoid silently losing messages.
func (c *ClientSession) Deliver(msg *pb.DownstreamMessage) {
	select {
	case c.deliveryCh <- msg:
	default:
		c.identityMu.RLock()
		identStr := c.Identity.String()
		c.identityMu.RUnlock()
		logging.Logger.Warn().Str("identity", identStr).Msg("delivery buffer full, dropping message for slow client")
		// Notify the client that messages are being dropped due to backpressure.
		// Use a non-blocking send to avoid deadlock if the buffer is still full.
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
			// Buffer still full — client is severely behind, notice also dropped
		}
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
