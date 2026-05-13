package router

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/scitrera/aether/internal/logging"
)

// defaultFanOutConcurrency is the default capacity of the per-SubscriptionManager fan-out semaphore.
const defaultFanOutConcurrency = 64

// OffsetMode controls where a consumer starts reading from the stream
type OffsetMode int

const (
	// OffsetFromNow starts consuming from the next new message (default for shared/broadcast)
	OffsetFromNow OffsetMode = iota
	// OffsetResume resumes from the last stored offset, falling back to Now if no stored offset exists
	OffsetResume
)

// SubscribeOptions configures subscription behavior
type SubscribeOptions struct {
	// ConsumerName enables server-side offset tracking. When set, RabbitMQ tracks
	// the consumer's offset and can resume from the last stored position.
	// Should be unique per logical consumer (e.g., identity string for agents).
	ConsumerName string

	// EnableOffsetTracking enables automatic offset commits when ConsumerName is set.
	// Offsets are stored every CommitCount messages or CommitInterval, whichever comes first.
	EnableOffsetTracking bool

	// CommitCount is the number of messages between offset commits (default: 50)
	CommitCount int

	// CommitInterval is the time between offset commits (default: 10s)
	CommitInterval time.Duration

	// Exclusive indicates this is a dedicated subscription (not shared).
	// When true, a new consumer is always created even if one exists for the topic.
	// This is needed for unique identity topics where each client needs its own offset.
	Exclusive bool

	// OffsetMode controls where the consumer starts reading.
	// OffsetFromNow (default): start from next new message.
	// OffsetResume: resume from last stored offset, falling back to Now.
	OffsetMode OffsetMode

	// StartTimestampMs is an optional unix-millisecond timestamp hint. It is used
	// only when OffsetMode == OffsetResume and no stored offset exists for the
	// consumer. When > 0, the subscription starts from this timestamp via
	// stream.OffsetSpecification{}.Timestamp(StartTimestampMs), replaying messages
	// published at or after that point instead of skipping them via .Next().
	// A value of 0 (the default) means "not set" and preserves the legacy
	// fall-back-to-Next behaviour.
	StartTimestampMs int64
}

// DefaultSubscribeOptions returns options for shared/broadcast subscriptions
func DefaultSubscribeOptions() SubscribeOptions {
	return SubscribeOptions{
		EnableOffsetTracking: false,
		CommitCount:          50,
		CommitInterval:       10 * time.Second,
		Exclusive:            false,
	}
}

// ExclusiveSubscribeOptions returns options for dedicated subscriptions with offset tracking.
// Defaults to OffsetResume so consumers replay from their last stored offset on reconnection.
func ExclusiveSubscribeOptions(consumerName string) SubscribeOptions {
	return SubscribeOptions{
		ConsumerName:         consumerName,
		EnableOffsetTracking: true,
		CommitCount:          50,
		CommitInterval:       10 * time.Second,
		Exclusive:            true,
		OffsetMode:           OffsetResume,
	}
}

// SubscriptionManager manages shared consumers for topics with local fan-out.
// Multiple handlers can subscribe to the same topic, sharing a single RabbitMQ consumer.
// This reduces resource usage when many connections need the same topic (e.g., workspace broadcasts).
//
// For unique identity topics, exclusive subscriptions with offset tracking can be created,
// allowing message replay on reconnection.
type SubscriptionManager struct {
	env                 *stream.Environment
	subscriptions       map[string]*topicSubscription
	mu                  sync.RWMutex
	fanOutSemaphore     chan struct{}
	streamCapacityBytes int64
}

// topicSubscription represents a single RabbitMQ consumer shared by multiple handlers
type topicSubscription struct {
	topic           string
	consumer        *stream.Consumer
	handlers        map[uint64]func([]byte) // handlerID -> handler
	mu              sync.RWMutex
	nextID          atomic.Uint64
	exclusive       bool          // If true, this is a dedicated (non-shared) subscription
	consumerName    string        // For offset tracking
	fanOutSemaphore chan struct{} // bounded concurrency for multi-handler fan-out
}

// NewSubscriptionManager creates a new subscription manager.
// streamCapacityBytes sets the max byte capacity for declared streams; 0 defaults to 1GB.
func NewSubscriptionManager(env *stream.Environment, streamCapacityBytes int64) *SubscriptionManager {
	if streamCapacityBytes <= 0 {
		streamCapacityBytes = 1_000_000_000
	}
	return &SubscriptionManager{
		env:                 env,
		subscriptions:       make(map[string]*topicSubscription),
		fanOutSemaphore:     make(chan struct{}, defaultFanOutConcurrency),
		streamCapacityBytes: streamCapacityBytes,
	}
}

// Subscribe adds a handler for a topic using default options (shared, no offset tracking).
// If this is the first handler for the topic, a RabbitMQ consumer is created.
// Returns an unsubscribe function.
func (sm *SubscriptionManager) Subscribe(topic string, handler func([]byte)) (func(), error) {
	return sm.SubscribeWithOptions(topic, handler, DefaultSubscribeOptions())
}

// SubscribeWithOptions adds a handler for a topic with custom options.
// For exclusive subscriptions, a dedicated consumer is created with offset tracking.
// For shared subscriptions, handlers share a single consumer.
func (sm *SubscriptionManager) SubscribeWithOptions(topic string, handler func([]byte), opts SubscribeOptions) (func(), error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// For exclusive subscriptions, use a unique key to avoid sharing
	subscriptionKey := topic
	if opts.Exclusive && opts.ConsumerName != "" {
		subscriptionKey = fmt.Sprintf("%s::%s", topic, opts.ConsumerName)
	}

	sub, exists := sm.subscriptions[subscriptionKey]
	if !exists || opts.Exclusive {
		// Create new consumer for first subscriber or exclusive subscriptions
		newSub, err := sm.createSubscription(topic, opts)
		if err != nil {
			return nil, err
		}
		sm.subscriptions[subscriptionKey] = newSub
		sub = newSub
	}

	// Add handler to the subscription
	handlerID := sub.addHandler(handler)

	// Return unsubscribe function
	unsubscribe := func() {
		sm.unsubscribe(subscriptionKey, handlerID)
	}

	return unsubscribe, nil
}

// createSubscription creates a new topic subscription with a RabbitMQ consumer
func (sm *SubscriptionManager) createSubscription(topic string, opts SubscribeOptions) (*topicSubscription, error) {
	// Ensure stream exists
	err := sm.env.DeclareStream(topic, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.B(sm.streamCapacityBytes),
	})
	if err != nil && err != stream.StreamAlreadyExists {
		return nil, fmt.Errorf("failed to declare stream %s: %w", topic, err)
	}

	sub := &topicSubscription{
		topic:           topic,
		handlers:        make(map[uint64]func([]byte)),
		exclusive:       opts.Exclusive,
		consumerName:    opts.ConsumerName,
		fanOutSemaphore: sm.fanOutSemaphore,
	}

	// Create message handler that fans out to all registered handlers
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		data := message.GetData()
		sub.fanOut(data)
	}

	// Build consumer options
	consumerOpts := stream.NewConsumerOptions()

	if opts.ConsumerName != "" && opts.EnableOffsetTracking {
		// Named consumer with offset tracking - can resume from last position
		consumerOpts.SetConsumerName(opts.ConsumerName)

		// Set up auto-commit for offset tracking
		commitCount := opts.CommitCount
		if commitCount <= 0 {
			commitCount = 50
		}
		commitInterval := opts.CommitInterval
		if commitInterval <= 0 {
			commitInterval = 10 * time.Second
		}

		consumerOpts.SetAutoCommit(stream.NewAutoCommitStrategy().
			SetCountBeforeStorage(commitCount).
			SetFlushInterval(commitInterval))

		switch opts.OffsetMode {
		case OffsetResume:
			// Query stored offset; if found, resume from there
			storedOffset, err := sm.env.QueryOffset(opts.ConsumerName, topic)
			if err == nil && storedOffset >= 0 {
				consumerOpts.SetOffset(stream.OffsetSpecification{}.Offset(storedOffset + 1))
				logging.Logger.Info().Str("consumer", opts.ConsumerName).Str("topic", topic).Int64("offset", storedOffset).Msg("resuming from stored offset")
			} else if opts.StartTimestampMs > 0 {
				// No stored offset but caller provided a timestamp hint (e.g. the
				// moment the gateway accepted the user message that triggered the
				// agent startup). RabbitMQ Streams Timestamp() takes unix-millis and
				// returns the first message at or after that instant, so the trigger
				// message is replayed to the cold-started agent. See Fix B in the
				// plan at two-things-we-should-encapsulated-matsumoto.md.
				consumerOpts.SetOffset(stream.OffsetSpecification{}.Timestamp(opts.StartTimestampMs))
				logging.Logger.Info().Str("consumer", opts.ConsumerName).Str("topic", topic).Int64("timestamp_ms", opts.StartTimestampMs).Msg("no stored offset, starting from trigger timestamp")
			} else {
				// No stored offset — start from next new message
				consumerOpts.SetOffset(stream.OffsetSpecification{}.Next())
				logging.Logger.Info().Str("consumer", opts.ConsumerName).Str("topic", topic).Msg("no stored offset, starting from next message")
			}
		default: // OffsetFromNow
			consumerOpts.SetOffset(stream.OffsetSpecification{}.Next())
		}

		logging.Logger.Info().Str("consumer", opts.ConsumerName).Str("topic", topic).Int("commit_count", commitCount).Dur("commit_interval", commitInterval).Str("offset_mode", offsetModeName(opts.OffsetMode)).Msg("creating tracked consumer")
	} else {
		// Anonymous consumer - start from next message, no tracking
		consumerOpts.SetOffset(stream.OffsetSpecification{}.Next())
	}

	consumer, err := sm.env.NewConsumer(topic, handleMessages, consumerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer for %s: %w", topic, err)
	}

	sub.consumer = consumer

	if opts.Exclusive {
		logging.Logger.Info().Str("topic", topic).Str("consumer", opts.ConsumerName).Msg("created exclusive consumer")
	} else {
		logging.Logger.Info().Str("topic", topic).Msg("created shared consumer")
	}

	return sub, nil
}

// unsubscribe removes a handler from a topic. If it's the last handler,
// the RabbitMQ consumer is closed.
func (sm *SubscriptionManager) unsubscribe(topic string, handlerID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.subscriptions[topic]
	if !exists {
		return
	}

	remaining := sub.removeHandler(handlerID)

	// If no handlers left, close the consumer and remove the subscription
	if remaining == 0 {
		if sub.consumer != nil {
			if err := sub.consumer.Close(); err != nil {
				logging.Logger.Error().Err(err).Str("topic", topic).Msg("error closing consumer")
			} else {
				logging.Logger.Info().Str("topic", topic).Msg("closed shared consumer, no more subscribers")
			}
		}
		delete(sm.subscriptions, topic)
	}
}

// SubscriberCount returns the number of handlers subscribed to a topic
func (sm *SubscriptionManager) SubscriberCount(topic string) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sub, exists := sm.subscriptions[topic]
	if !exists {
		return 0
	}

	sub.mu.RLock()
	defer sub.mu.RUnlock()
	return len(sub.handlers)
}

// TopicCount returns the number of topics with active consumers
func (sm *SubscriptionManager) TopicCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.subscriptions)
}

// Stats returns subscription statistics
func (sm *SubscriptionManager) Stats() map[string]int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := make(map[string]int)
	for topic, sub := range sm.subscriptions {
		sub.mu.RLock()
		stats[topic] = len(sub.handlers)
		sub.mu.RUnlock()
	}
	return stats
}

// Close closes all consumers
func (sm *SubscriptionManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for topic, sub := range sm.subscriptions {
		if sub.consumer != nil {
			if err := sub.consumer.Close(); err != nil {
				logging.Logger.Error().Err(err).Str("topic", topic).Msg("error closing consumer")
			}
		}
	}
	sm.subscriptions = make(map[string]*topicSubscription)
}

// addHandler adds a handler and returns its ID
func (ts *topicSubscription) addHandler(handler func([]byte)) uint64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	id := ts.nextID.Add(1)
	ts.handlers[id] = handler

	logging.Logger.Debug().Uint64("handler_id", id).Str("topic", ts.topic).Int("total", len(ts.handlers)).Msg("added handler")
	return id
}

// removeHandler removes a handler and returns the remaining count
func (ts *topicSubscription) removeHandler(id uint64) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	delete(ts.handlers, id)
	logging.Logger.Debug().Uint64("handler_id", id).Str("topic", ts.topic).Int("remaining", len(ts.handlers)).Msg("removed handler")
	return len(ts.handlers)
}

// offsetModeName returns a human-readable name for the given OffsetMode.
func offsetModeName(m OffsetMode) string {
	switch m {
	case OffsetResume:
		return "resume"
	default:
		return "now"
	}
}

// fanOut delivers a message to all registered handlers.
// For a single handler it calls directly. For multiple handlers it dispatches
// each in its own goroutine with bounded concurrency via fanOutSemaphore.
// A sync.WaitGroup ensures all handlers finish before returning, so the
// caller's data buffer is safe to reuse immediately after fanOut returns.
func (ts *topicSubscription) fanOut(data []byte) {
	ts.mu.RLock()
	handlers := make([]func([]byte), 0, len(ts.handlers))
	for _, h := range ts.handlers {
		handlers = append(handlers, h)
	}
	ts.mu.RUnlock()

	if len(handlers) == 0 {
		return
	}
	if len(handlers) == 1 {
		handlers[0](data)
		return
	}

	var wg sync.WaitGroup
	for _, h := range handlers {
		select {
		case ts.fanOutSemaphore <- struct{}{}:
			wg.Add(1)
			go func(handler func([]byte)) {
				defer func() {
					<-ts.fanOutSemaphore
					wg.Done()
				}()
				handler(data)
			}(h)
		default:
			// Semaphore full; call synchronously to avoid dropping the message.
			h(data)
		}
	}
	wg.Wait()
}
