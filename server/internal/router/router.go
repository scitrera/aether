package router

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

// poolEntry wraps a ProducerPool with last-access tracking for eviction.
// lastUsed is stored as Unix nanoseconds in an atomic int64 so it can be
// updated under only a read lock without a data race.
type poolEntry struct {
	pool     *ProducerPool
	lastUsed atomic.Int64 // Unix nanoseconds; updated on every access
}

type Router struct {
	env           *stream.Environment
	subscriptions *SubscriptionManager

	// Producer pool management
	producerPools map[string]*poolEntry
	poolsMu       sync.RWMutex

	// Pool evictor lifecycle
	evictorDone chan struct{}

	// publishTimeout is the maximum time to wait for a publish confirmation.
	publishTimeout time.Duration
}

func NewRouter(streamURL string, streamCapacityBytes int64) (*Router, error) {
	// For rabbitmq-stream-go-client, we typically need a URI like rabbitmq-stream://guest:guest@localhost:5552
	// If the user provides amqp://, we might need to adjust, but let's assume valid stream URL for now.
	// The client handles multiple addresses and other options.
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUri(streamURL),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream environment: %w", err)
	}

	r := &Router{
		env:            env,
		subscriptions:  NewSubscriptionManager(env, streamCapacityBytes),
		producerPools:  make(map[string]*poolEntry),
		evictorDone:    make(chan struct{}),
		publishTimeout: 5 * time.Second,
	}
	r.startPoolEvictor()
	return r, nil
}

func (r *Router) Close() {
	// Stop the pool evictor
	close(r.evictorDone)

	// Close all producer pools
	r.poolsMu.Lock()
	pools := r.producerPools
	r.producerPools = nil
	r.poolsMu.Unlock()

	for _, entry := range pools {
		if entry != nil {
			entry.pool.Close()
		}
	}

	if r.subscriptions != nil {
		r.subscriptions.Close()
	}
	if r.env != nil {
		r.env.Close()
	}
}

// GetProducerPoolStats returns statistics for all producer pools
func (r *Router) GetProducerPoolStats() map[string]ProducerPoolStats {
	r.poolsMu.RLock()
	defer r.poolsMu.RUnlock()

	stats := make(map[string]ProducerPoolStats)
	for topic, entry := range r.producerPools {
		stats[topic] = entry.pool.Stats()
	}
	return stats
}

// HealthCheck verifies RabbitMQ Streams connectivity by attempting to declare a test stream
func (r *Router) HealthCheck(ctx context.Context) error {
	if r.env == nil {
		return fmt.Errorf("stream environment not initialized")
	}

	// Try to declare a health check stream (this is idempotent)
	// Using a short-lived stream for health checks
	testStream := "_aether_health_check"
	err := r.env.DeclareStream(testStream, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.MB(1),
	})
	if err != nil && err != stream.StreamAlreadyExists {
		return fmt.Errorf("failed to connect to RabbitMQ Streams: %w", err)
	}

	return nil
}

func (r *Router) Publish(ctx context.Context, topic string, payload []byte) error {
	ctx, span := tracing.Tracer.Start(ctx, "router.Publish")
	defer span.End()
	span.SetAttributes(
		attribute.String("topic", topic),
		attribute.Int("payload_size", len(payload)),
	)

	publishStart := time.Now()

	// Get or create producer pool for this topic
	pool, err := r.getOrCreatePool(topic)
	if err != nil {
		rabbitmqPublishTotal.WithLabelValues("failure").Inc()
		rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
		return fmt.Errorf("failed to get producer pool: %w", err)
	}

	// Checkout a producer from the pool
	pp, err := pool.Checkout(ctx)
	if err != nil {
		rabbitmqPublishTotal.WithLabelValues("failure").Inc()
		rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
		return fmt.Errorf("failed to checkout producer: %w", err)
	}
	defer pool.Return(pp)

	// Send the message
	message := amqp.NewMessage(payload)
	err = pp.producer.Send(message)
	if err != nil {
		pp.healthy = false
		rabbitmqPublishTotal.WithLabelValues("failure").Inc()
		rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
		return fmt.Errorf("failed to send message: %w", err)
	}

	// Wait for confirmation using the producer's pre-registered channel
	// (registered once at creation to avoid cross-goroutine confirmation confusion)
	timer := time.NewTimer(r.publishTimeout)
	defer timer.Stop()
	select {
	case confirmations := <-pp.confirmCh:
		for _, c := range confirmations {
			if !c.IsConfirmed() {
				pp.healthy = false
				rabbitmqPublishTotal.WithLabelValues("failure").Inc()
				rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
				return fmt.Errorf("message not confirmed")
			}
		}
	case <-ctx.Done():
		pp.healthy = false
		rabbitmqPublishTotal.WithLabelValues("failure").Inc()
		rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
		return ctx.Err()
	case <-timer.C:
		pp.healthy = false
		rabbitmqPublishTotal.WithLabelValues("failure").Inc()
		rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
		return fmt.Errorf("publish confirmation timed out after %v", r.publishTimeout)
	}

	rabbitmqPublishTotal.WithLabelValues("success").Inc()
	rabbitmqPublishDuration.Observe(time.Since(publishStart).Seconds())
	return nil
}

// getOrCreatePool gets or creates a producer pool for a topic
func (r *Router) getOrCreatePool(topic string) (*ProducerPool, error) {
	now := time.Now()

	// Fast path: check if pool exists (read lock)
	r.poolsMu.RLock()
	pools := r.producerPools
	entry, exists := pools[topic]
	r.poolsMu.RUnlock()

	if pools == nil {
		return nil, fmt.Errorf("router is closed")
	}
	if exists {
		entry.lastUsed.Store(now.UnixNano())
		return entry.pool, nil
	}

	// Slow path: create pool (write lock)
	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	if r.producerPools == nil {
		return nil, fmt.Errorf("router is closed")
	}

	// Double-check after acquiring write lock (another goroutine might have created it)
	if entry, exists := r.producerPools[topic]; exists {
		entry.lastUsed.Store(now.UnixNano())
		return entry.pool, nil
	}

	// Create new pool
	pool, err := NewProducerPool(r.env, ProducerPoolConfig{
		Topic:               topic,
		MinSize:             2,
		MaxSize:             10,
		IdleTimeout:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	e := &poolEntry{pool: pool}
	e.lastUsed.Store(now.UnixNano())
	r.producerPools[topic] = e
	return pool, nil
}

// Subscribe adds a handler for a topic. Multiple handlers for the same topic
// share a single RabbitMQ consumer, with messages fanned out locally.
// Returns an unsubscribe function that must be called when done.
func (r *Router) Subscribe(topic string, handler func([]byte)) (func(), error) {
	return r.subscriptions.Subscribe(topic, handler)
}

// SubscribeExclusive creates an exclusive subscription with offset tracking.
// Each call creates a dedicated consumer that tracks its position in the stream.
// The consumerName should be unique per logical consumer (e.g., identity string).
// On reconnection, messages are replayed from the last committed offset.
func (r *Router) SubscribeExclusive(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscriptions.SubscribeWithOptions(topic, handler, ExclusiveSubscribeOptions(consumerName))
}

// SubscribeExclusiveFromNow creates an exclusive subscription that starts from the next new message,
// ignoring any previously stored offset. Useful for principals where historical messages are not needed.
func (r *Router) SubscribeExclusiveFromNow(topic string, consumerName string, handler func([]byte)) (func(), error) {
	opts := ExclusiveSubscribeOptions(consumerName)
	opts.OffsetMode = OffsetFromNow
	return r.subscriptions.SubscribeWithOptions(topic, handler, opts)
}

// SubscribeExclusiveFromTimestamp creates an exclusive subscription with offset tracking
// (OffsetResume) and a unix-millisecond timestamp hint. When no stored offset exists for
// consumerName, the subscription starts from the given timestamp instead of the default
// .Next(); when a stored offset exists, it wins over the hint. A startTimestampMs of 0
// behaves exactly like SubscribeExclusive (falls back to .Next()). This exists to support
// cold-starting pool-dispatched agents whose trigger message was published just before they
// subscribed — see Fix B in the plan at two-things-we-should-encapsulated-matsumoto.md.
func (r *Router) SubscribeExclusiveFromTimestamp(topic string, consumerName string, startTimestampMs int64, handler func([]byte)) (func(), error) {
	opts := ExclusiveSubscribeOptions(consumerName)
	opts.StartTimestampMs = startTimestampMs
	return r.subscriptions.SubscribeWithOptions(topic, handler, opts)
}

// SubscribeWithOptions adds a handler with custom subscription options.
func (r *Router) SubscribeWithOptions(topic string, handler func([]byte), opts SubscribeOptions) (func(), error) {
	return r.subscriptions.SubscribeWithOptions(topic, handler, opts)
}

// SubscriptionStats returns the number of handlers per topic
func (r *Router) SubscriptionStats() map[string]int {
	return r.subscriptions.Stats()
}

// startPoolEvictor starts a background goroutine that periodically evicts idle,
// empty producer pools from the producerPools map to prevent unbounded growth.
func (r *Router) startPoolEvictor() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "poolEvictor").Msg("recovered from panic in background goroutine")
			}
		}()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-r.evictorDone:
				return
			case <-ticker.C:
				r.evictIdlePools()
			}
		}
	}()
}

// evictIdlePools removes pools that have been idle for more than 5 minutes
// and have no active (checked-out) producers.
func (r *Router) evictIdlePools() {
	const idleThreshold = 5 * time.Minute

	now := time.Now()

	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	if r.producerPools == nil {
		return
	}

	for topic, entry := range r.producerPools {
		lastUsed := time.Unix(0, entry.lastUsed.Load())
		if now.Sub(lastUsed) > idleThreshold && entry.pool.ActiveCount() == 0 {
			entry.pool.Close()
			delete(r.producerPools, topic)
		}
	}
}
