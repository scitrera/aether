package router

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/scitrera/aether/internal/logging"
)

// ProducerPool manages a pool of RabbitMQ stream producers with health monitoring
// and adaptive sizing. Producers are reused across publish operations to reduce
// resource overhead compared to creating/destroying producers per message.
type ProducerPool struct {
	env                 *stream.Environment
	topic               string
	minSize             int
	maxSize             int
	idleTimeout         time.Duration
	healthCheckInterval time.Duration

	// Pool management
	mu         sync.Mutex
	producers  []*pooledProducer
	available  chan *pooledProducer
	stopHealth chan struct{}
	closed     bool

	// Metrics (atomic counters)
	totalCreated   atomic.Int64
	totalDestroyed atomic.Int64
	checkouts      atomic.Int64
	returns        atomic.Int64
	healthChecks   atomic.Int64
	healthFailures atomic.Int64
	waitTimeNs     atomic.Int64 // Total wait time in nanoseconds
}

// pooledProducer wraps a producer with metadata for pool management
type pooledProducer struct {
	producer      *stream.Producer
	confirmCh     <-chan []*stream.ConfirmationStatus // registered once at creation
	lastUsed      time.Time
	healthy       bool
	checkoutCount int64
}

// ProducerPoolConfig contains configuration for a producer pool
type ProducerPoolConfig struct {
	Topic               string
	MinSize             int
	MaxSize             int
	IdleTimeout         time.Duration
	HealthCheckInterval time.Duration
}

// NewProducerPool creates a new producer pool for a specific topic
func NewProducerPool(env *stream.Environment, cfg ProducerPoolConfig) (*ProducerPool, error) {
	if env == nil {
		return nil, fmt.Errorf("stream environment is required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if cfg.MinSize < 0 {
		cfg.MinSize = 0
	}
	if cfg.MaxSize < 1 {
		cfg.MaxSize = 10
	}
	if cfg.MinSize > cfg.MaxSize {
		cfg.MinSize = cfg.MaxSize
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 30 * time.Second
	}

	// Ensure stream exists
	err := env.DeclareStream(cfg.Topic, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(1),
	})
	if err != nil && err != stream.StreamAlreadyExists {
		return nil, fmt.Errorf("failed to declare stream %s: %w", cfg.Topic, err)
	}

	pool := &ProducerPool{
		env:                 env,
		topic:               cfg.Topic,
		minSize:             cfg.MinSize,
		maxSize:             cfg.MaxSize,
		idleTimeout:         cfg.IdleTimeout,
		healthCheckInterval: cfg.HealthCheckInterval,
		producers:           make([]*pooledProducer, 0, cfg.MaxSize),
		available:           make(chan *pooledProducer, cfg.MaxSize),
		stopHealth:          make(chan struct{}),
	}

	// Pre-warm pool with minimum connections
	for i := 0; i < cfg.MinSize; i++ {
		pp, err := pool.createProducer()
		if err != nil {
			// Clean up any created producers and fail
			pool.Close()
			return nil, fmt.Errorf("failed to pre-warm pool: %w", err)
		}
		pool.producers = append(pool.producers, pp)
		pool.available <- pp
	}

	// Start background health checker
	go pool.healthCheckLoop()

	return pool, nil
}

// createProducer creates a new pooled producer instance
func (p *ProducerPool) createProducer() (*pooledProducer, error) {
	producer, err := p.env.NewProducer(p.topic, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for %s: %w", p.topic, err)
	}

	pp := &pooledProducer{
		producer:  producer,
		confirmCh: producer.NotifyPublishConfirmation(),
		lastUsed:  time.Now(),
		healthy:   true,
	}

	p.totalCreated.Add(1)
	return pp, nil
}

// Checkout retrieves a producer from the pool. If no producers are available
// and the pool is not at max capacity, a new producer is created.
// Returns error if pool is closed or context is cancelled.
func (p *ProducerPool) Checkout(ctx context.Context) (*pooledProducer, error) {
	startTime := time.Now()
	defer func() {
		p.waitTimeNs.Add(time.Since(startTime).Nanoseconds())
	}()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pp := <-p.available:
		// Got a producer from the pool
		pp.lastUsed = time.Now()
		pp.checkoutCount++
		p.checkouts.Add(1)
		return pp, nil
	default:
		// No available producers - try to create a new one if under max
		p.mu.Lock()
		canCreate := len(p.producers) < p.maxSize
		if canCreate {
			// Hold lock through create-and-append to prevent TOCTOU race
			pp, err := p.createProducer()
			if err != nil {
				p.mu.Unlock()
				return nil, err
			}
			p.producers = append(p.producers, pp)
			p.mu.Unlock()

			pp.lastUsed = time.Now()
			pp.checkoutCount++
			p.checkouts.Add(1)
			return pp, nil
		}
		p.mu.Unlock()

		// Pool is at max - wait for an available producer
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.stopHealth:
			// Pool is closing; stopHealth is closed by Close()
			return nil, fmt.Errorf("pool is closed")
		case pp := <-p.available:
			pp.lastUsed = time.Now()
			pp.checkoutCount++
			p.checkouts.Add(1)
			return pp, nil
		}
	}
}

// Return returns a producer to the pool. If the producer is unhealthy,
// it will be destroyed and removed from the pool.
func (p *ProducerPool) Return(pp *pooledProducer) {
	if pp == nil {
		return
	}

	p.mu.Lock()
	if p.closed {
		// Pool is closed, destroy the producer
		p.mu.Unlock()
		p.destroyProducer(pp)
		return
	}
	p.mu.Unlock()

	p.returns.Add(1)

	if !pp.healthy {
		// Don't return unhealthy producers to the pool
		p.destroyProducerAndRemove(pp)
		return
	}

	// Return to pool
	select {
	case p.available <- pp:
		// Successfully returned to pool
	default:
		// Pool is full (shouldn't happen, but handle gracefully)
		p.destroyProducerAndRemove(pp)
	}
}

// destroyProducer closes a producer and updates metrics
func (p *ProducerPool) destroyProducer(pp *pooledProducer) {
	if pp.producer != nil {
		pp.producer.Close()
	}
	p.totalDestroyed.Add(1)
}

// destroyProducerAndRemove closes a producer and removes it from the pool
func (p *ProducerPool) destroyProducerAndRemove(pp *pooledProducer) {
	p.destroyProducer(pp)

	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove from producers slice
	for i, existing := range p.producers {
		if existing == pp {
			p.producers = append(p.producers[:i], p.producers[i+1:]...)
			break
		}
	}
}

// healthCheckLoop runs periodic health checks on idle producers
func (p *ProducerPool) healthCheckLoop() {
	defer func() {
		if r := recover(); r != nil {
			logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "healthCheckLoop").Msg("recovered from panic in background goroutine")
		}
	}()
	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopHealth:
			return
		case <-ticker.C:
			p.performHealthChecks()
		}
	}
}

// performHealthChecks checks all available producers and removes unhealthy or idle ones.
// It drains the available channel into a local slice so health checks do not compete
// with concurrent Checkout calls, then returns surviving producers back to the channel.
func (p *ProducerPool) performHealthChecks() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	// Drain all currently-available producers into a local slice without blocking.
	// Producers that are checked out are not in the channel and are skipped entirely.
	var available []*pooledProducer
	for {
		select {
		case pp := <-p.available:
			available = append(available, pp)
		default:
			goto drained
		}
	}
drained:

	now := time.Now()
	for _, pp := range available {
		p.healthChecks.Add(1)

		// Basic health check - producer must not be nil
		if pp.producer == nil {
			pp.healthy = false
			p.healthFailures.Add(1)
			p.destroyProducerAndRemove(pp)
			continue
		}

		// Check if idle for too long; only evict if above minimum pool size
		if now.Sub(pp.lastUsed) > p.idleTimeout {
			p.mu.Lock()
			currentSize := len(p.producers)
			p.mu.Unlock()

			if currentSize > p.minSize {
				pp.healthy = false
				p.destroyProducerAndRemove(pp)
				continue
			}
		}

		// Producer is healthy — return it to the available channel.
		select {
		case p.available <- pp:
		default:
			// Channel full (shouldn't happen), destroy to avoid leak.
			p.destroyProducerAndRemove(pp)
		}
	}
}

// Stats returns current pool statistics
func (p *ProducerPool) Stats() ProducerPoolStats {
	p.mu.Lock()
	totalProducers := len(p.producers)
	availableProducers := len(p.available)
	p.mu.Unlock()

	avgWaitTime := int64(0)
	checkouts := p.checkouts.Load()
	if checkouts > 0 {
		avgWaitTime = p.waitTimeNs.Load() / checkouts
	}

	return ProducerPoolStats{
		Topic:              p.topic,
		TotalProducers:     totalProducers,
		AvailableProducers: availableProducers,
		InUseProducers:     totalProducers - availableProducers,
		TotalCreated:       p.totalCreated.Load(),
		TotalDestroyed:     p.totalDestroyed.Load(),
		Checkouts:          checkouts,
		Returns:            p.returns.Load(),
		HealthChecks:       p.healthChecks.Load(),
		HealthFailures:     p.healthFailures.Load(),
		AvgWaitTimeNs:      avgWaitTime,
	}
}

// ProducerPoolStats contains pool statistics for monitoring
type ProducerPoolStats struct {
	Topic              string
	TotalProducers     int
	AvailableProducers int
	InUseProducers     int
	TotalCreated       int64
	TotalDestroyed     int64
	Checkouts          int64
	Returns            int64
	HealthChecks       int64
	HealthFailures     int64
	AvgWaitTimeNs      int64
}

// ActiveCount returns the number of producers currently checked out of the pool.
func (p *ProducerPool) ActiveCount() int {
	p.mu.Lock()
	total := len(p.producers)
	available := len(p.available)
	p.mu.Unlock()
	return total - available
}

// Close closes the pool and all producers
func (p *ProducerPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Stop health checker
	close(p.stopHealth)

	// Drain the available channel without closing producers here.
	// Do NOT close the channel itself — a concurrent Return() could send on it
	// after we release the lock but before it observes p.closed, causing a panic.
	// The channel will be garbage-collected when the pool is no longer referenced.
	// We close all producers exactly once below via p.producers.
	for {
		select {
		case <-p.available:
		default:
			goto drained
		}
	}
drained:

	// Close all producers exactly once using the canonical slice.
	// This covers both idle producers (previously in the channel) and any
	// that are currently checked out, without double-closing either.
	p.mu.Lock()
	producers := p.producers
	p.producers = nil
	p.mu.Unlock()

	for _, pp := range producers {
		if pp != nil && pp.producer != nil {
			pp.producer.Close()
			p.totalDestroyed.Add(1)
		}
	}

	return nil
}
