package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/scitrera/aether/internal/logging"
)

// DelayedPublisher publishes messages with a delay using RabbitMQ's x-delayed-message exchange.
type DelayedPublisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	mu           sync.RWMutex
	closed       bool
	stopCh       chan struct{} // signals monitorConnection to stop
	stopOnce     sync.Once     // ensures stopCh is closed exactly once
}

// NewDelayedPublisher creates a new delayed message publisher.
func NewDelayedPublisher(amqpURL string) (*DelayedPublisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare the delayed message exchange
	// This requires the rabbitmq_delayed_message_exchange plugin to be enabled
	err = ch.ExchangeDeclare(
		"aether_delayed",    // name
		"x-delayed-message", // type - requires plugin
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		amqp.Table{
			"x-delayed-type": "direct",
		},
	)
	if err != nil {
		// If the plugin is not available, fallback to dead-letter exchange approach
		logging.Logger.Warn().Err(err).Msg("x-delayed-message plugin not available, using DLX fallback")
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("delayed message exchange plugin required: %w", err)
	}

	dp := &DelayedPublisher{
		conn:         conn,
		channel:      ch,
		exchangeName: "aether_delayed",
		stopCh:       make(chan struct{}),
	}

	// Start connection health monitor
	go dp.monitorConnection()

	return dp, nil
}

// NewDelayedPublisherWithDLX creates a delayed publisher using dead-letter exchanges
// as a fallback when the delayed-message plugin is not available.
func NewDelayedPublisherWithDLX(amqpURL string) (*DelayedPublisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare the delayed queue with TTL and dead-letter routing
	_, err = ch.QueueDeclare(
		"aether_delayed_queue", // name
		true,                   // durable
		false,                  // delete when unused
		false,                  // exclusive
		false,                  // no-wait
		amqp.Table{
			"x-message-ttl":             int64(3600000), // default 1 hour max delay
			"x-dead-letter-exchange":    "",             // default exchange
			"x-dead-letter-routing-key": "aether_retry_queue",
		},
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare delayed queue: %w", err)
	}

	// Declare the retry queue where messages arrive after TTL
	_, err = ch.QueueDeclare(
		"aether_retry_queue", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare retry queue: %w", err)
	}

	// Bind retry queue to default exchange
	err = ch.QueueBind(
		"aether_retry_queue",
		"aether_retry_queue",
		"",    // default exchange
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind retry queue: %w", err)
	}

	dp := &DelayedPublisher{
		conn:         conn,
		channel:      ch,
		exchangeName: "", // Use default exchange for DLX approach
		stopCh:       make(chan struct{}),
	}

	go dp.monitorConnection()

	return dp, nil
}

// monitorConnection monitors the connection and reconnects if needed.
func (dp *DelayedPublisher) monitorConnection() {
	closeCh := dp.conn.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case <-dp.stopCh:
		return
	case <-closeCh:
		dp.mu.RLock()
		if dp.closed {
			dp.mu.RUnlock()
			return
		}
		dp.mu.RUnlock()
		logging.Logger.Warn().Msg("DelayedPublisher connection closed, will require recreation")
		return
	}
}

// PublishDelayed publishes a message with a delay.
func (dp *DelayedPublisher) PublishDelayed(
	ctx context.Context,
	routingKey string,
	body []byte,
	delay time.Duration,
) error {
	dp.mu.RLock()
	if dp.closed {
		dp.mu.RUnlock()
		return fmt.Errorf("publisher is closed")
	}
	dp.mu.RUnlock()

	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	// Cap delay at 24 hours
	if delay > 24*time.Hour {
		delay = 24 * time.Hour
	}

	// Use different publishing strategy based on exchange type
	if dp.exchangeName != "" {
		return dp.publishWithDelayedExchange(ctx, routingKey, body, delay)
	}
	return dp.publishWithDLX(ctx, routingKey, body, delay)
}

// publishWithDelayedExchange publishes using the x-delayed-message plugin.
func (dp *DelayedPublisher) publishWithDelayedExchange(
	ctx context.Context,
	routingKey string,
	body []byte,
	delay time.Duration,
) error {
	return dp.channel.PublishWithContext(
		ctx,
		dp.exchangeName, // exchange
		routingKey,      // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Headers: amqp.Table{
				"x-delay": delay.Milliseconds(),
			},
		},
	)
}

// publishWithDLX publishes using dead-letter exchange with message TTL.
func (dp *DelayedPublisher) publishWithDLX(
	ctx context.Context,
	routingKey string,
	body []byte,
	delay time.Duration,
) error {
	// For DLX approach, we publish to the delayed queue with per-message TTL
	return dp.channel.PublishWithContext(
		ctx,
		"",                     // default exchange
		"aether_delayed_queue", // routing key - goes to delayed queue
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Expiration:   fmt.Sprintf("%d", delay.Milliseconds()),
			Headers: amqp.Table{
				"x-retry-routing-key": routingKey, // Store original routing key
			},
		},
	)
}

// ConsumeRetries returns a channel for consuming retry messages.
func (dp *DelayedPublisher) ConsumeRetries(consumerID string) (<-chan amqp.Delivery, error) {
	return dp.channel.Consume(
		"aether_retry_queue",
		consumerID,
		false, // auto-ack = false (manual ACK for reliability)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
}

// Close shuts down the delayed publisher.
func (dp *DelayedPublisher) Close() error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if dp.closed {
		return nil
	}
	dp.closed = true

	// Signal monitorConnection goroutine to stop before closing the connection,
	// preventing the goroutine from blocking on a closed connection's NotifyClose.
	dp.stopOnce.Do(func() { close(dp.stopCh) })

	var errs []error
	if dp.channel != nil {
		if err := dp.channel.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if dp.conn != nil {
		if err := dp.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing publisher: %v", errs)
	}
	return nil
}

// IsPluginAvailable checks if the delayed message plugin is available.
func IsPluginAvailable(amqpURL string) bool {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return false
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	defer ch.Close()

	// Try to declare a delayed exchange
	err = ch.ExchangeDeclare(
		"aether_test_delayed",
		"x-delayed-message",
		false, // durable
		false, // auto-deleted
		true,  // internal
		false, // no-wait
		amqp.Table{
			"x-delayed-type": "direct",
		},
	)
	if err != nil {
		return false
	}

	// Clean up test exchange
	ch.ExchangeDelete("aether_test_delayed", false, false)

	return true
}
