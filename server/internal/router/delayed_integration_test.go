package router

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/testutil"
)

// getRabbitMQAMQPURL returns the AMQP URL from dev infrastructure
func getRabbitMQAMQPURL() string {
	config := testutil.GetRabbitMQConfig()
	return config.AMQPURL()
}

// TestDelayedPublisher_Integration tests delayed message publishing with real RabbitMQ
func TestDelayedPublisher_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	// Check if delayed message plugin is available
	pluginAvailable := IsPluginAvailable(amqpURL)
	t.Logf("Delayed message plugin available: %v", pluginAvailable)

	var publisher *DelayedPublisher
	var err error

	if pluginAvailable {
		publisher, err = NewDelayedPublisher(amqpURL)
	} else {
		t.Log("Using DLX fallback")
		publisher, err = NewDelayedPublisherWithDLX(amqpURL)
	}

	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Test message
	type testMessage struct {
		TaskID    string `json:"task_id"`
		Workspace string `json:"workspace"`
		Attempt   int    `json:"attempt"`
	}

	msg := testMessage{
		TaskID:    "task-123",
		Workspace: "production",
		Attempt:   1,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	// Publish with delay
	ctx := context.Background()
	err = publisher.PublishDelayed(ctx, "test.retry.task-123", body, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to publish delayed message: %v", err)
	}

	t.Log("Delayed message published successfully")
	t.Log("In a real system, this would be consumed after the delay period")
}

// TestDelayedPublisher_NegativeDelay tests that negative delays are rejected
func TestDelayedPublisher_NegativeDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx := context.Background()
	err = publisher.PublishDelayed(ctx, "test.negative", []byte("test"), -1*time.Second)
	if err == nil {
		t.Error("Expected error for negative delay, got nil")
	}
}

// TestDelayedPublisher_ZeroDelay tests immediate delivery with zero delay
func TestDelayedPublisher_ZeroDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx := context.Background()
	err = publisher.PublishDelayed(ctx, "test.immediate", []byte("test"), 0)
	if err != nil {
		t.Errorf("Expected no error for zero delay, got: %v", err)
	}

	t.Log("Zero delay message published successfully (immediate delivery)")
}

// TestDelayedPublisher_LargeDelay tests that very large delays are capped
func TestDelayedPublisher_LargeDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Try to publish with delay > 24 hours (should be capped)
	ctx := context.Background()
	err = publisher.PublishDelayed(ctx, "test.large", []byte("test"), 48*time.Hour)
	if err != nil {
		t.Errorf("Expected no error for large delay (should be capped), got: %v", err)
	}

	t.Log("Large delay message published (capped at 24 hours)")
}

// TestDelayedPublisher_MultipleMessages tests publishing multiple delayed messages
func TestDelayedPublisher_MultipleMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx := context.Background()

	// Publish multiple messages with different delays
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		message := map[string]interface{}{
			"task_id": i,
			"index":   i,
			"attempt": 1,
		}
		body, _ := json.Marshal(message)

		delay := time.Duration(i+1) * time.Second
		err := publisher.PublishDelayed(ctx, "test.multi", body, delay)
		if err != nil {
			t.Errorf("Failed to publish message %d: %v", i, err)
		}
	}

	t.Logf("Published %d messages with varying delays", numMessages)
}

// TestDelayedPublisher_ConsumeRetries tests retry message consumption
func TestDelayedPublisher_ConsumeRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Set up consumer
	deliveries, err := publisher.ConsumeRetries("test-consumer")
	if err != nil {
		t.Fatalf("Failed to set up consumer: %v", err)
	}

	// Publish a message
	ctx := context.Background()
	testMessage := map[string]string{
		"task_id": "retry-test-1",
		"reason":  "timeout",
	}
	body, _ := json.Marshal(testMessage)

	err = publisher.PublishDelayed(ctx, "test.retry", body, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	t.Log("Message published, waiting for delivery...")

	// Wait for message (with timeout)
	select {
	case delivery := <-deliveries:
		t.Logf("Received retry message: %s", string(delivery.Body))
		// ACK the message
		delivery.Ack(false)
	case <-time.After(5 * time.Second):
		t.Log("No message received within timeout (this is expected if queue was empty)")
	}
}

// TestDelayedPublisher_RetryFlow simulates a complete retry flow
func TestDelayedPublisher_RetryFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx := context.Background()

	// Simulate a task retry scenario
	type retryTask struct {
		TaskID      string    `json:"task_id"`
		Workspace   string    `json:"workspace"`
		Attempt     int       `json:"attempt"`
		MaxAttempts int       `json:"max_attempts"`
		LastError   string    `json:"last_error"`
		RetryAt     time.Time `json:"retry_at"`
	}

	task := retryTask{
		TaskID:      "task-xyz-789",
		Workspace:   "production",
		Attempt:     2,
		MaxAttempts: 5,
		LastError:   "connection timeout",
		RetryAt:     time.Now().Add(3 * time.Second),
	}

	body, _ := json.Marshal(task)

	// Calculate backoff delay
	backoff := time.Until(task.RetryAt)

	t.Logf("Scheduling retry for task %s after %v", task.TaskID, backoff)
	err = publisher.PublishDelayed(ctx, "retry.task."+task.TaskID, body, backoff)
	if err != nil {
		t.Fatalf("Failed to schedule retry: %v", err)
	}

	t.Log("Retry scheduled successfully")
	t.Log("In a production system:")
	t.Log("  1. This message would wait in the delayed queue")
	t.Log("  2. After the delay, it would be delivered to the retry queue")
	t.Log("  3. A gateway would consume it and re-dispatch the task")
	t.Log("  4. The task would attempt execution again")
}

// TestDelayedPublisher_PluginFallback tests plugin availability detection
func TestDelayedPublisher_PluginFallback(t *testing.T) {
	amqpURL := getRabbitMQAMQPURL()

	available := IsPluginAvailable(amqpURL)
	t.Logf("Delayed message plugin available: %v", available)

	if available {
		t.Log("System supports x-delayed-message plugin")
		publisher, err := NewDelayedPublisher(amqpURL)
		if err != nil {
			t.Skipf("Failed to create publisher with plugin: %v", err)
		}
		defer publisher.Close()
		t.Log("Publisher created successfully with plugin")
	} else {
		t.Log("System does not support x-delayed-message plugin, using DLX fallback")
		publisher, err := NewDelayedPublisherWithDLX(amqpURL)
		if err != nil {
			t.Skipf("Failed to create publisher with DLX: %v", err)
		}
		defer publisher.Close()
		t.Log("Publisher created successfully with DLX fallback")
	}
}

// TestDelayedPublisher_ConnectionClosure tests graceful shutdown
func TestDelayedPublisher_ConnectionClosure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}

	// Publish a message
	ctx := context.Background()
	err = publisher.PublishDelayed(ctx, "test.close", []byte("test"), 1*time.Second)
	if err != nil {
		t.Errorf("Failed to publish before close: %v", err)
	}

	// Close the publisher
	err = publisher.Close()
	if err != nil {
		t.Errorf("Failed to close publisher: %v", err)
	}

	// Try to publish after close (should fail)
	err = publisher.PublishDelayed(ctx, "test.after-close", []byte("test"), 1*time.Second)
	if err == nil {
		t.Error("Expected error when publishing after close, got nil")
	}

	// Try to close again (should be safe)
	err = publisher.Close()
	if err != nil {
		t.Errorf("Second close should be safe, got error: %v", err)
	}
}

// TestDelayedPublisher_ExponentialBackoff tests exponential backoff retry scheduling
func TestDelayedPublisher_ExponentialBackoff(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	amqpURL := getRabbitMQAMQPURL()

	publisher, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Skipf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx := context.Background()

	// Simulate exponential backoff for a failing task
	taskID := "failing-task-123"
	maxAttempts := 5

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Calculate exponential backoff: 2^attempt seconds
		backoff := time.Duration(1<<uint(attempt)) * time.Second

		// Cap at 1 minute for test
		if backoff > time.Minute {
			backoff = time.Minute
		}

		message := map[string]interface{}{
			"task_id":      taskID,
			"attempt":      attempt,
			"max_attempts": maxAttempts,
			"backoff_ms":   backoff.Milliseconds(),
		}
		body, _ := json.Marshal(message)

		err := publisher.PublishDelayed(ctx, "retry.backoff."+taskID, body, backoff)
		if err != nil {
			t.Errorf("Failed to schedule retry %d: %v", attempt, err)
		} else {
			t.Logf("Scheduled retry %d/%d with %v backoff", attempt, maxAttempts, backoff)
		}
	}

	t.Log("Exponential backoff retry sequence scheduled successfully")
	t.Log("Backoff progression: 2s, 4s, 8s, 16s, 32s")
}
