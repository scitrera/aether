package router

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/testutil"
)

func TestDelayedPublisherCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Test with real connection if available
	config := testutil.GetRabbitMQConfig()
	connStr := config.AMQPURL()

	// Test that we can detect plugin availability
	// Note: This requires RabbitMQ to be running with the delayed message plugin
	available := IsPluginAvailable(connStr)
	t.Logf("Testing delayed publisher creation... Plugin available: %v", available)
}

// TestIsPluginAvailable tests the plugin availability check
func TestIsPluginAvailable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := testutil.GetRabbitMQConfig()
	connStr := config.AMQPURL()
	available := IsPluginAvailable(connStr)

	if available {
		t.Log("Delayed message plugin is available")
	} else {
		t.Log("Delayed message plugin is NOT available (using DLX fallback)")
	}
}

// TestDelayedPublisher_NegativeDelayRejected verifies that PublishDelayed rejects negative delays.
func TestDelayedPublisher_NegativeDelayRejected(t *testing.T) {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		t.Skip("RABBITMQ_URL not set, skipping integration test")
	}

	dp, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Fatalf("NewDelayedPublisherWithDLX() error = %v", err)
	}
	defer dp.Close()

	ctx := context.Background()
	err = dp.PublishDelayed(ctx, "test-key", []byte(`{"test": true}`), -1*time.Second)
	if err == nil {
		t.Error("Expected error for negative delay, got nil")
	}
}

// TestDelayedPublisher_DelayCapAt24Hours verifies that delays exceeding 24 hours are capped and published successfully.
func TestDelayedPublisher_DelayCapAt24Hours(t *testing.T) {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		t.Skip("RABBITMQ_URL not set, skipping integration test")
	}

	dp, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Fatalf("NewDelayedPublisherWithDLX() error = %v", err)
	}
	defer dp.Close()

	ctx := context.Background()
	// 48-hour delay should be silently capped to 24h without error
	err = dp.PublishDelayed(ctx, "test-key", []byte(`{"test": true}`), 48*time.Hour)
	if err != nil {
		t.Errorf("Expected no error when delay is capped to 24h, got: %v", err)
	}
}

// TestDelayedPublisher_ClosedPublisherRejectsPublish verifies that a closed publisher returns an error.
func TestDelayedPublisher_ClosedPublisherRejectsPublish(t *testing.T) {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		t.Skip("RABBITMQ_URL not set, skipping integration test")
	}

	dp, err := NewDelayedPublisherWithDLX(amqpURL)
	if err != nil {
		t.Fatalf("NewDelayedPublisherWithDLX() error = %v", err)
	}

	if err := dp.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ctx := context.Background()
	err = dp.PublishDelayed(ctx, "test-key", []byte(`{"test": true}`), time.Second)
	if err == nil {
		t.Error("Expected error publishing on closed publisher, got nil")
	}
}
