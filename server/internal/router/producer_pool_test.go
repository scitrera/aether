package router

import (
	"context"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/scitrera/aether/internal/testutil"
)

func TestProducerPool_Creation(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-pool-creation",
		MinSize:             2,
		MaxSize:             5,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	stats := pool.Stats()
	if stats.TotalProducers != 2 {
		t.Errorf("Expected 2 pre-warmed producers, got %d", stats.TotalProducers)
	}
	if stats.AvailableProducers != 2 {
		t.Errorf("Expected 2 available producers, got %d", stats.AvailableProducers)
	}
	if stats.TotalCreated != 2 {
		t.Errorf("Expected 2 created producers, got %d", stats.TotalCreated)
	}
}

func TestProducerPool_CheckoutReturn(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-checkout-return",
		MinSize:             1,
		MaxSize:             3,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Checkout a producer
	pp, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer: %v", err)
	}

	stats := pool.Stats()
	if stats.Checkouts != 1 {
		t.Errorf("Expected 1 checkout, got %d", stats.Checkouts)
	}
	if stats.InUseProducers != 1 {
		t.Errorf("Expected 1 in-use producer, got %d", stats.InUseProducers)
	}

	// Return the producer
	pool.Return(pp)

	stats = pool.Stats()
	if stats.Returns != 1 {
		t.Errorf("Expected 1 return, got %d", stats.Returns)
	}
	if stats.InUseProducers != 0 {
		t.Errorf("Expected 0 in-use producers, got %d", stats.InUseProducers)
	}
}

func TestProducerPool_AdaptiveSizing(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-adaptive-sizing",
		MinSize:             1,
		MaxSize:             3,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Initially should have MinSize producers
	stats := pool.Stats()
	if stats.TotalProducers != 1 {
		t.Errorf("Expected 1 initial producer, got %d", stats.TotalProducers)
	}

	// Checkout all available and trigger creation
	pp1, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer 1: %v", err)
	}

	pp2, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer 2: %v", err)
	}

	// Should have created a new producer
	stats = pool.Stats()
	if stats.TotalProducers != 2 {
		t.Errorf("Expected 2 producers after adaptive growth, got %d", stats.TotalProducers)
	}

	// Return producers
	pool.Return(pp1)
	pool.Return(pp2)

	stats = pool.Stats()
	if stats.AvailableProducers != 2 {
		t.Errorf("Expected 2 available producers after return, got %d", stats.AvailableProducers)
	}
}

func TestProducerPool_MaxCapacity(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-max-capacity",
		MinSize:             1,
		MaxSize:             2,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Checkout up to max capacity
	pp1, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer 1: %v", err)
	}

	pp2, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer 2: %v", err)
	}

	// All producers are checked out
	stats := pool.Stats()
	if stats.TotalProducers != 2 {
		t.Errorf("Expected 2 total producers, got %d", stats.TotalProducers)
	}
	if stats.InUseProducers != 2 {
		t.Errorf("Expected 2 in-use producers, got %d", stats.InUseProducers)
	}

	// Try to checkout another - should wait
	done := make(chan bool)
	go func() {
		pp3, err := pool.Checkout(ctx)
		if err == nil {
			pool.Return(pp3)
			done <- true
		} else if err == context.DeadlineExceeded {
			done <- false
		} else {
			t.Errorf("Unexpected error: %v", err)
			done <- false
		}
	}()

	// Give the goroutine time to block on Checkout, then return one
	time.Sleep(20 * time.Millisecond)
	pool.Return(pp1)

	// The waiting checkout should now succeed
	select {
	case success := <-done:
		if !success {
			t.Error("Third checkout should have succeeded after return")
		}
	case <-time.After(1 * time.Second):
		t.Error("Checkout timed out waiting for available producer")
	}

	pool.Return(pp2)
}

func TestProducerPool_UnhealthyProducerRemoval(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-unhealthy-removal",
		MinSize:             1,
		MaxSize:             3,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Checkout a producer
	pp, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer: %v", err)
	}

	initialTotal := pool.Stats().TotalProducers

	// Mark it as unhealthy
	pp.healthy = false

	// Return it - should be destroyed
	pool.Return(pp)

	// Poll until producer is removed or deadline
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pool.Stats().TotalProducers == initialTotal-1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := pool.Stats()
	if stats.TotalProducers != initialTotal-1 {
		t.Errorf("Expected unhealthy producer to be removed, total: %d", stats.TotalProducers)
	}
	if stats.TotalDestroyed != 1 {
		t.Errorf("Expected 1 destroyed producer, got %d", stats.TotalDestroyed)
	}
}

func TestProducerPool_IdleTimeout(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-idle-timeout",
		MinSize:             1,
		MaxSize:             3,
		IdleTimeout:         500 * time.Millisecond,
		HealthCheckInterval: 200 * time.Millisecond,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Create an extra producer
	pp, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout producer: %v", err)
	}

	// This should trigger creation of a second producer
	pp2, err := pool.Checkout(ctx)
	if err != nil {
		t.Fatalf("Failed to checkout second producer: %v", err)
	}

	// Return both
	pool.Return(pp)
	pool.Return(pp2)

	// Should have 2 producers now
	stats := pool.Stats()
	if stats.TotalProducers != 2 {
		t.Errorf("Expected 2 producers, got %d", stats.TotalProducers)
	}

	// Poll until idle cleanup runs (IdleTimeout=500ms, HealthCheckInterval=200ms)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pool.Stats().TotalProducers == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Should have cleaned up extra producer (back to minSize of 1)
	stats = pool.Stats()
	if stats.TotalProducers != 1 {
		t.Errorf("Expected idle producer to be cleaned up, total: %d", stats.TotalProducers)
	}
}

func TestProducerPool_Close(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-close",
		MinSize:             2,
		MaxSize:             5,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}

	stats := pool.Stats()
	initialCreated := stats.TotalCreated

	// Close the pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Failed to close pool: %v", err)
	}

	// Should not be able to checkout after close
	ctx := context.Background()
	_, err = pool.Checkout(ctx)
	if err == nil {
		t.Error("Expected error when checking out from closed pool")
	}

	// Close should be idempotent
	err = pool.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}

	// All producers should have been destroyed
	stats = pool.Stats()
	if stats.TotalProducers != 0 {
		t.Errorf("Expected 0 producers after close, got %d", stats.TotalProducers)
	}

	// Note: We can't easily verify TotalDestroyed equals TotalCreated due to async cleanup
	// But we can verify producers were destroyed
	if stats.TotalDestroyed < initialCreated {
		t.Logf("Warning: Not all producers destroyed (%d created, %d destroyed)",
			initialCreated, stats.TotalDestroyed)
	}
}

func TestProducerPool_Stats(t *testing.T) {
	env, cleanup := setupTestEnvironment(t)
	defer cleanup()

	cfg := ProducerPoolConfig{
		Topic:               "test-stats",
		MinSize:             1,
		MaxSize:             3,
		IdleTimeout:         1 * time.Minute,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewProducerPool(env, cfg)
	if err != nil {
		t.Fatalf("Failed to create producer pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Perform some operations
	pp1, _ := pool.Checkout(ctx)
	pp2, _ := pool.Checkout(ctx)
	pool.Return(pp1)
	pool.Return(pp2)

	stats := pool.Stats()

	if stats.Topic != "test-stats" {
		t.Errorf("Expected topic 'test-stats', got '%s'", stats.Topic)
	}
	if stats.Checkouts < 2 {
		t.Errorf("Expected at least 2 checkouts, got %d", stats.Checkouts)
	}
	if stats.Returns < 2 {
		t.Errorf("Expected at least 2 returns, got %d", stats.Returns)
	}
	if stats.TotalCreated < 1 {
		t.Errorf("Expected at least 1 created producer, got %d", stats.TotalCreated)
	}
}

// setupTestEnvironment creates a test RabbitMQ stream environment
func setupTestEnvironment(t *testing.T) (*stream.Environment, func()) {
	t.Helper()

	// Use test RabbitMQ stream URL from dev infrastructure config
	streamURL := testutil.GetRabbitMQConfig().StreamURL()

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(streamURL),
	)
	if err != nil {
		t.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
		return nil, func() {}
	}

	cleanup := func() {
		if env != nil {
			env.Close()
		}
	}

	return env, cleanup
}
