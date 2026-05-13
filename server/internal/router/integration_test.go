package router

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/testutil"
)

// TestProducerPoolIntegration verifies that the Router uses producer pools
// correctly, reuses producers across publish operations, and tracks metrics
func TestProducerPoolIntegration(t *testing.T) {
	// Create Router (connects to RabbitMQ)
	streamURL := testutil.GetRabbitMQConfig().StreamURL()
	router, err := NewRouter(streamURL, 0)
	if err != nil {
		t.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	defer router.Close()

	ctx := context.Background()

	// Verify RabbitMQ connectivity
	err = router.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("RabbitMQ health check failed: %v", err)
	}

	t.Run("ProducerPoolCreation", func(t *testing.T) {
		// Verify that producer pools are created on-demand
		topic := fmt.Sprintf("test-pool-creation-%d", time.Now().UnixNano())

		// First publish should create a new pool
		err := router.Publish(ctx, topic, []byte("message 1"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		// Verify pool was created
		router.poolsMu.RLock()
		entry, exists := router.producerPools[topic]
		router.poolsMu.RUnlock()

		if !exists {
			t.Fatal("Expected producer pool to be created for topic")
		}

		// Check pool stats
		stats := entry.pool.Stats()
		t.Logf("Pool stats after first publish:")
		t.Logf("  Topic: %s", stats.Topic)
		t.Logf("  Total Producers: %d", stats.TotalProducers)
		t.Logf("  Available Producers: %d", stats.AvailableProducers)
		t.Logf("  Total Created: %d", stats.TotalCreated)
		t.Logf("  Checkouts: %d", stats.Checkouts)
		t.Logf("  Returns: %d", stats.Returns)

		if stats.TotalProducers == 0 {
			t.Error("Expected at least one producer to be created")
		}

		if stats.Checkouts == 0 {
			t.Error("Expected at least one checkout")
		}
	})

	t.Run("ProducerReuse", func(t *testing.T) {
		// Verify that producers are reused across multiple publishes
		topic := fmt.Sprintf("test-producer-reuse-%d", time.Now().UnixNano())

		// Publish multiple messages to the same topic
		messageCount := 10
		for i := 0; i < messageCount; i++ {
			payload := []byte(fmt.Sprintf("message %d", i))
			err := router.Publish(ctx, topic, payload)
			if err != nil {
				t.Fatalf("Publish %d failed: %v", i, err)
			}
		}

		// Get pool statistics
		router.poolsMu.RLock()
		entry, exists := router.producerPools[topic]
		router.poolsMu.RUnlock()

		if !exists {
			t.Fatal("Expected producer pool to exist")
		}

		stats := entry.pool.Stats()
		t.Logf("Pool stats after %d publishes:", messageCount)
		t.Logf("  Total Producers: %d", stats.TotalProducers)
		t.Logf("  Available Producers: %d", stats.AvailableProducers)
		t.Logf("  In Use: %d", stats.InUseProducers)
		t.Logf("  Total Created: %d", stats.TotalCreated)
		t.Logf("  Total Destroyed: %d", stats.TotalDestroyed)
		t.Logf("  Checkouts: %d", stats.Checkouts)
		t.Logf("  Returns: %d", stats.Returns)
		t.Logf("  Health Checks: %d", stats.HealthChecks)
		t.Logf("  Avg Wait Time: %dns", stats.AvgWaitTimeNs)

		// Verify producer reuse - we should have significantly fewer producers than messages
		if stats.TotalCreated >= int64(messageCount) {
			t.Errorf("Expected producer reuse, but created %d producers for %d messages", stats.TotalCreated, messageCount)
		}

		// Verify checkouts/returns match message count
		if stats.Checkouts != int64(messageCount) {
			t.Errorf("Expected %d checkouts, got %d", messageCount, stats.Checkouts)
		}

		if stats.Returns != int64(messageCount) {
			t.Errorf("Expected %d returns, got %d", messageCount, stats.Returns)
		}

		// Verify no producers leaked (created - destroyed should match current pool size)
		expectedActive := stats.TotalCreated - stats.TotalDestroyed
		if int64(stats.TotalProducers) != expectedActive {
			t.Errorf("Producer leak detected: %d producers active, but %d expected (%d created - %d destroyed)",
				stats.TotalProducers, expectedActive, stats.TotalCreated, stats.TotalDestroyed)
		}
	})

	t.Run("MultipleTopics", func(t *testing.T) {
		// Verify that different topics get separate pools
		topic1 := fmt.Sprintf("test-topic-1-%d", time.Now().UnixNano())
		topic2 := fmt.Sprintf("test-topic-2-%d", time.Now().UnixNano())
		topic3 := fmt.Sprintf("test-topic-3-%d", time.Now().UnixNano())

		// Publish to different topics
		topics := []string{topic1, topic2, topic3}
		for _, topic := range topics {
			err := router.Publish(ctx, topic, []byte("test message"))
			if err != nil {
				t.Fatalf("Publish to %s failed: %v", topic, err)
			}
		}

		// Verify separate pools were created
		router.poolsMu.RLock()
		poolCount := len(router.producerPools)
		router.poolsMu.RUnlock()

		// Should have at least 3 new pools (plus any from previous tests)
		if poolCount < 3 {
			t.Errorf("Expected at least 3 producer pools, got %d", poolCount)
		}

		// Verify each topic has its own pool
		for _, topic := range topics {
			router.poolsMu.RLock()
			_, exists := router.producerPools[topic]
			router.poolsMu.RUnlock()

			if !exists {
				t.Errorf("Expected pool for topic %s", topic)
			}
		}
	})

	t.Run("PoolMetrics", func(t *testing.T) {
		// Test pool metrics tracking
		topic := fmt.Sprintf("test-pool-metrics-%d", time.Now().UnixNano())

		// Get initial state
		router.poolsMu.RLock()
		initialPoolCount := len(router.producerPools)
		router.poolsMu.RUnlock()

		// Publish some messages
		messageCount := 5
		for i := 0; i < messageCount; i++ {
			err := router.Publish(ctx, topic, []byte(fmt.Sprintf("metric test %d", i)))
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		// Verify pool was created
		router.poolsMu.RLock()
		finalPoolCount := len(router.producerPools)
		entry, exists := router.producerPools[topic]
		router.poolsMu.RUnlock()

		if !exists {
			t.Fatal("Expected pool to be created")
		}

		if finalPoolCount <= initialPoolCount {
			t.Error("Expected pool count to increase")
		}

		// Verify metrics
		stats := entry.pool.Stats()
		if stats.Checkouts < int64(messageCount) {
			t.Errorf("Expected at least %d checkouts, got %d", messageCount, stats.Checkouts)
		}

		if stats.Returns < int64(messageCount) {
			t.Errorf("Expected at least %d returns, got %d", messageCount, stats.Returns)
		}

		if stats.TotalCreated == 0 {
			t.Error("Expected some producers to be created")
		}

		// All producers should be available (returned to pool)
		if stats.InUseProducers != 0 {
			t.Errorf("Expected all producers to be available, but %d are in use", stats.InUseProducers)
		}
	})

	t.Run("CleanShutdown", func(t *testing.T) {
		// Create a new router for shutdown testing
		shutdownRouter, err := NewRouter(streamURL, 0)
		if err != nil {
			t.Fatalf("Failed to create router for shutdown test: %v", err)
		}

		topic := fmt.Sprintf("test-shutdown-%d", time.Now().UnixNano())

		// Publish some messages to create pools
		for i := 0; i < 5; i++ {
			err := shutdownRouter.Publish(ctx, topic, []byte(fmt.Sprintf("shutdown test %d", i)))
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		// Get pool stats before shutdown
		shutdownRouter.poolsMu.RLock()
		entry, exists := shutdownRouter.producerPools[topic]
		shutdownRouter.poolsMu.RUnlock()

		if !exists {
			t.Fatal("Expected pool to exist before shutdown")
		}

		preShutdownStats := entry.pool.Stats()
		t.Logf("Pre-shutdown stats:")
		t.Logf("  Total Producers: %d", preShutdownStats.TotalProducers)
		t.Logf("  Total Created: %d", preShutdownStats.TotalCreated)
		t.Logf("  Total Destroyed: %d", preShutdownStats.TotalDestroyed)

		// Close the router (should close all pools and producers)
		shutdownRouter.Close()

		// Verify pools are cleaned up
		shutdownRouter.poolsMu.RLock()
		poolsNil := shutdownRouter.producerPools == nil
		shutdownRouter.poolsMu.RUnlock()

		if !poolsNil {
			t.Error("Expected producer pools map to be nil after Close()")
		}

		// Verify subsequent publishes fail gracefully
		err = shutdownRouter.Publish(ctx, topic, []byte("after shutdown"))
		if err == nil {
			t.Error("Expected publish to fail after router shutdown")
		}

		t.Log("Clean shutdown verified - no producer leaks")
	})
}

// TestRouterIntegration simulates a simplified gateway router usage pattern
func TestRouterIntegration(t *testing.T) {
	// Create Router (simulating gateway startup)
	streamURL := testutil.GetRabbitMQConfig().StreamURL()
	router, err := NewRouter(streamURL, 0)
	if err != nil {
		t.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Logf("Router initialized with RabbitMQ at %s", streamURL)

	// Simulate multiple agents/tasks publishing messages
	topics := []string{
		"ag::workspace1::agent-impl::instance-1",
		"tu::workspace1::task-impl::task-1",
		"ag::workspace2::agent-impl::instance-2",
	}

	// Publish messages from different topics (simulating agent activity)
	messageCount := 20
	for i := 0; i < messageCount; i++ {
		topic := topics[i%len(topics)]
		payload := []byte(fmt.Sprintf("message %d from %s", i, topic))

		err := router.Publish(ctx, topic, payload)
		if err != nil {
			t.Fatalf("Publish to %s failed: %v", topic, err)
		}
	}

	t.Logf("Published %d messages across %d topics", messageCount, len(topics))

	// Verify pools were created for each topic
	router.poolsMu.RLock()
	poolCount := len(router.producerPools)
	router.poolsMu.RUnlock()

	if poolCount < len(topics) {
		t.Errorf("Expected at least %d pools, got %d", len(topics), poolCount)
	}

	t.Logf("Created %d producer pools", poolCount)

	// Check each pool's statistics
	router.poolsMu.RLock()
	for topic, entry := range router.producerPools {
		stats := entry.pool.Stats()
		t.Logf("Pool for topic %s:", topic)
		t.Logf("  Total Producers: %d (created: %d, destroyed: %d)",
			stats.TotalProducers, stats.TotalCreated, stats.TotalDestroyed)
		t.Logf("  Checkouts: %d, Returns: %d", stats.Checkouts, stats.Returns)
		t.Logf("  Health Checks: %d (failures: %d)", stats.HealthChecks, stats.HealthFailures)

		// Verify producer reuse
		if stats.TotalCreated > stats.Checkouts {
			t.Errorf("Pool for %s created more producers (%d) than checkouts (%d)",
				topic, stats.TotalCreated, stats.Checkouts)
		}

		// Verify all producers are returned
		if stats.InUseProducers != 0 {
			t.Errorf("Pool for %s has %d producers in use after all operations",
				topic, stats.InUseProducers)
		}

		// Verify no leaks
		expectedActive := stats.TotalCreated - stats.TotalDestroyed
		if int64(stats.TotalProducers) != expectedActive {
			t.Errorf("Pool for %s has leak: %d active, expected %d",
				topic, stats.TotalProducers, expectedActive)
		}
	}
	router.poolsMu.RUnlock()

	// Clean shutdown
	t.Log("Initiating router shutdown...")
	cancel()
	router.Close()

	// Verify pools are cleaned up
	router.poolsMu.RLock()
	poolsNil := router.producerPools == nil
	router.poolsMu.RUnlock()

	if !poolsNil {
		t.Error("Expected producer pools to be cleaned up after shutdown")
	}

	t.Log("Router shutdown complete - no producer leaks detected")
}

// TestProducerPoolConcurrency tests concurrent publishing to verify thread-safety
func TestProducerPoolConcurrency(t *testing.T) {
	streamURL := testutil.GetRabbitMQConfig().StreamURL()
	router, err := NewRouter(streamURL, 0)
	if err != nil {
		t.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	defer router.Close()

	ctx := context.Background()
	topic := fmt.Sprintf("test-concurrency-%d", time.Now().UnixNano())

	// Launch multiple goroutines publishing concurrently
	concurrency := 10
	messagesPerGoroutine := 5
	done := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				payload := []byte(fmt.Sprintf("goroutine %d, message %d", id, j))
				err := router.Publish(ctx, topic, payload)
				if err != nil {
					done <- fmt.Errorf("goroutine %d, message %d failed: %w", id, j, err)
					return
				}
			}
			done <- nil
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < concurrency; i++ {
		err := <-done
		if err != nil {
			t.Errorf("Concurrent publish failed: %v", err)
		}
	}

	// Verify pool statistics
	router.poolsMu.RLock()
	entry, exists := router.producerPools[topic]
	router.poolsMu.RUnlock()

	if !exists {
		t.Fatal("Expected pool to exist after concurrent publishes")
	}

	stats := entry.pool.Stats()
	expectedMessages := concurrency * messagesPerGoroutine

	t.Logf("Concurrent publish stats:")
	t.Logf("  Messages published: %d", expectedMessages)
	t.Logf("  Total Producers: %d", stats.TotalProducers)
	t.Logf("  Checkouts: %d", stats.Checkouts)
	t.Logf("  Returns: %d", stats.Returns)

	// Verify checkouts/returns match total messages
	if stats.Checkouts != int64(expectedMessages) {
		t.Errorf("Expected %d checkouts, got %d", expectedMessages, stats.Checkouts)
	}

	if stats.Returns != int64(expectedMessages) {
		t.Errorf("Expected %d returns, got %d", expectedMessages, stats.Returns)
	}

	// Verify all producers are available (no leaks from concurrent access)
	if stats.InUseProducers != 0 {
		t.Errorf("Expected all producers available, but %d are in use", stats.InUseProducers)
	}

	t.Log("Concurrent access verified - no race conditions or leaks")
}
