//go:build integration

package router

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/scitrera/aether/internal/testutil"
)

// BenchmarkPublishWithPooling benchmarks message publishing with producer pooling
func BenchmarkPublishWithPooling(b *testing.B) {
	streamURL := os.Getenv("STREAM_URL")
	if streamURL == "" {
		streamURL = testutil.GetRabbitMQConfig().StreamURL()
	}

	// Check if RabbitMQ is available
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(streamURL))
	if err != nil {
		b.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	defer env.Close()

	router, err := NewRouter(streamURL, 0)
	if err != nil {
		b.Fatalf("Failed to create router: %v", err)
	}
	defer router.Close()

	topic := "bench.test.pooled"
	payload := []byte("benchmark message with pooling")

	// Ensure stream exists (topics ARE stream names in this implementation)
	_ = env.DeclareStream(topic, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(1),
	})

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark
	for i := 0; i < b.N; i++ {
		err := router.Publish(ctx, topic, payload)
		if err != nil {
			b.Fatalf("Publish failed: %v", err)
		}
	}
}

// BenchmarkPublishWithoutPooling benchmarks message publishing without producer pooling (create/destroy each time)
func BenchmarkPublishWithoutPooling(b *testing.B) {
	streamURL := os.Getenv("STREAM_URL")
	if streamURL == "" {
		streamURL = testutil.GetRabbitMQConfig().StreamURL()
	}

	// Check if RabbitMQ is available
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(streamURL))
	if err != nil {
		b.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	defer env.Close()

	topic := "bench.test.no-pool"
	payload := []byte("benchmark message without pooling")

	// Ensure stream exists
	_ = env.DeclareStream(topic, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(1),
	})

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark - create and destroy producer each time (old behavior)
	for i := 0; i < b.N; i++ {
		producer, err := env.NewProducer(topic, stream.NewProducerOptions())
		if err != nil {
			b.Fatalf("Failed to create producer: %v", err)
		}

		amqpMsg := amqp.NewMessage(payload)
		err = producer.Send(amqpMsg)
		if err != nil {
			producer.Close()
			b.Fatalf("Send failed: %v", err)
		}

		producer.Close()
	}
}

// BenchmarkPublishConcurrentWithPooling tests concurrent publishing with pooling
func BenchmarkPublishConcurrentWithPooling(b *testing.B) {
	streamURL := os.Getenv("STREAM_URL")
	if streamURL == "" {
		streamURL = testutil.GetRabbitMQConfig().StreamURL()
	}

	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(streamURL))
	if err != nil {
		b.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	defer env.Close()

	router, err := NewRouter(streamURL, 0)
	if err != nil {
		b.Fatalf("Failed to create router: %v", err)
	}
	defer router.Close()

	topic := "bench.test.concurrent"
	payload := []byte("concurrent benchmark message")

	_ = env.DeclareStream(topic, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(1),
	})

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := router.Publish(ctx, topic, payload)
			if err != nil {
				b.Fatalf("Publish failed: %v", err)
			}
		}
	})
}

// BenchmarkProducerPoolCheckoutReturn benchmarks the pool checkout/return overhead
func BenchmarkProducerPoolCheckoutReturn(b *testing.B) {
	streamURL := os.Getenv("STREAM_URL")
	if streamURL == "" {
		streamURL = testutil.GetRabbitMQConfig().StreamURL()
	}

	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(streamURL))
	if err != nil {
		b.Fatalf("RabbitMQ not available at %s: %v", streamURL, err)
	}
	defer env.Close()

	streamName := "bench.pool.checkout"
	_ = env.DeclareStream(streamName, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(1),
	})

	pool, err := NewProducerPool(env, ProducerPoolConfig{
		Topic:               streamName,
		MinSize:             2,
		MaxSize:             10,
		HealthCheckInterval: 30 * time.Second,
		IdleTimeout:         5 * time.Minute,
	})
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		producer, err := pool.Checkout(ctx)
		if err != nil {
			b.Fatalf("Checkout failed: %v", err)
		}
		pool.Return(producer)
	}
}
