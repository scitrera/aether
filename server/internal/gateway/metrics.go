package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	redisOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "redis_operations_total",
		Help:      "Total number of Redis session/lock operations",
	}, []string{"operation", "status"})

	sessionLockDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "aether",
		Name:      "session_lock_duration_seconds",
		Help:      "Duration of session lock acquisition",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms to ~3.2s
	})

	circuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "aether",
		Name:      "circuit_breaker_state",
		Help:      "Circuit breaker state: 0=closed, 1=open, 2=half-open",
	}, []string{"subsystem"})

	messagesRouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "messages_routed_total",
		Help:      "Total number of messages successfully routed",
	}, []string{"workspace", "message_type"})

	messageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "message_errors_total",
		Help:      "Total number of message routing errors",
	}, []string{"workspace", "error_type"})

	activeConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "aether",
		Name:      "active_connections",
		Help:      "Number of currently active client connections",
	}, []string{"workspace", "principal_type"})

	connectionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "aether",
		Name:      "connection_duration_seconds",
		Help:      "Duration of client connections",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1s to ~4.5h
	}, []string{"workspace", "principal_type"})

	messageRoutingLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "aether",
		Name:      "message_routing_latency_seconds",
		Help:      "Latency of message routing from receive to publish",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms to ~3.2s
	}, []string{"workspace"})

	kvOperationLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "aether",
		Name:      "kv_operation_latency_seconds",
		Help:      "Latency of KV store operations",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1ms to ~800ms
	}, []string{"operation", "scope"})

	kvOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "kv_operations_total",
		Help:      "Total number of KV operations",
	}, []string{"operation", "scope", "status"})

	connectionAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "connection_attempts_total",
		Help:      "Total number of connection attempts",
	}, []string{"workspace", "principal_type", "status"})

	orchestrationTriggers = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "orchestration_triggers_total",
		Help:      "Total number of orchestration triggers for offline targets",
	}, []string{"workspace"})

	topicSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "aether",
		Name:      "topic_subscriptions_active",
		Help:      "Number of active topic subscriptions",
	})

	// proxyLocalBypassTotal counts data-plane envelopes that took the in-process
	// fast path between caller and target sidecars on the same gateway instance,
	// versus those that fell through to RMQ. Labelled by envelope_type
	// (tunnel_data, tunnel_ack, proxy_http_body_chunk) and result
	// (hit, rmq_fallback, full_buffer, disabled).
	proxyLocalBypassTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "proxy_local_bypass_total",
		Help:      "Proxy data-plane envelopes routed via the local single-node bypass",
	}, []string{"envelope_type", "result"})
)
