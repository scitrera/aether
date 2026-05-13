package metering

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MessagesRouted counts messages routed per workspace and message type.
	// Use for billing by message volume.
	MessagesRouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Subsystem: "metering",
		Name:      "messages_routed_total",
		Help:      "Total messages routed per workspace and message type",
	}, []string{"workspace", "message_type"})

	// BytesRouted counts payload bytes routed per workspace.
	// Use for billing by data transfer volume.
	BytesRouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Subsystem: "metering",
		Name:      "bytes_routed_total",
		Help:      "Total payload bytes routed per workspace",
	}, []string{"workspace"})

	// ActiveConnections tracks current active connections per workspace and principal type.
	// Use for billing by peak concurrent connections.
	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "aether",
		Subsystem: "metering",
		Name:      "active_connections",
		Help:      "Current active connections per workspace and principal type",
	}, []string{"workspace", "principal_type"})

	// KVOperations counts KV store operations per workspace and operation type.
	// Use for billing by API call volume.
	KVOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Subsystem: "metering",
		Name:      "kv_operations_total",
		Help:      "Total KV operations per workspace and operation type",
	}, []string{"workspace", "operation"})

	// CheckpointOperations counts checkpoint operations per workspace and operation type.
	// Use for billing by checkpoint storage usage.
	CheckpointOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Subsystem: "metering",
		Name:      "checkpoint_operations_total",
		Help:      "Total checkpoint operations per workspace and operation type",
	}, []string{"workspace", "operation"})

	// TaskOperations counts task lifecycle operations per workspace and operation type.
	// Use for billing by task execution volume.
	TaskOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Subsystem: "metering",
		Name:      "task_operations_total",
		Help:      "Total task operations per workspace and operation type",
	}, []string{"workspace", "operation"})
)
