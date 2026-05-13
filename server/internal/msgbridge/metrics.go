package msgbridge

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messagesRoutedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "aether",
			Subsystem: "msgbridge",
			Name:      "messages_routed_total",
			Help:      "Total messages routed by the bridge",
		},
		[]string{"direction", "platform", "status"}, // direction: inbound/outbound, status: delivered/failed/dropped
	)

	messageRoutingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "aether",
			Subsystem: "msgbridge",
			Name:      "message_routing_duration_seconds",
			Help:      "Duration of message routing operations",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"direction", "platform"},
	)

	platformHealthy = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "aether",
			Subsystem: "msgbridge",
			Name:      "platform_healthy",
			Help:      "Whether a platform adapter is healthy (1=healthy, 0=unhealthy)",
		},
		[]string{"platform"},
	)

	activeMappings = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aether",
			Subsystem: "msgbridge",
			Name:      "active_mappings",
			Help:      "Number of enabled channel mappings",
		},
	)
)
