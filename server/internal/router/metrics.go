package router

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	rabbitmqPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aether",
		Name:      "rabbitmq_publish_total",
		Help:      "Total number of RabbitMQ stream publish attempts",
	}, []string{"status"})

	rabbitmqPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "aether",
		Name:      "rabbitmq_publish_duration_seconds",
		Help:      "Duration of RabbitMQ stream publish operations",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms to ~3.2s
	})
)
