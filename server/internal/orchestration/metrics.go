package orchestration

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DispatcherMetrics holds Prometheus metrics for the orchestration dispatcher.
// These metrics provide observability into dispatcher health, including queue depth,
// processing rates, and latency percentiles for monitoring and alerting.
type DispatcherMetrics struct {
	// QueueDepth tracks the current number of pending tasks in the orchestration queue
	QueueDepth prometheus.Gauge

	// TasksClaimed tracks the total number of tasks claimed by orchestrators
	TasksClaimed prometheus.Counter

	// TasksCompleted tracks the total number of tasks that completed successfully
	TasksCompleted prometheus.Counter

	// TasksFailed tracks the total number of tasks that failed
	TasksFailed prometheus.Counter

	// ClaimLatency tracks the distribution of task claim latencies in seconds
	ClaimLatency prometheus.Histogram
}

// NewDispatcherMetrics creates and registers Prometheus metrics for the dispatcher.
// Metrics are automatically registered with the default Prometheus registry.
// For testing, use NewDispatcherMetricsWithRegistry with a custom registry to avoid conflicts.
func NewDispatcherMetrics() *DispatcherMetrics {
	return NewDispatcherMetricsWithRegistry(nil)
}

// NewDispatcherMetricsWithRegistry creates metrics with a custom registry.
// Pass nil to use the default registry (for production).
// Pass prometheus.NewRegistry() for isolated test metrics.
func NewDispatcherMetricsWithRegistry(reg prometheus.Registerer) *DispatcherMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	factory := promauto.With(reg)

	return &DispatcherMetrics{
		QueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dispatcher_queue_depth",
			Help: "Current number of pending tasks in the orchestration queue",
		}),
		TasksClaimed: factory.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_tasks_claimed_total",
			Help: "Total number of tasks claimed by orchestrators",
		}),
		TasksCompleted: factory.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_tasks_completed_total",
			Help: "Total number of tasks completed successfully",
		}),
		TasksFailed: factory.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_tasks_failed_total",
			Help: "Total number of tasks that failed",
		}),
		ClaimLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name: "dispatcher_claim_latency_seconds",
			Help: "Histogram of task claim latency in seconds",
			Buckets: []float64{
				0.001, // 1ms
				0.005, // 5ms
				0.010, // 10ms
				0.025, // 25ms
				0.050, // 50ms
				0.100, // 100ms
				0.250, // 250ms
				0.500, // 500ms
				1.000, // 1s
				2.500, // 2.5s
				5.000, // 5s
			},
		}),
	}
}

// RecordTaskClaimed increments the claimed counter and records claim latency.
// latencySeconds should be computed using time.Since(startTime).Seconds()
func (m *DispatcherMetrics) RecordTaskClaimed(latencySeconds float64) {
	m.TasksClaimed.Inc()
	m.ClaimLatency.Observe(latencySeconds)
}

// RecordTaskCompleted increments the completed counter
func (m *DispatcherMetrics) RecordTaskCompleted() {
	m.TasksCompleted.Inc()
}

// RecordTaskFailed increments the failed counter
func (m *DispatcherMetrics) RecordTaskFailed() {
	m.TasksFailed.Inc()
}

// UpdateQueueDepth sets the current queue depth gauge value
func (m *DispatcherMetrics) UpdateQueueDepth(depth float64) {
	m.QueueDepth.Set(depth)
}
