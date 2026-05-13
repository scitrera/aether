package admin

import "github.com/prometheus/client_golang/prometheus"

var (
	// adminActiveConnections tracks the total number of active gRPC connections.
	// Updated periodically by the admin server's metrics refresh goroutine.
	adminActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "aether",
		Name:      "admin_active_connections_total",
		Help:      "Total number of active gRPC connections (admin aggregate)",
	})
)

func init() {
	prometheus.MustRegister(adminActiveConnections)
}

// updateActiveConnectionsMetric sets the activeConnections gauge from a GatewayStats snapshot.
func updateActiveConnectionsMetric(stats *GatewayStats) {
	if stats == nil {
		return
	}
	total := stats.AgentConnections +
		stats.TaskConnections +
		stats.UserConnections +
		stats.OrchestratorConnections
	if stats.WorkflowEngineConnected {
		total++
	}
	if stats.MetricsBridgeConnected {
		total++
	}
	adminActiveConnections.Set(float64(total))
}
