package admin

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// metrics.go contains only the active-connections Prometheus gauge and a small
// helper that aggregates GatewayStats. There is no per-handler HTTP endpoint,
// so these tests verify the gauge stays consistent with the stats snapshot it
// is fed (the surface that actually gets executed at runtime by the admin
// metrics-refresh goroutine).

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("gauge Write: %v", err)
	}
	if m.Gauge == nil || m.Gauge.Value == nil {
		t.Fatal("gauge value not set")
	}
	return *m.Gauge.Value
}

func TestUpdateActiveConnectionsMetric_NilStats(t *testing.T) {
	// Should be a no-op without panicking.
	updateActiveConnectionsMetric(nil)
}

func TestUpdateActiveConnectionsMetric_SumsAllConnectionTypes(t *testing.T) {
	stats := &GatewayStats{
		AgentConnections:        3,
		TaskConnections:         2,
		UserConnections:         5,
		OrchestratorConnections: 1,
	}
	updateActiveConnectionsMetric(stats)

	want := float64(3 + 2 + 5 + 1)
	if got := gaugeValue(t, adminActiveConnections); got != want {
		t.Errorf("gauge = %v, want %v", got, want)
	}
}

func TestUpdateActiveConnectionsMetric_IncludesWorkflowEngineAndMetricsBridge(t *testing.T) {
	stats := &GatewayStats{
		AgentConnections:        1,
		TaskConnections:         1,
		UserConnections:         1,
		OrchestratorConnections: 0,
		WorkflowEngineConnected: true,
		MetricsBridgeConnected:  true,
	}
	updateActiveConnectionsMetric(stats)

	want := float64(1 + 1 + 1 + 0 + 1 + 1)
	if got := gaugeValue(t, adminActiveConnections); got != want {
		t.Errorf("gauge = %v, want %v", got, want)
	}
}

func TestUpdateActiveConnectionsMetric_AllZero(t *testing.T) {
	stats := &GatewayStats{}
	updateActiveConnectionsMetric(stats)

	if got := gaugeValue(t, adminActiveConnections); got != 0 {
		t.Errorf("gauge = %v, want 0", got)
	}
}
