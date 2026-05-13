package orchestration

// DispatcherStats holds statistics for the orchestration dispatcher.
type DispatcherStats struct {
	QueueDepth        float64 `json:"queue_depth"`
	ClaimRate         float64 `json:"claim_rate"`
	FailureRate       float64 `json:"failure_rate"`
	AvgClaimLatencyMs float64 `json:"avg_claim_latency_ms"`
}
