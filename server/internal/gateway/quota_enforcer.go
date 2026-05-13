package gateway

import (
	"github.com/scitrera/aether/internal/quota"
)

// QuotaEnforcer encapsulates per-tenant quota and rate limiting configuration for the gateway.
// It holds the quota checker, per-client message rate limit settings, and the
// workspace-level rate limiter for multi-tenant throughput control.
type QuotaEnforcer struct {
	quotaManager          QuotaChecker
	messageRateLimit      float64
	messageRateBurst      int
	maxTaskPayloadSize    int // 0 means use default (512KB)
	maxMessagePayloadSize int // 0 means use default (1MB)
	workspaceRateLimiter  *quota.WorkspaceRateLimiter

	// foreignAuditRateLimiter enforces per-principal rate limits on
	// SubmitAuditEvent submissions to prevent log-flooding from compromised
	// or buggy clients. nil disables per-principal limiting (default).
	foreignAuditRateLimiter *quota.PrincipalRateLimiter

	// Proxy/tunnel quotas.
	maxConcurrentTunnelsPerWorkspace int    // 0 means use default (256)
	maxRequestBodyBytes              int    // 0 means use default (8MB)
	maxTunnelBytes                   int64  // per-tunnel cumulative byte cap; 0 means unlimited
	maxChainDepth                    uint32 // 0 means use default (8)
}

// newQuotaEnforcer creates a QuotaEnforcer with the given defaults.
func newQuotaEnforcer(rateLimit float64, burst int) *QuotaEnforcer {
	return &QuotaEnforcer{
		messageRateLimit: rateLimit,
		messageRateBurst: burst,
	}
}

// getMaxTaskPayloadSize returns the effective max task payload size, defaulting to 512KB.
func (qe *QuotaEnforcer) getMaxTaskPayloadSize() int {
	if qe.maxTaskPayloadSize > 0 {
		return qe.maxTaskPayloadSize
	}
	return 512 * 1024 // 512KB default
}

// getMaxMessagePayloadSize returns the effective max message payload size, defaulting to 1MB.
func (qe *QuotaEnforcer) getMaxMessagePayloadSize() int {
	if qe.maxMessagePayloadSize > 0 {
		return qe.maxMessagePayloadSize
	}
	return 1024 * 1024 // 1MB default
}

// getMaxConcurrentTunnelsPerWorkspace returns the per-workspace concurrent tunnel
// cap, defaulting to 256 when unset.
func (qe *QuotaEnforcer) getMaxConcurrentTunnelsPerWorkspace() int {
	if qe.maxConcurrentTunnelsPerWorkspace > 0 {
		return qe.maxConcurrentTunnelsPerWorkspace
	}
	return 256
}

// getMaxRequestBodyBytes returns the proxy HTTP request-body cap, defaulting to 8MB.
func (qe *QuotaEnforcer) getMaxRequestBodyBytes() int {
	if qe.maxRequestBodyBytes > 0 {
		return qe.maxRequestBodyBytes
	}
	return 8 * 1024 * 1024
}

// getMaxTunnelBytes returns the per-tunnel cumulative byte cap. 0 means unlimited.
func (qe *QuotaEnforcer) getMaxTunnelBytes() int64 {
	return qe.maxTunnelBytes
}

// getMaxChainDepth returns the proxy/tunnel hop-count cap, defaulting to 8.
// A ProxyHttpRequest or TunnelOpen with proxy_chain_depth >= this value is
// rejected before forwarding, breaking sandbox→sandbox proxy loops.
func (qe *QuotaEnforcer) getMaxChainDepth() uint32 {
	if qe.maxChainDepth > 0 {
		return qe.maxChainDepth
	}
	return 8
}
