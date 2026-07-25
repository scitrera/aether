package gateway

import (
	"context"

	"github.com/scitrera/aether/server/internal/audit"
)

// auditLog logs an audit event asynchronously if the audit logger is configured.
//
// A coalescing gate runs before the write: when an audit coalescer is
// configured (audit_coalesce_window > 0), a burst of identical successful
// message-route / proxy-route events from the same sender→target is recorded
// once per window (the first = the authorization record) and the rest are
// suppressed. Every failure, denial, and non-high-volume op is always written.
// When no coalescer is configured (window = 0) s.auditCoalescer is nil and
// shouldLog is a zero-overhead passthrough.
func (s *GatewayServer) auditLog(ctx context.Context, event *audit.AuditEvent) {
	if s.auditLogger == nil {
		return
	}
	if !s.auditCoalescer.shouldLog(event) {
		return
	}
	s.auditLogger.LogEvent(ctx, event)
}
