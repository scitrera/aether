package gateway

import (
	"context"

	"github.com/scitrera/aether/internal/audit"
)

// auditLog logs an audit event asynchronously if the audit logger is configured.
func (s *GatewayServer) auditLog(ctx context.Context, event *audit.AuditEvent) {
	if s.auditLogger != nil {
		s.auditLogger.LogEvent(ctx, event)
	}
}
