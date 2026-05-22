// Package aether - comprehensive audit log query ops for the Go SDK.
//
// This file exposes QueryAuditLog, the synchronous wrapper around the gateway's
// AuditQuery / AuditQueryResponse round-trip. Only system principals
// (OrchestratorClient, WorkflowEngineClient) are unconditionally permitted;
// agent/user principals require ACL admin_operations or workspace read access.
//
// Correlation pattern mirrors SubmitAuditEvent: a fresh request_id is minted
// per call, registered in pendingAuditQueryRequests, and resolved by
// handleAuditQueryResponse in the dispatch loop.

package aether

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultAuditQueryTimeout is the default timeout for synchronous audit log queries.
const DefaultAuditQueryTimeout = 15 * time.Second

// AuditQueryOpts configures a single QueryAuditLog call.
type AuditQueryOpts struct {
	// Operation filters by operation name (e.g. "proxy_http_routed", "tunnel_opened").
	Operation string

	// EventType filters by event type (e.g. "message", "connection").
	EventType string

	// Workspace filters to a specific workspace.
	Workspace string

	// ActorID filters by actor identity string.
	ActorID string

	// Limit caps the number of returned entries (default: 100, max: 500).
	Limit int32

	// Timeout caps the wait for the correlated response. Zero uses DefaultAuditQueryTimeout.
	Timeout time.Duration
}

// AuditQueryResult is the SDK-facing result of a QueryAuditLog call.
type AuditQueryResult struct {
	// Success is true when the gateway accepted and executed the query.
	Success bool

	// Error carries the gateway's rejection reason. Empty on success.
	Error string

	// Entries holds the matched audit records.
	Entries []*pb.AuditEntry

	// TotalCount is the number of entries returned (len(Entries)).
	TotalCount int32
}

// QueryAuditLog sends an AuditQuery to the gateway and waits for the response.
// Only system principals are unconditionally permitted; other identities require
// ACL admin_operations or workspace read access on the queried workspace.
func (c *BaseClient) QueryAuditLog(ctx context.Context, opts AuditQueryOpts) (*AuditQueryResult, error) {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultAuditQueryTimeout
	}

	requestID := c.NextRequestID()
	ch := c.pendingAuditQueryRequests.Register(requestID)
	defer c.pendingAuditQueryRequests.Delete(requestID)

	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	query := &pb.AuditQuery{
		RequestId: requestID,
		Operation: opts.Operation,
		EventType: opts.EventType,
		Workspace: opts.Workspace,
		ActorId:   opts.ActorID,
		Limit:     limit,
	}

	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_AuditQuery{AuditQuery: query},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, NewTimeoutError("audit query timed out", timeout.Seconds())
	case resp := <-ch:
		return &AuditQueryResult{
			Success:    resp.GetSuccess(),
			Error:      resp.GetError(),
			Entries:    resp.GetEntries(),
			TotalCount: resp.GetTotalCount(),
		}, nil
	}
}

// handleAuditQueryResponse resolves a pending audit query request in the dispatch loop.
func (c *BaseClient) handleAuditQueryResponse(_ context.Context, resp *pb.AuditQueryResponse) error {
	if resp == nil {
		return nil
	}
	c.pendingAuditQueryRequests.Resolve(resp.GetRequestId(), resp)
	return nil
}
