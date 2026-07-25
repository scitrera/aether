// Package aether connection-status query op for the Go SDK.
//
// This file adds a thin BaseClient helper that mirrors the Python async
// client's connection_status() method. It asks the gateway whether a given
// principal currently holds a live session lock.
//
// Self-checks (principal == caller's identity) are trivially allowed by the
// gateway; cross-principal checks require the caller to hold a
// `capability/query_connections` READ grant.
//
// The op follows the same request_id-correlated SendOpSync pattern used by
// AuthorityGrantOps: the raw protobuf ConnectionStatusResponse is returned
// regardless of `ok`, so callers interpret `ok=false` (unknown principal,
// permission denied) and the embedded `error` themselves. `connected` is the
// authoritative presence boolean; `last_seen_at` is best-effort and may be 0.

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultConnectionStatusTimeout is the default timeout for synchronous
// connection-status queries. Mirrors the Python client default (10s).
const DefaultConnectionStatusTimeout = 10 * time.Second

// ConnectionOps provides connection-status queries on a BaseClient.
type ConnectionOps struct {
	client *BaseClient
	syncMu sync.Mutex // serializes synchronous connection-status queries
}

// newConnectionOps creates a new ConnectionOps helper for a client.
func newConnectionOps(client *BaseClient) *ConnectionOps {
	return &ConnectionOps{client: client}
}

// Status queries the gateway for the live-connection status of a principal
// and waits for the correlated response. A zero timeout uses
// DefaultConnectionStatusTimeout.
//
// principalType is the REST-form principal kind (e.g. "service", "agent",
// "task"); principalID is the principal's ID. The raw
// ConnectionStatusResponse is returned regardless of `ok`.
func (o *ConnectionOps) Status(ctx context.Context, principalType, principalID string, timeout time.Duration) (*pb.ConnectionStatusResponse, error) {
	o.syncMu.Lock()
	defer o.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultConnectionStatusTimeout
	}

	requestID := o.client.NextRequestID()
	req := &pb.ConnectionStatusRequest{
		RequestId: requestID,
		Principal: &pb.PrincipalRef{
			PrincipalType: principalType,
			PrincipalId:   principalID,
		},
	}

	ch := o.client.RegisterPendingConnectionStatusRequest(requestID)
	defer o.client.pendingConnectionStatusRequests.Delete(requestID)

	if err := o.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ConnectionStatusRequest{ConnectionStatusRequest: req},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("connection status query timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// Connection returns the ConnectionOps helper for this client.
func (c *BaseClient) Connection() *ConnectionOps {
	c.connectionOnce.Do(func() {
		c.connectionInstance = newConnectionOps(c)
	})
	return c.connectionInstance
}

// ConnectionStatus is a convenience wrapper that queries the live-connection
// status of a principal using the default timeout. It mirrors the Python
// async client's connection_status(). The raw ConnectionStatusResponse is
// returned regardless of `ok`; callers must check `resp.GetOk()` and
// `resp.GetError()`. The authoritative presence signal is `resp.GetConnected()`.
func (c *BaseClient) ConnectionStatus(ctx context.Context, principalType, principalID string) (*pb.ConnectionStatusResponse, error) {
	return c.Connection().Status(ctx, principalType, principalID, 0)
}

// =============================================================================
// Pending-request plumbing (called from client.go dispatchResponse)
// =============================================================================

// RegisterPendingConnectionStatusRequest registers a pending
// connection-status request channel keyed by request ID.
func (c *BaseClient) RegisterPendingConnectionStatusRequest(requestID string) chan *pb.ConnectionStatusResponse {
	return c.pendingConnectionStatusRequests.Register(requestID)
}

// ResolvePendingConnectionStatusRequest delivers a connection-status response
// to the pending channel keyed by request ID. Returns true if a pending
// request was found.
func (c *BaseClient) ResolvePendingConnectionStatusRequest(requestID string, resp *pb.ConnectionStatusResponse) bool {
	return c.pendingConnectionStatusRequests.Resolve(requestID, resp)
}

// handleConnectionStatusResponse processes a connection-status response from
// the server. Called from BaseClient.dispatchResponse for
// DownstreamMessage_ConnectionStatusResponse.
func (c *BaseClient) handleConnectionStatusResponse(_ context.Context, resp *pb.ConnectionStatusResponse) error {
	if reqID := resp.GetRequestId(); reqID != "" {
		if c.ResolvePendingConnectionStatusRequest(reqID, resp) {
			return nil
		}
	}
	// Fallback: deliver to first pending request (servers that omit request_id).
	c.pendingConnectionStatusRequests.ResolveFirst(resp)
	return nil
}
