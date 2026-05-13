// Package aether admin-query and session-management ops for the Go SDK.
//
// This file adds two BaseClient helpers that mirror the existing
// WorkspaceOps / AgentOps / ACLOps pattern:
//
//   - AdminOps wraps AdminQuery / AdminResponse for gateway-wide queries
//     (health, info, stats, list/get connections).
//   - SessionOps wraps SessionOperation / SessionOperationResponse for
//     session management (DISCONNECT, LIST, GET).
//
// Both surfaces support an asynchronous SendOp and a synchronous SendOpSync
// using request_id correlation. The synchronous calls fall back to
// "resolve first" if the server happens to omit request_id, matching the
// existing helpers.

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// SDK Response Types
// =============================================================================

// AdminResponse represents a response to an AdminQuery (health, info, stats,
// connection queries). The shape mirrors the proto AdminResponse but uses
// SDK-friendly Go types.
type AdminResponse struct {
	Success bool
	Error   string

	// Health is populated for GET_HEALTH queries.
	Health *pb.HealthInfo

	// Info is populated for GET_INFO queries.
	Info *pb.GatewayInfo

	// Stats is populated for GET_STATS queries.
	Stats *pb.GatewayStats

	// Connection is populated for GET_CONNECTION queries (single).
	Connection *pb.ConnectionInfo

	// Connections is populated for LIST_CONNECTIONS queries.
	Connections []*pb.ConnectionInfo

	// TotalCount is the count of connections matching the filter (may
	// differ from len(Connections) when paginated).
	TotalCount int32

	// RequestId is the correlation ID echoed from the originating query.
	RequestId string
}

// SessionOperationResponse represents a response to a SessionOperation.
type SessionOperationResponse struct {
	Success bool
	Message string
	Error   string

	// Connection is populated for GET (single session).
	Connection *pb.ConnectionInfo

	// Connections is populated for LIST.
	Connections []*pb.ConnectionInfo

	// TotalCount is the count of sessions matching the filter before
	// pagination is applied.
	TotalCount int32

	// RequestId is the correlation ID echoed from the originating
	// SessionOperation.
	RequestId string
}

// AdminResponseHandler is invoked when an unsolicited AdminResponse arrives
// (no matching pending request).
type AdminResponseHandler func(ctx context.Context, resp *AdminResponse) error

// SessionResponseHandler is invoked when an unsolicited
// SessionOperationResponse arrives (no matching pending request).
type SessionResponseHandler func(ctx context.Context, resp *SessionOperationResponse) error

// =============================================================================
// AdminOps
// =============================================================================

// AdminOps provides admin-query (health/info/stats/connections) operations
// on a BaseClient.
type AdminOps struct {
	client *BaseClient
	syncMu sync.Mutex
}

// newAdminOps creates a new AdminOps helper for a client.
func newAdminOps(client *BaseClient) *AdminOps {
	return &AdminOps{client: client}
}

// SendOp sends an admin query asynchronously.
// The response is delivered via OnAdminResponse.
func (o *AdminOps) SendOp(op *pb.AdminQuery) error {
	return o.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_AdminQuery{AdminQuery: op},
	})
}

// SendOpSync sends an admin query and waits for the correlated response.
func (o *AdminOps) SendOpSync(ctx context.Context, op *pb.AdminQuery, timeout time.Duration) (*AdminResponse, error) {
	o.syncMu.Lock()
	defer o.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultAdminTimeout
	}

	requestID := o.client.NextRequestID()
	op.RequestId = requestID
	ch := o.client.RegisterPendingAdminRequest(requestID)
	defer o.client.pendingAdminRequests.Delete(requestID)

	if err := o.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("admin query timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// Admin returns the AdminOps helper for this client.
func (c *BaseClient) Admin() *AdminOps {
	c.adminOnce.Do(func() {
		c.adminInstance = newAdminOps(c)
	})
	return c.adminInstance
}

// =============================================================================
// SessionOps
// =============================================================================

// SessionOps provides session-management operations on a BaseClient.
type SessionOps struct {
	client *BaseClient
	syncMu sync.Mutex
}

// newSessionOps creates a new SessionOps helper for a client.
func newSessionOps(client *BaseClient) *SessionOps {
	return &SessionOps{client: client}
}

// SendOp sends a session operation asynchronously.
// The response is delivered via OnSessionResponse.
func (o *SessionOps) SendOp(op *pb.SessionOperation) error {
	return o.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_SessionOp{SessionOp: op},
	})
}

// SendOpSync sends a session operation and waits for the correlated
// response.
func (o *SessionOps) SendOpSync(ctx context.Context, op *pb.SessionOperation, timeout time.Duration) (*SessionOperationResponse, error) {
	o.syncMu.Lock()
	defer o.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultAdminTimeout
	}

	requestID := o.client.NextRequestID()
	op.RequestId = requestID
	ch := o.client.RegisterPendingSessionRequest(requestID)
	defer o.client.pendingSessionRequests.Delete(requestID)

	if err := o.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("session operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// Disconnect forcibly disconnects a session by session ID.
func (o *SessionOps) Disconnect(ctx context.Context, sessionID, reason string) (*SessionOperationResponse, error) {
	return o.SendOpSync(ctx, &pb.SessionOperation{
		Op:        pb.SessionOperation_DISCONNECT,
		SessionId: sessionID,
		Reason:    reason,
	}, 0)
}

// List lists active sessions with an optional filter.
func (o *SessionOps) List(ctx context.Context, filter *pb.ConnectionFilter) (*SessionOperationResponse, error) {
	return o.SendOpSync(ctx, &pb.SessionOperation{
		Op:     pb.SessionOperation_LIST,
		Filter: filter,
	}, 0)
}

// Get retrieves a single session by ID.
func (o *SessionOps) Get(ctx context.Context, sessionID string) (*SessionOperationResponse, error) {
	return o.SendOpSync(ctx, &pb.SessionOperation{
		Op:        pb.SessionOperation_GET,
		SessionId: sessionID,
	}, 0)
}

// Session returns the SessionOps helper for this client.
func (c *BaseClient) Session() *SessionOps {
	c.sessionOnce.Do(func() {
		c.sessionInstance = newSessionOps(c)
	})
	return c.sessionInstance
}

// =============================================================================
// Pending-request plumbing (called from client.go dispatchResponse)
// =============================================================================

// RegisterPendingAdminRequest registers a pending admin-query request
// channel keyed by request ID.
func (c *BaseClient) RegisterPendingAdminRequest(requestID string) chan *AdminResponse {
	return c.pendingAdminRequests.Register(requestID)
}

// ResolvePendingAdminRequest delivers an admin response to the pending
// channel keyed by request ID. Returns true if a pending request was found.
func (c *BaseClient) ResolvePendingAdminRequest(requestID string, resp *AdminResponse) bool {
	return c.pendingAdminRequests.Resolve(requestID, resp)
}

// RegisterPendingSessionRequest registers a pending session-operation
// request channel keyed by request ID.
func (c *BaseClient) RegisterPendingSessionRequest(requestID string) chan *SessionOperationResponse {
	return c.pendingSessionRequests.Register(requestID)
}

// ResolvePendingSessionRequest delivers a session response to the pending
// channel keyed by request ID. Returns true if a pending request was found.
func (c *BaseClient) ResolvePendingSessionRequest(requestID string, resp *SessionOperationResponse) bool {
	return c.pendingSessionRequests.Resolve(requestID, resp)
}

// =============================================================================
// Async handler registration
// =============================================================================

// OnAdminResponse registers a handler for unsolicited AdminResponse
// messages (no matching pending request). Useful when the gateway pushes a
// response that the caller did not initiate via SendOpSync.
func (c *BaseClient) OnAdminResponse(handler AdminResponseHandler) {
	c.handlers.OnAdminResponse = handler
}

// OnSessionResponse registers a handler for unsolicited
// SessionOperationResponse messages.
func (c *BaseClient) OnSessionResponse(handler SessionResponseHandler) {
	c.handlers.OnSessionResponse = handler
}

// =============================================================================
// Proto-to-SDK conversion + dispatch helpers (called from client.go)
// =============================================================================

// protoAdminResponseToSDK converts a protobuf AdminResponse to the SDK type.
func protoAdminResponseToSDK(resp *pb.AdminResponse) *AdminResponse {
	if resp == nil {
		return nil
	}
	return &AdminResponse{
		Success:     resp.GetSuccess(),
		Error:       resp.GetError(),
		Health:      resp.GetHealth(),
		Info:        resp.GetInfo(),
		Stats:       resp.GetStats(),
		Connection:  resp.GetConnection(),
		Connections: resp.GetConnections(),
		TotalCount:  resp.GetTotalCount(),
		RequestId:   resp.GetRequestId(),
	}
}

// protoSessionResponseToSDK converts a protobuf SessionOperationResponse to
// the SDK type.
func protoSessionResponseToSDK(resp *pb.SessionOperationResponse) *SessionOperationResponse {
	if resp == nil {
		return nil
	}
	return &SessionOperationResponse{
		Success:     resp.GetSuccess(),
		Message:     resp.GetMessage(),
		Error:       resp.GetError(),
		Connection:  resp.GetConnection(),
		Connections: resp.GetConnections(),
		TotalCount:  resp.GetTotalCount(),
		RequestId:   resp.GetRequestId(),
	}
}

// handleAdminResponse processes an admin-query response from the server.
// Called from BaseClient.dispatchResponse for DownstreamMessage_Admin.
func (c *BaseClient) handleAdminResponse(ctx context.Context, resp *pb.AdminResponse) error {
	sdkResp := protoAdminResponseToSDK(resp)

	if reqID := resp.GetRequestId(); reqID != "" {
		if c.ResolvePendingAdminRequest(reqID, sdkResp) {
			return nil
		}
	}
	// Fallback: deliver to first pending request, then fire handler.
	c.pendingAdminRequests.ResolveFirst(sdkResp)
	if c.handlers.OnAdminResponse != nil {
		return c.handlers.OnAdminResponse(ctx, sdkResp)
	}
	return nil
}

// handleSessionResponse processes a session-operation response from the
// server. Called from BaseClient.dispatchResponse for
// DownstreamMessage_SessionResponse.
func (c *BaseClient) handleSessionResponse(ctx context.Context, resp *pb.SessionOperationResponse) error {
	sdkResp := protoSessionResponseToSDK(resp)

	if reqID := resp.GetRequestId(); reqID != "" {
		if c.ResolvePendingSessionRequest(reqID, sdkResp) {
			return nil
		}
	}
	c.pendingSessionRequests.ResolveFirst(sdkResp)
	if c.handlers.OnSessionResponse != nil {
		return c.handlers.OnSessionResponse(ctx, sdkResp)
	}
	return nil
}
