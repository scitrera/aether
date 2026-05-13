// Package aether token operations for the Go SDK.
//
// This file provides TokenOps, a helper for managing API tokens via the
// Aether gateway's TokenOperation protocol.
//
// Operations can be performed in two modes:
//   - Async: Fire-and-forget via SendOp; responses delivered to OnTokenResponse
//   - Sync: Blocking via SendOpSync; waits for correlated response with timeout

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// TokenOps provides token management operations on a client.
type TokenOps struct {
	client *BaseClient
	syncMu sync.Mutex // serializes synchronous token operations
}

// newTokenOps creates a new TokenOps helper for a client.
func newTokenOps(client *BaseClient) *TokenOps {
	return &TokenOps{client: client}
}

// =============================================================================
// Async Operation
// =============================================================================

// SendOp sends a token operation asynchronously.
// The response is delivered via the OnTokenResponse handler callback.
func (t *TokenOps) SendOp(op *pb.TokenOperation) error {
	return t.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TokenOp{TokenOp: op},
	})
}

// =============================================================================
// Synchronous Operation
// =============================================================================

// SendOpSync sends a token operation and waits for the correlated response.
func (t *TokenOps) SendOpSync(ctx context.Context, op *pb.TokenOperation, timeout time.Duration) (*TokenResponse, error) {
	t.syncMu.Lock()
	defer t.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultWorkflowTimeout
	}

	requestID := t.client.NextRequestID()
	op.RequestId = requestID
	ch := t.client.RegisterPendingTokenRequest(requestID)
	defer t.client.pendingTokenRequests.Delete(requestID)

	if err := t.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("token operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// =============================================================================
// Convenience Methods
// =============================================================================

// List lists all API tokens.
func (t *TokenOps) List(ctx context.Context) (*TokenResponse, error) {
	return t.SendOpSync(ctx, &pb.TokenOperation{
		Op: pb.TokenOperation_LIST,
	}, 0)
}

// ListWithFilter lists API tokens with pagination.
func (t *TokenOps) ListWithFilter(ctx context.Context, limit, offset int32) (*TokenResponse, error) {
	return t.SendOpSync(ctx, &pb.TokenOperation{
		Op: pb.TokenOperation_LIST,
		Filter: &pb.TokenFilter{
			Limit:  limit,
			Offset: offset,
		},
	}, 0)
}

// Get retrieves a specific API token by ID.
func (t *TokenOps) Get(ctx context.Context, tokenID string) (*TokenResponse, error) {
	return t.SendOpSync(ctx, &pb.TokenOperation{
		Op:      pb.TokenOperation_GET,
		TokenId: tokenID,
	}, 0)
}

// Create creates a new API token.
func (t *TokenOps) Create(ctx context.Context, name, principalType string, workspacePatterns, scopes []string, expiresInHours int32, createdBy string) (*TokenResponse, error) {
	return t.SendOpSync(ctx, &pb.TokenOperation{
		Op: pb.TokenOperation_CREATE,
		CreateRequest: &pb.TokenCreateRequest{
			Name:              name,
			PrincipalType:     principalType,
			WorkspacePatterns: workspacePatterns,
			Scopes:            scopes,
			ExpiresInHours:    expiresInHours,
			CreatedBy:         createdBy,
		},
	}, 0)
}

// Delete deletes an API token by ID.
func (t *TokenOps) Delete(ctx context.Context, tokenID string) (*TokenResponse, error) {
	return t.SendOpSync(ctx, &pb.TokenOperation{
		Op:      pb.TokenOperation_DELETE,
		TokenId: tokenID,
	}, 0)
}

// Revoke revokes an API token by ID.
func (t *TokenOps) Revoke(ctx context.Context, tokenID string) (*TokenResponse, error) {
	return t.SendOpSync(ctx, &pb.TokenOperation{
		Op:      pb.TokenOperation_REVOKE,
		TokenId: tokenID,
	}, 0)
}
