// Package aether authority grant operations for the Go SDK.
//
// This file provides AuthorityGrantOps, a helper for managing runtime
// authority grants via the Aether gateway's AuthorityGrantOperation
// protocol. The high-level surface mirrors the Python async client's
// methods (exchange/derive/get/renew/revoke/list/batch/derive_for_target)
// plus the cache hint surfaces added in Phase 3.
//
// Operations can be performed in two modes:
//   - Async: Fire-and-forget via SendOp; responses delivered to OnAuthorityGrantResponse
//   - Sync: Blocking via SendOpSync; waits for correlated response with timeout

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultAuthorityGrantTimeout is the default timeout for synchronous
// authority-grant operations. Mirrors the Python client default.
const DefaultAuthorityGrantTimeout = 10 * time.Second

// AuthorityGrantOps provides runtime authority-grant management on a client.
type AuthorityGrantOps struct {
	client *BaseClient
	syncMu sync.Mutex // serializes synchronous authority-grant operations
}

// newAuthorityGrantOps creates a new AuthorityGrantOps helper for a client.
func newAuthorityGrantOps(client *BaseClient) *AuthorityGrantOps {
	return &AuthorityGrantOps{client: client}
}

// =============================================================================
// Async Operation
// =============================================================================

// SendOp sends an authority-grant operation asynchronously.
// The response is delivered via the OnAuthorityGrantResponse handler callback.
func (a *AuthorityGrantOps) SendOp(op *pb.AuthorityGrantOperation) error {
	return a.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_AuthorityGrantOp{AuthorityGrantOp: op},
	})
}

// =============================================================================
// Synchronous Operation
// =============================================================================

// SendOpSync sends an authority-grant operation and waits for the correlated
// response. A zero timeout uses DefaultAuthorityGrantTimeout.
func (a *AuthorityGrantOps) SendOpSync(ctx context.Context, op *pb.AuthorityGrantOperation, timeout time.Duration) (*pb.AuthorityGrantResponse, error) {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultAuthorityGrantTimeout
	}

	requestID := a.client.NextRequestID()
	op.RequestId = requestID
	ch := a.client.RegisterPendingAuthorityGrantRequest(requestID)
	defer a.client.pendingAuthorityGrantRequests.Delete(requestID)

	if err := a.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("authority grant operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// =============================================================================
// Convenience Methods
// =============================================================================

// ExchangeOpts configures an EXCHANGE op. Mirrors the Python keyword args on
// AsyncServiceClient.exchange_authority_grant.
type ExchangeOpts struct {
	WorkspaceScope           []string
	ResourceScope            []*pb.ACLAuthorityGrantResourceScopeEntry
	OperationScope           []string
	MaxAccessLevel           int32
	AudienceType             string
	AudienceID               string
	ValidWhileAudienceActive bool
	ExpiresAt                int64
	RenewableUntil           int64
	MayDelegate              bool
	RemainingHops            int32
	Reason                   string
	Metadata                 map[string]string
	Timeout                  time.Duration
}

// Exchange bootstraps a runtime authority grant for the current actor.
func (a *AuthorityGrantOps) Exchange(ctx context.Context, sourceSessionID string, opts ExchangeOpts) (*pb.AuthorityGrantResponse, error) {
	op := &pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_EXCHANGE,
		ExchangeRequest: &pb.AuthorityGrantExchangeRequest{
			SourceSessionId:          sourceSessionID,
			WorkspaceScope:           opts.WorkspaceScope,
			ResourceScope:            opts.ResourceScope,
			OperationScope:           opts.OperationScope,
			MaxAccessLevel:           opts.MaxAccessLevel,
			AudienceType:             opts.AudienceType,
			AudienceId:               opts.AudienceID,
			ValidWhileAudienceActive: opts.ValidWhileAudienceActive,
			ExpiresAt:                opts.ExpiresAt,
			RenewableUntil:           opts.RenewableUntil,
			MayDelegate:              opts.MayDelegate,
			RemainingHops:            opts.RemainingHops,
			Reason:                   opts.Reason,
			Metadata:                 opts.Metadata,
		},
	}
	return a.SendOpSync(ctx, op, opts.Timeout)
}

// DeriveOpts configures a DERIVE op. Mirrors the Python keyword args on
// AsyncServiceClient.derive_authority_grant.
type DeriveOpts struct {
	WorkspaceScope           []string
	ResourceScope            []*pb.ACLAuthorityGrantResourceScopeEntry
	OperationScope           []string
	MaxAccessLevel           int32
	AudienceType             string
	AudienceID               string
	ValidWhileAudienceActive bool
	ExpiresAt                int64
	RenewableUntil           int64
	MayDelegate              bool
	RemainingHops            int32
	Reason                   string
	Metadata                 map[string]string
	Timeout                  time.Duration
}

// Derive derives a child authority grant from an existing parent grant.
func (a *AuthorityGrantOps) Derive(ctx context.Context, parentGrantID, delegateType, delegateID string, opts DeriveOpts) (*pb.AuthorityGrantResponse, error) {
	op := &pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_DERIVE,
		DeriveRequest: &pb.AuthorityGrantDeriveRequest{
			ParentGrantId: parentGrantID,
			Delegate: &pb.PrincipalRef{
				PrincipalType: delegateType,
				PrincipalId:   delegateID,
			},
			WorkspaceScope:           opts.WorkspaceScope,
			ResourceScope:            opts.ResourceScope,
			OperationScope:           opts.OperationScope,
			MaxAccessLevel:           opts.MaxAccessLevel,
			AudienceType:             opts.AudienceType,
			AudienceId:               opts.AudienceID,
			ValidWhileAudienceActive: opts.ValidWhileAudienceActive,
			ExpiresAt:                opts.ExpiresAt,
			RenewableUntil:           opts.RenewableUntil,
			MayDelegate:              opts.MayDelegate,
			RemainingHops:            opts.RemainingHops,
			Reason:                   opts.Reason,
			Metadata:                 opts.Metadata,
		},
	}
	return a.SendOpSync(ctx, op, opts.Timeout)
}

// Get returns a runtime authority grant by ID.
func (a *AuthorityGrantOps) Get(ctx context.Context, grantID string) (*pb.AuthorityGrantResponse, error) {
	return a.SendOpSync(ctx, &pb.AuthorityGrantOperation{
		Op:      pb.AuthorityGrantOperation_GET,
		GrantId: grantID,
	}, 0)
}

// Renew renews a runtime authority grant lease.
//
// Pass expiresAt as an absolute new expiry (proto units), or 0 plus a
// non-zero extendSeconds to ask the server to extend the current expiry by
// N seconds (server clamps against renewable_until). expiresAt takes
// precedence when non-zero.
func (a *AuthorityGrantOps) Renew(ctx context.Context, grantID string, expiresAt int64, extendSeconds int32) (*pb.AuthorityGrantResponse, error) {
	return a.SendOpSync(ctx, &pb.AuthorityGrantOperation{
		Op:      pb.AuthorityGrantOperation_RENEW,
		GrantId: grantID,
		RenewRequest: &pb.ACLRenewAuthorityGrantRequest{
			GrantId:       grantID,
			ExpiresAt:     expiresAt,
			ExtendSeconds: extendSeconds,
		},
	}, 0)
}

// Revoke revokes a runtime authority grant by ID.
func (a *AuthorityGrantOps) Revoke(ctx context.Context, grantID string) (*pb.AuthorityGrantResponse, error) {
	return a.SendOpSync(ctx, &pb.AuthorityGrantOperation{
		Op:      pb.AuthorityGrantOperation_REVOKE,
		GrantId: grantID,
	}, 0)
}

// ListOpts paginates and filters list operations.
type ListOpts struct {
	AudienceType   string
	AudienceID     string
	IncludeRevoked bool
	Limit          int32
	Offset         int32
	Timeout        time.Duration
}

// ListMyGrants lists grants where the actor is delegate or subject.
func (a *AuthorityGrantOps) ListMyGrants(ctx context.Context, opts ListOpts) (*pb.AuthorityGrantResponse, error) {
	return a.SendOpSync(ctx, &pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_LIST_MY_GRANTS,
		ListRequest: &pb.AuthorityGrantListRequest{
			AudienceType:   opts.AudienceType,
			AudienceId:     opts.AudienceID,
			IncludeRevoked: opts.IncludeRevoked,
			Limit:          opts.Limit,
			Offset:         opts.Offset,
		},
	}, opts.Timeout)
}

// ListGrantsOnMe lists grants where the actor is the subject (i.e., grants
// OTHERS hold on me).
func (a *AuthorityGrantOps) ListGrantsOnMe(ctx context.Context, opts ListOpts) (*pb.AuthorityGrantResponse, error) {
	return a.SendOpSync(ctx, &pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_LIST_GRANTS_ON_ME,
		ListRequest: &pb.AuthorityGrantListRequest{
			AudienceType:   opts.AudienceType,
			AudienceId:     opts.AudienceID,
			IncludeRevoked: opts.IncludeRevoked,
			Limit:          opts.Limit,
			Offset:         opts.Offset,
		},
	}, opts.Timeout)
}

// BatchExchange exchanges multiple authority grants in a single round-trip.
func (a *AuthorityGrantOps) BatchExchange(ctx context.Context, requests []*pb.AuthorityGrantExchangeRequest, stopOnFirstError bool, timeout time.Duration) (*pb.AuthorityGrantResponse, error) {
	return a.SendOpSync(ctx, &pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_BATCH_EXCHANGE,
		BatchExchangeRequest: &pb.AuthorityGrantBatchExchangeRequest{
			Requests:         requests,
			StopOnFirstError: stopOnFirstError,
		},
	}, timeout)
}

// DeriveForTargetOpts mirrors the Python keyword args on
// AsyncServiceClient.derive_authority_grant_for_target.
type DeriveForTargetOpts struct {
	AudienceType   string
	AudienceID     string
	OperationScope []string
	MaxAccessLevel int32
	ExpiresAt      int64
	RenewableUntil int64
	MayDelegate    bool
	RemainingHops  int32
	Reason         string
	Timeout        time.Duration
}

// DeriveForTarget idempotently derives a child grant: returns an existing
// visible grant matching (parent, target, audience) or mints a new one.
//
// Safe to call repeatedly without leaking grants — the gateway de-duplicates
// against the actor's currently visible derived grants.
func (a *AuthorityGrantOps) DeriveForTarget(ctx context.Context, parentGrantID, targetType, targetID string, opts DeriveForTargetOpts) (*pb.AuthorityGrantResponse, error) {
	op := &pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_DERIVE_FOR_TARGET,
		DeriveForTargetRequest: &pb.AuthorityGrantDeriveForTargetRequest{
			ParentGrantId: parentGrantID,
			Target: &pb.PrincipalRef{
				PrincipalType: targetType,
				PrincipalId:   targetID,
			},
			AudienceType:   opts.AudienceType,
			AudienceId:     opts.AudienceID,
			OperationScope: opts.OperationScope,
			MaxAccessLevel: opts.MaxAccessLevel,
			ExpiresAt:      opts.ExpiresAt,
			RenewableUntil: opts.RenewableUntil,
			MayDelegate:    opts.MayDelegate,
			RemainingHops:  opts.RemainingHops,
			Reason:         opts.Reason,
		},
	}
	return a.SendOpSync(ctx, op, opts.Timeout)
}
