// Phase 2 Stage B sqlite-native authority-request lifecycle.
//
// This file mirrors internal/acl/authority_request_lifecycle.go but hangs
// off the sqlite-native Store. Stage A added the storage CRUD (sibling
// authority_requests.go in this package); Stage B layers Submit / Approve /
// Deny / Cancel / Sweep on top, calling Store.CreateAuthorityGrant for the
// mint and Store.ResolveAuthorityRequest for the row flip.
//
// Aetherlite parity (constraint from the Stage B prompt): the legacy
// *acl.Service path holds the canonical lifecycle definition; this file
// reuses the same constants (acl.MaxAuthorityRequestDurationSeconds, etc.),
// the same intersection helpers (acl.IntersectStringSlices,
// acl.IntersectResourceScope), and the same audit-emit method
// (acl.AuditLogger.LogAuthorityRequestEvent) so both backends emit identical
// audit shapes and obey identical refinement rules.

package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/logging"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	"github.com/scitrera/aether/pkg/models"
)

// SubmitAuthorityRequest validates the request payload, fills in
// server-managed fields, persists the row, and emits a "created" audit
// event. See acl.Service.SubmitAuthorityRequest godoc for the canonical
// semantic contract; this is the sqlite-native sibling.
func (s *Store) SubmitAuthorityRequest(ctx context.Context, req *acl.AuthorityRequest) (*acl.AuthorityRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", acl.ErrAuthorityRequestInvalid)
	}
	if req.RequestingActor.PrincipalRef().IsZero() {
		return nil, fmt.Errorf("%w: requesting_actor is required", acl.ErrAuthorityRequestInvalid)
	}
	if req.RoutingTarget.IsEmpty() {
		return nil, fmt.Errorf("%w: routing_target is required", acl.ErrAuthorityRequestInvalid)
	}
	if req.RoutingTarget.Capability != "" && req.RoutingTarget.Principal != nil {
		if ref := req.RoutingTarget.Principal.PrincipalRef(); !ref.IsZero() {
			return nil, fmt.Errorf("%w: routing_target must set exactly one of principal or capability", acl.ErrAuthorityRequestInvalid)
		}
	}
	if req.DurationSeconds <= 0 {
		return nil, fmt.Errorf("%w: duration_seconds must be > 0", acl.ErrAuthorityRequestInvalid)
	}
	if err := acl.ValidateAccessLevel(req.RequestedAccess); err != nil {
		return nil, fmt.Errorf("%w: requested_access: %v", acl.ErrAuthorityRequestInvalid, err)
	}

	if req.DurationSeconds > acl.MaxAuthorityRequestDurationSeconds {
		req.DurationSeconds = acl.MaxAuthorityRequestDurationSeconds
	}

	now := time.Now().UTC()
	if req.CreatedAt.IsZero() {
		req.CreatedAt = now
	}
	if req.ExpiresAt.IsZero() {
		req.ExpiresAt = req.CreatedAt.Add(time.Duration(acl.DefaultAuthorityRequestTimeoutSeconds) * time.Second)
	}
	if req.Status == "" {
		req.Status = acl.AuthorityRequestStatusPending
	}

	if err := s.CreateAuthorityRequest(ctx, req); err != nil {
		return nil, err
	}

	s.aclAudit.LogAuthorityRequestEvent(ctx, req, acl.OpAuthorityRequestCreated, req.RequestingActor, nil)
	return req, nil
}

// ApproveAuthorityRequest mints a grant via Store.CreateAuthorityGrant and
// flips the request row. Scope refinements in `decision` are intersected
// with the request (approvers cannot broaden). See
// acl.Service.ApproveAuthorityRequest for the canonical doc.
func (s *Store) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *acl.ApproveDecision) (*acl.AuthorityRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("%w: request_id is required", acl.ErrAuthorityRequestInvalid)
	}
	if approverIdentity.PrincipalRef().IsZero() {
		return nil, fmt.Errorf("%w: approver identity is required", acl.ErrAuthorityRequestInvalid)
	}
	if decision == nil {
		decision = &acl.ApproveDecision{}
	}

	req, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if req.Status.IsTerminal() {
		return nil, acl.ErrAuthorityRequestAlreadyResolved
	}

	subject := req.RequestingActor
	if !req.TargetSubject.PrincipalRef().IsZero() {
		subject = req.TargetSubject
	}

	workspaceScope := acl.IntersectStringSlices(req.WorkspaceScope, decision.GrantedWorkspaceScope)
	operationScope := acl.IntersectStringSlices(req.OperationScope, decision.GrantedOperationScope)
	resourceScope := acl.IntersectResourceScope(req.ResourceScope, decision.GrantedResourceScope)

	maxAccess := req.RequestedAccess
	if decision.GrantedAccessLevel > 0 && decision.GrantedAccessLevel < maxAccess {
		maxAccess = decision.GrantedAccessLevel
	}
	if err := acl.ValidateAccessLevel(maxAccess); err != nil {
		return nil, fmt.Errorf("%w: effective access level invalid: %v", acl.ErrAuthorityRequestInvalid, err)
	}

	durationSeconds := req.DurationSeconds
	if decision.GrantedDurationSeconds > 0 && decision.GrantedDurationSeconds < durationSeconds {
		durationSeconds = decision.GrantedDurationSeconds
	}
	if durationSeconds > acl.MaxAuthorityRequestDurationSeconds {
		durationSeconds = acl.MaxAuthorityRequestDurationSeconds
	}
	if durationSeconds <= 0 {
		return nil, fmt.Errorf("%w: effective duration must be > 0", acl.ErrAuthorityRequestInvalid)
	}

	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(durationSeconds) * time.Second)

	metadata := make(map[string]interface{}, len(req.Metadata)+len(decision.Metadata))
	for k, v := range req.Metadata {
		metadata[k] = v
	}
	for k, v := range decision.Metadata {
		metadata[k] = v
	}

	reason := strings.TrimSpace(decision.Reason)
	if reason == "" {
		reason = req.Reason
	}

	grantReq := aclstore.CreateAuthorityGrantRequest{
		Subject:                  subject,
		Delegate:                 req.RequestingActor,
		IssuedBy:                 approverIdentity,
		MayDelegate:              decision.MayDelegate,
		RemainingHops:            decision.RemainingHops,
		WorkspaceScope:           workspaceScope,
		ResourceScope:            resourceScope,
		OperationScope:           operationScope,
		MaxAccessLevel:           maxAccess,
		AudienceType:             req.AudienceType,
		AudienceID:               req.AudienceID,
		ValidWhileAudienceActive: true,
		ExpiresAt:                expiresAt,
		RenewableUntil:           expiresAt,
		Reason:                   reason,
		Metadata:                 metadata,
	}

	grant, err := s.CreateAuthorityGrant(ctx, grantReq)
	if err != nil {
		return nil, fmt.Errorf("authority approval: mint grant: %w", err)
	}

	if err := s.ResolveAuthorityRequest(ctx, requestID,
		acl.AuthorityRequestStatusApproved, approverIdentity,
		decision.Reason, grant.GrantID, now,
	); err != nil {
		logging.Logger.Warn().Err(err).
			Str("request_id", requestID).
			Str("grant_id", grant.GrantID).
			Msg("acl: authority request row-flip failed after grant mint; grant remains valid")
		return nil, fmt.Errorf("authority approval: resolve request row: %w", err)
	}

	resolved, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	s.aclAudit.LogAuthorityRequestEvent(ctx, resolved, acl.OpAuthorityRequestApproved, approverIdentity,
		map[string]interface{}{"granted_grant_id": grant.GrantID})
	return resolved, nil
}

// DenyAuthorityRequest flips PENDING → DENIED and emits an audit event.
func (s *Store) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*acl.AuthorityRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("%w: request_id is required", acl.ErrAuthorityRequestInvalid)
	}
	if approverIdentity.PrincipalRef().IsZero() {
		return nil, fmt.Errorf("%w: approver identity is required", acl.ErrAuthorityRequestInvalid)
	}
	if err := s.ResolveAuthorityRequest(ctx, requestID,
		acl.AuthorityRequestStatusDenied, approverIdentity,
		reason, "", time.Now().UTC(),
	); err != nil {
		return nil, err
	}
	resolved, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	s.aclAudit.LogAuthorityRequestEvent(ctx, resolved, acl.OpAuthorityRequestDenied, approverIdentity, nil)
	return resolved, nil
}

// CancelOpenAuthorityRequest is the lifecycle wrapper around the storage-
// layer CancelAuthorityRequest. See acl.Service.CancelOpenAuthorityRequest
// for the rationale on the distinct method name.
func (s *Store) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*acl.AuthorityRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("%w: request_id is required", acl.ErrAuthorityRequestInvalid)
	}
	if err := s.CancelAuthorityRequest(ctx, requestID, reason); err != nil {
		return nil, err
	}
	resolved, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	s.aclAudit.LogAuthorityRequestEvent(ctx, resolved, acl.OpAuthorityRequestCancelled, resolved.RequestingActor, nil)
	return resolved, nil
}

// SweepExpiredAuthorityRequests sweeps expired pending rows and emits an
// audit event per swept row.
func (s *Store) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*acl.AuthorityRequest, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expired, err := s.ExpireAuthorityRequests(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	for _, r := range expired {
		s.aclAudit.LogAuthorityRequestEvent(ctx, r, acl.OpAuthorityRequestExpired, models.Identity{}, nil)
	}
	return expired, nil
}
