// Phase 2 Stage B: authority-request lifecycle service.
//
// This file layers the Submit / Approve / Deny / Cancel / Sweep flow on top
// of the Stage A storage CRUD methods (authority_requests.go) and the
// existing CreateAuthorityGrant mint path (authority_grants.go). It is the
// thin glue between an approver's decision and the standard authority-grant
// machinery: approval = mint a grant via CreateAuthorityGrant + flip the
// request row via the storage-layer ResolveAuthorityRequest + emit a typed
// audit event. Denial / expiry / cancellation skip the mint and only flip +
// emit.
//
// Stage B does NOT add gateway handlers, the task_waker integration, or the
// Python SDK surface. Those land in Stage C, which calls the methods defined
// here.
//
// Layout choice (option (a) per the Stage B prompt): the lifecycle methods
// hang off *acl.Service rather than a wrapping AuthorityRequestService type.
// Rationale: Stage A already extended *acl.Service with the storage methods
// (authority_requests.go), CreateAuthorityGrant is already on *Service, and
// the audit adapter (*Service.audit) is right there too. A wrapping type
// would add a layer with zero behavioral gain. The aetherlite parity path
// (internal/storage/acl/sqlite.Store) carries a sibling lifecycle file with
// matching method bodies — both implementations satisfy the same shape so
// the Stage C gateway layer can use either backend.

package acl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// Authority-request lifecycle constants.
const (
	// MaxAuthorityRequestDurationSeconds caps the duration any approval can
	// grant. The approver may request a shorter grant via
	// ApproveDecision.GrantedDurationSeconds; broader durations clamp here.
	// 1 hour default — short enough to keep blast radius bounded; deployments
	// that need longer can introduce a per-tenant policy override later.
	MaxAuthorityRequestDurationSeconds int64 = 3600

	// DefaultAuthorityRequestTimeoutSeconds caps how long a request can sit
	// PENDING before SweepExpiredAuthorityRequests transitions it to EXPIRED.
	// Independent of grant duration: this is "how long the request can wait
	// for an approver" vs "if approved, how long the resulting grant lives".
	// 30 minutes default — operators can override per-policy in Stage C+.
	DefaultAuthorityRequestTimeoutSeconds int64 = 1800

	// Lifecycle-event operation strings emitted into
	// comprehensive_audit_log.operation. These are also stamped into the
	// audit event's metadata.request_lifecycle_event field so dashboards can
	// filter regardless of column.
	OpAuthorityRequestCreated   = "authority_request_created"
	OpAuthorityRequestApproved  = "authority_request_approved"
	OpAuthorityRequestDenied    = "authority_request_denied"
	OpAuthorityRequestExpired   = "authority_request_expired"
	OpAuthorityRequestCancelled = "authority_request_cancelled"
)

// ApproveDecision carries the approver's refinements when accepting an
// authority request. Empty / zero fields mean "inherit from the request"
// (no narrowing), so the minimal Approve call is
// `ApproveDecision{Reason: "lgtm"}`.
//
// Approvers cannot broaden scope: each Granted* field is INTERSECTED with
// the requested value during minting. Anything in the granted set that is
// NOT in the requested set is silently dropped; the audit log captures the
// final minted scope so downstream observers see what was actually granted
// (the approver's intent is preserved in ResolutionReason).
type ApproveDecision struct {
	// Reason is the human-readable resolution explanation persisted on the
	// request row (resolution_reason) and emitted in the audit event.
	Reason string

	// GrantedWorkspaceScope narrows req.WorkspaceScope. Empty = inherit.
	GrantedWorkspaceScope []string

	// GrantedResourceScope narrows req.ResourceScope. Empty = inherit.
	// Per-key the inner slices are intersected; keys missing from the
	// granted map (when the granted map is non-empty) are dropped.
	GrantedResourceScope map[string][]string

	// GrantedOperationScope narrows req.OperationScope. Empty = inherit.
	GrantedOperationScope []string

	// GrantedAccessLevel caps req.RequestedAccess. 0 = inherit. Validated
	// against ValidateAccessLevel; the resulting grant's MaxAccessLevel is
	// min(req.RequestedAccess, GrantedAccessLevel) when GrantedAccessLevel
	// is non-zero, otherwise req.RequestedAccess.
	GrantedAccessLevel int

	// GrantedDurationSeconds caps req.DurationSeconds. 0 = inherit.
	// The resulting grant's ExpiresAt = now + min(req.DurationSeconds,
	// GrantedDurationSeconds, MaxAuthorityRequestDurationSeconds).
	GrantedDurationSeconds int64

	// MayDelegate / RemainingHops propagate verbatim to the minted grant.
	// Approvers default to MayDelegate=false; opting in is explicit.
	MayDelegate   bool
	RemainingHops int

	// Metadata is merged into the minted grant's Metadata map.
	Metadata map[string]interface{}
}

// SubmitAuthorityRequest validates the request payload, fills in
// server-managed fields (RequestID, CreatedAt, ExpiresAt, Status), persists
// the row via CreateAuthorityRequest, and emits an audit event
// (EventTypeAuthorityRequest, operation=authority_request_created).
//
// Validation:
//   - req must be non-nil and RequestingActor non-zero
//   - exactly one of RoutingTarget.Principal / RoutingTarget.Capability set
//   - DurationSeconds > 0 (clamped to MaxAuthorityRequestDurationSeconds)
//   - RequestedAccess passes ValidateAccessLevel
//
// Server-managed fields:
//   - RequestID  = uuid (when empty)
//   - CreatedAt  = time.Now().UTC() (when zero)
//   - ExpiresAt  = CreatedAt + min(DefaultAuthorityRequestTimeoutSeconds,
//     server-policy-override) — distinct from grant duration
//   - Status     = AuthorityRequestStatusPending
//
// Returns the persisted request with all fields populated.
func (s *Service) SubmitAuthorityRequest(ctx context.Context, req *AuthorityRequest) (*AuthorityRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrAuthorityRequestInvalid)
	}
	if req.RequestingActor.PrincipalRef().IsZero() {
		return nil, fmt.Errorf("%w: requesting_actor is required", ErrAuthorityRequestInvalid)
	}
	if req.RoutingTarget.IsEmpty() {
		return nil, fmt.Errorf("%w: routing_target is required", ErrAuthorityRequestInvalid)
	}
	// Exactly one of principal / capability — reject ambiguous shapes early.
	if req.RoutingTarget.Capability != "" && req.RoutingTarget.Principal != nil {
		if ref := req.RoutingTarget.Principal.PrincipalRef(); !ref.IsZero() {
			return nil, fmt.Errorf("%w: routing_target must set exactly one of principal or capability", ErrAuthorityRequestInvalid)
		}
	}
	if req.DurationSeconds <= 0 {
		return nil, fmt.Errorf("%w: duration_seconds must be > 0", ErrAuthorityRequestInvalid)
	}
	if err := ValidateAccessLevel(req.RequestedAccess); err != nil {
		return nil, fmt.Errorf("%w: requested_access: %v", ErrAuthorityRequestInvalid, err)
	}

	// Clamp the grant duration to the policy max. Per the prompt: the
	// request-timeout (how long the row can sit pending) is independent
	// of the grant-duration (how long the eventual grant lives).
	if req.DurationSeconds > MaxAuthorityRequestDurationSeconds {
		req.DurationSeconds = MaxAuthorityRequestDurationSeconds
	}

	if req.RequestID == "" {
		// CreateAuthorityRequest will generate one if we leave it empty,
		// but we own this here so the audit event carries the same ID
		// regardless of which storage codepath runs.
		req.RequestID = newAuthorityRequestID()
	}
	now := time.Now().UTC()
	if req.CreatedAt.IsZero() {
		req.CreatedAt = now
	}
	if req.ExpiresAt.IsZero() {
		req.ExpiresAt = req.CreatedAt.Add(time.Duration(DefaultAuthorityRequestTimeoutSeconds) * time.Second)
	}
	if req.Status == "" {
		req.Status = AuthorityRequestStatusPending
	}

	if err := s.CreateAuthorityRequest(ctx, req); err != nil {
		return nil, err
	}

	// Audit emit is fire-and-forget; never block the lifecycle on it.
	s.audit.LogAuthorityRequestEvent(ctx, req, OpAuthorityRequestCreated, req.RequestingActor, nil)

	return req, nil
}

// ApproveAuthorityRequest mints an AuthorityGrant via CreateAuthorityGrant
// and flips the request row to APPROVED. The approver may narrow scope via
// the ApproveDecision payload; broadening is rejected at the intersection
// helpers (silent drop of out-of-scope additions).
//
// Order of operations:
//  1. Re-read the request and assert it is still PENDING. If terminal,
//     return ErrAuthorityRequestAlreadyResolved.
//  2. Build a CreateAuthorityGrantRequest from the request + intersection
//     of the ApproveDecision refinements.
//  3. Call s.CreateAuthorityGrant — if this fails, no row flip happens and
//     the error surfaces unchanged.
//  4. Call s.ResolveAuthorityRequest with the freshly minted grant_id. If
//     this fails, log a warning and surface the error — grants are valid
//     by themselves, so we do NOT roll back the grant. The waker (Stage C)
//     will retry the row flip on its next scan.
//  5. Emit an audit event (operation=authority_request_approved) with the
//     grant_id in metadata, then re-read and return the resolved row.
//
// Authority check: Stage B trusts approverIdentity (the gateway handler
// added in Stage C enforces ACL CheckAccess against the request's routing
// capability or principal before calling this).
func (s *Service) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *ApproveDecision) (*AuthorityRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("%w: request_id is required", ErrAuthorityRequestInvalid)
	}
	if approverIdentity.PrincipalRef().IsZero() {
		return nil, fmt.Errorf("%w: approver identity is required", ErrAuthorityRequestInvalid)
	}
	if decision == nil {
		decision = &ApproveDecision{}
	}

	req, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if req.Status.IsTerminal() {
		return nil, ErrAuthorityRequestAlreadyResolved
	}

	// Subject of the eventual grant: TargetSubject when set (on-behalf-of
	// escalation), otherwise the requester acting in their own name.
	subject := req.RequestingActor
	if !req.TargetSubject.PrincipalRef().IsZero() {
		subject = req.TargetSubject
	}

	// Scope refinements: intersect the requested with the granted (empty
	// granted means "inherit from request").
	workspaceScope := intersectStringSlices(req.WorkspaceScope, decision.GrantedWorkspaceScope)
	operationScope := intersectStringSlices(req.OperationScope, decision.GrantedOperationScope)
	resourceScope := intersectResourceScope(req.ResourceScope, decision.GrantedResourceScope)

	// Access level cap: min(requested, granted) with 0=inherit semantics.
	maxAccess := req.RequestedAccess
	if decision.GrantedAccessLevel > 0 && decision.GrantedAccessLevel < maxAccess {
		maxAccess = decision.GrantedAccessLevel
	}
	if err := ValidateAccessLevel(maxAccess); err != nil {
		return nil, fmt.Errorf("%w: effective access level invalid: %v", ErrAuthorityRequestInvalid, err)
	}

	// Duration cap: min(requested, granted, policy-max).
	durationSeconds := req.DurationSeconds
	if decision.GrantedDurationSeconds > 0 && decision.GrantedDurationSeconds < durationSeconds {
		durationSeconds = decision.GrantedDurationSeconds
	}
	if durationSeconds > MaxAuthorityRequestDurationSeconds {
		durationSeconds = MaxAuthorityRequestDurationSeconds
	}
	if durationSeconds <= 0 {
		return nil, fmt.Errorf("%w: effective duration must be > 0", ErrAuthorityRequestInvalid)
	}

	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(durationSeconds) * time.Second)

	// Merge request + approver metadata. Approver values win on key collision
	// (the approver is annotating the final grant; the request's metadata is
	// the original ask).
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

	grantReq := CreateAuthorityGrantRequest{
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

	// Flip the request row. If this fails, the grant remains valid by itself
	// — the request will look pending until the waker (Stage C) retries the
	// sweep. We log a warning but surface the error so the caller knows the
	// row-state and the grant-state diverged.
	if err := s.ResolveAuthorityRequest(ctx, requestID,
		AuthorityRequestStatusApproved, approverIdentity,
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

	// Emit the approval audit event with the freshly minted grant id.
	s.audit.LogAuthorityRequestEvent(ctx, resolved, OpAuthorityRequestApproved, approverIdentity,
		map[string]interface{}{"granted_grant_id": grant.GrantID})

	return resolved, nil
}

// DenyAuthorityRequest flips PENDING → DENIED and emits an audit event. No
// grant is minted. ResolutionReason is propagated to the row and audit
// metadata for downstream visibility.
func (s *Service) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*AuthorityRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("%w: request_id is required", ErrAuthorityRequestInvalid)
	}
	if approverIdentity.PrincipalRef().IsZero() {
		return nil, fmt.Errorf("%w: approver identity is required", ErrAuthorityRequestInvalid)
	}

	if err := s.ResolveAuthorityRequest(ctx, requestID,
		AuthorityRequestStatusDenied, approverIdentity,
		reason, "", time.Now().UTC(),
	); err != nil {
		return nil, err
	}

	resolved, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	s.audit.LogAuthorityRequestEvent(ctx, resolved, OpAuthorityRequestDenied, approverIdentity, nil)
	return resolved, nil
}

// CancelOpenAuthorityRequest is the lifecycle wrapper around the storage-
// layer CancelAuthorityRequest. The storage method (defined on *Service in
// authority_requests.go) already flips PENDING → CANCELLED; this wrapper
// adds the audit-event emit so callers see the lifecycle row in
// comprehensive_audit_log.
//
// Naming: we keep the storage method named CancelAuthorityRequest to honor
// the Stage A interface contract (the storage tests + acl.Store interface
// reference it by that name). The public lifecycle entry point therefore
// takes a distinct name (CancelOpenAuthorityRequest) so callers and gateway
// handlers can pick the right one without overload ambiguity.
func (s *Service) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*AuthorityRequest, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("%w: request_id is required", ErrAuthorityRequestInvalid)
	}
	if err := s.CancelAuthorityRequest(ctx, requestID, reason); err != nil {
		return nil, err
	}
	resolved, err := s.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	// Cancellation is requester-initiated; the audit row records the
	// requesting_actor as the actor since the storage layer's
	// CancelAuthorityRequest does not capture a resolved_by identity.
	s.audit.LogAuthorityRequestEvent(ctx, resolved, OpAuthorityRequestCancelled, resolved.RequestingActor, nil)
	return resolved, nil
}

// SweepExpiredAuthorityRequests is the background-task entry point for the
// waker / scheduler (Stage C wires it). It invokes ExpireAuthorityRequests
// to atomically flip all PENDING rows whose expires_at <= now, then emits
// an audit event per swept row. The list of swept rows is returned so the
// caller (e.g. task_waker) can fail any tasks still waiting on them.
//
// `limit` bounds the scan; pass 0 for unlimited (small deployments) or a
// reasonable value (e.g. 256) under load.
func (s *Service) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*AuthorityRequest, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expired, err := s.ExpireAuthorityRequests(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	// Audit emit is best-effort per row; we do NOT short-circuit the loop on
	// emit failure (LogAuthorityRequestEvent is non-blocking and drops
	// silently on full queues anyway).
	for _, r := range expired {
		s.audit.LogAuthorityRequestEvent(ctx, r, OpAuthorityRequestExpired, models.Identity{}, nil)
	}
	return expired, nil
}

// =============================================================================
// Scope intersection helpers
// =============================================================================

// intersectStringSlices returns the intersection of `requested` and `granted`.
// When `granted` is empty the helper returns `requested` (inherit-on-empty).
// Out-of-scope additions in `granted` are silently dropped — approvers cannot
// broaden a request.
//
// Result preserves the order of `requested` and de-duplicates within a single
// call.
func intersectStringSlices(requested, granted []string) []string {
	if len(granted) == 0 {
		if requested == nil {
			return []string{}
		}
		return requested
	}
	allow := make(map[string]struct{}, len(granted))
	for _, v := range granted {
		allow[v] = struct{}{}
	}
	out := make([]string, 0, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for _, v := range requested {
		if _, ok := allow[v]; !ok {
			continue
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// intersectResourceScope returns the intersection of two map[string][]string
// scopes. Keys present in both maps are kept; per-key the inner slices are
// intersected. Keys missing from `granted` (when `granted` is non-empty) are
// dropped — the approver did not authorize that resource type.
//
// When `granted` is empty the helper returns `requested` (inherit-on-empty).
// A nil requested returns an empty (non-nil) map so downstream serialization
// emits `{}` rather than SQL NULL.
func intersectResourceScope(requested, granted map[string][]string) map[string][]string {
	if len(granted) == 0 {
		if requested == nil {
			return map[string][]string{}
		}
		return requested
	}
	out := make(map[string][]string, len(requested))
	for key, reqValues := range requested {
		gValues, ok := granted[key]
		if !ok {
			continue
		}
		out[key] = intersectStringSlices(reqValues, gValues)
	}
	return out
}

// newAuthorityRequestID centralizes request-id minting so both the lifecycle
// (which generates the id before persisting to ensure the audit event and
// the storage row agree) and the storage layer (which currently defaults to
// uuid.New().String() inside CreateAuthorityRequest) can share one source.
func newAuthorityRequestID() string {
	return uuid.New().String()
}
