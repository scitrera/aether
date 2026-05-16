// Package acl: Phase 2 authority-request lifecycle types.
//
// AuthorityRequest is a typed "sudo" handshake on top of the existing
// AuthorityGrant machinery: a running task asks for elevated authority and
// parks until an approver resolves it. Approval mints a standard
// AuthorityGrant via CreateAuthorityGrant; denial / expiry / cancellation
// leave the request resolved with a reason and no grant.
//
// Stage A scope: lifecycle types only. The storage layer
// (internal/storage/acl.AuthorityRequestStore) and SQL migrations consume
// these types; the gateway handler, ACL service wiring, and task_waker
// integration are added in later stages (B/C).
//
// The fields are intentionally independent of the proto bindings so the
// storage interface does not pull `api/proto` into the dependency graph.
// The proto-side `AuthorityRequest` message (see api/proto/aether.proto)
// uses the same field set under proto-native types.
package acl

import (
	"time"

	"github.com/scitrera/aether/pkg/models"
)

// AuthorityRequestStatus is the lifecycle state of an AuthorityRequest row.
// The canonical string values are persisted to acl_authority_requests.status
// and surfaced over the wire via the proto enum AuthorityRequestStatus.
type AuthorityRequestStatus string

const (
	// AuthorityRequestStatusPending indicates a request awaiting an approver.
	AuthorityRequestStatusPending AuthorityRequestStatus = "pending"
	// AuthorityRequestStatusApproved indicates the request was approved and
	// a corresponding AuthorityGrant has been minted (GrantedGrantID).
	AuthorityRequestStatusApproved AuthorityRequestStatus = "approved"
	// AuthorityRequestStatusDenied indicates the approver declined the
	// request. ResolutionReason carries the human-readable explanation.
	AuthorityRequestStatusDenied AuthorityRequestStatus = "denied"
	// AuthorityRequestStatusExpired indicates the request was not resolved
	// before ExpiresAt elapsed.
	AuthorityRequestStatusExpired AuthorityRequestStatus = "expired"
	// AuthorityRequestStatusCancelled indicates the requester (or owning
	// task) withdrew the request before resolution.
	AuthorityRequestStatusCancelled AuthorityRequestStatus = "cancelled"
)

// IsTerminal reports whether the request has reached a final state and is
// no longer eligible for resolution / cancellation transitions.
func (s AuthorityRequestStatus) IsTerminal() bool {
	switch s {
	case AuthorityRequestStatusApproved,
		AuthorityRequestStatusDenied,
		AuthorityRequestStatusExpired,
		AuthorityRequestStatusCancelled:
		return true
	default:
		return false
	}
}

// AuthorityRequestRoutingTarget addresses approvers for a request. Exactly
// one of (Principal, Capability) is populated:
//
//   - Principal: a specific approver identity (user, role, group). Only the
//     bound principal may resolve.
//   - Capability: a capability-gate string (e.g. "capability/approve/<action>").
//     Any actor whose ACL CheckAccess passes for the gate may resolve.
type AuthorityRequestRoutingTarget struct {
	Principal  *models.Identity `json:"principal,omitempty"`
	Capability string           `json:"capability,omitempty"`
}

// IsEmpty reports whether the routing target has no addressable approver.
// Storage enforces non-empty routing on insert (a request that nobody can
// resolve is a bug, not a valid state).
func (t AuthorityRequestRoutingTarget) IsEmpty() bool {
	if t.Capability != "" {
		return false
	}
	if t.Principal == nil {
		return true
	}
	ref := t.Principal.PrincipalRef()
	return ref.IsZero()
}

// AuthorityRequest is a single lifecycle row of acl_authority_requests.
//
// Field shape:
//   - ResourceScope mirrors CreateAuthorityGrantRequest.ResourceScope so the
//     approval path can pass it through to CreateAuthorityGrant without
//     conversion. Keys are resource_type strings; values are glob patterns.
//   - RequestedAccess uses the same access-level int as AuthorityGrant
//     (AccessNone..AccessSuperAdmin, the legacy 0/10/20/30/40/50 scale).
//   - TargetSubject is zero-valued (IsZero) when the requester is asking for
//     elevation in their own name -- the common case. A non-zero value
//     signals an on-behalf-of escalation that the approver must explicitly
//     consent to.
//   - GrantedGrantID is only populated when Status == Approved; the empty
//     string otherwise. The storage layer enforces this invariant.
type AuthorityRequest struct {
	RequestID       string
	Status          AuthorityRequestStatus
	RequestingActor models.Identity
	TargetSubject   models.Identity

	WorkspaceScope  []string
	ResourceScope   map[string][]string
	OperationScope  []string
	RequestedAccess int
	DurationSeconds int64

	AudienceType  string
	AudienceID    string
	RoutingTarget AuthorityRequestRoutingTarget

	Reason   string
	TaskID   string
	Metadata map[string]interface{}

	CreatedAt  time.Time
	ExpiresAt  time.Time
	ResolvedAt *time.Time

	GrantedGrantID   string
	ResolvedBy       models.Identity
	ResolutionReason string
}

// AuthorityRequestFilter narrows ListAuthorityRequests results. The filter
// is exclusively additive -- empty fields impose no constraint.
//
// Resolver targeting: callers asking "what requests can I resolve?" populate
// ResolverPrincipal (their own identity) AND/OR ResolverCapabilities (the
// gate strings their ACL access grants). The storage layer ORs the two
// matching paths together: a request matches when its routing addresses the
// principal OR its capability gate appears in the supplied list.
type AuthorityRequestFilter struct {
	Status               AuthorityRequestStatus
	Workspace            string
	ResolverPrincipal    *models.Identity
	ResolverCapabilities []string
	Limit                int
	Offset               int
}

// ErrAuthorityRequestNotFound is returned by GetAuthorityRequest /
// ResolveAuthorityRequest / CancelAuthorityRequest when no row matches the
// supplied request_id.
var ErrAuthorityRequestNotFound = authorityRequestError("authority request not found")

// ErrAuthorityRequestAlreadyResolved is returned by ResolveAuthorityRequest
// / CancelAuthorityRequest when the target row is already in a terminal
// state. Resolution is idempotent: callers that observe this error should
// re-read the row to recover the existing resolution rather than retrying.
var ErrAuthorityRequestAlreadyResolved = authorityRequestError("authority request already resolved")

// ErrAuthorityRequestInvalid is returned by CreateAuthorityRequest when the
// input fails validation (e.g. empty routing target, non-positive duration).
var ErrAuthorityRequestInvalid = authorityRequestError("invalid authority request")

// authorityRequestError is the underlying type for the package's typed
// sentinel errors. Keeping it unexported lets callers do errors.Is checks
// against the package-level vars without leaking the type.
type authorityRequestError string

// Error satisfies the error interface.
func (e authorityRequestError) Error() string { return string(e) }
