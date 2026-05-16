package acl

// This file re-exports the shared ACL types and constants from the legacy
// internal/acl package under the new internal/storage/acl interface
// namespace. The legacy package remains the source of truth during Stage 1
// of the storage-interfaces refactor; Stage 2 will introduce a native
// sqlite sibling and may eventually let us collapse the legacy package
// into this one. For now, downstream callers can
//
//	import "github.com/scitrera/aether/internal/storage/acl"
//
// and find every type, constant, and helper they need to construct ACL
// rules, evaluate decisions, mint authority grants, and query the audit log
// — no double-import of the legacy package required.

import (
	legacy "github.com/scitrera/aether/internal/acl"
)

// Core types — aliased so a single import gets callers everything they need.
type (
	// Rule is an ACL grant row. See legacy.ACLRule for field docs.
	Rule = legacy.ACLRule
	// Decision is the result of an access-check evaluation.
	Decision = legacy.ACLDecision
	// AuthorityGrant is the persisted delegated-authorization capability
	// used by the on-behalf-of model.
	AuthorityGrant = legacy.AuthorityGrant
	// ResolvedAuthority is the validated authority envelope for a single
	// request (returned by ResolveAuthority).
	ResolvedAuthority = legacy.ResolvedAuthority
	// RuleFilter is the query-side filter for ListRules.
	RuleFilter = legacy.RuleFilter
	// AuthorityGrantFilter is the query-side filter for
	// ListAuthorityGrants.
	AuthorityGrantFilter = legacy.AuthorityGrantFilter
	// VisibleGrantsFilter scopes a runtime grant-list query to the
	// calling actor (as delegate, as subject, or both).
	VisibleGrantsFilter = legacy.VisibleGrantsFilter
	// CreateAuthorityGrantRequest is the input shape for
	// CreateAuthorityGrant.
	CreateAuthorityGrantRequest = legacy.CreateAuthorityGrantRequest
	// RenewAuthorityGrantOpts groups the inputs to
	// RenewAuthorityGrantOpts (absolute target vs relative extension).
	RenewAuthorityGrantOpts = legacy.RenewAuthorityGrantOpts
	// RevokedAuthorityGrant is the lightweight projection returned by
	// RevokeAuthorityGrantCascade so callers can address affected
	// delegates without a follow-up GetAuthorityGrant per row.
	RevokedAuthorityGrant = legacy.RevokedAuthorityGrant
	// FallbackPolicy is a single row of acl_fallback_policies.
	FallbackPolicy = legacy.FallbackPolicy
	// AuditLogFilter is the query-side filter for QueryAuditLog.
	AuditLogFilter = legacy.AuditLogFilter
	// AuditLogEntry is a single row of the acl_audit_log view (a
	// projection over comprehensive_audit_log).
	AuditLogEntry = legacy.AuditLogEntry
	// RequestAuthorityContext is the request-time on-behalf-of context
	// supplied by the caller after transport authentication.
	RequestAuthorityContext = legacy.RequestAuthorityContext
	// GrantAudienceContext carries live request/session data used to
	// validate that a persisted authority grant is being used by the
	// bound audience (session, task, agent, or service).
	GrantAudienceContext = legacy.GrantAudienceContext

	// AuthorityRequest is a Phase 2 "sudo" request row: a running task
	// asks for elevated authority and parks until an approver resolves
	// it. Approval mints a standard AuthorityGrant via the existing
	// CreateAuthorityGrant path; no parallel grant type exists.
	AuthorityRequest = legacy.AuthorityRequest
	// AuthorityRequestStatus is the lifecycle state of an AuthorityRequest.
	AuthorityRequestStatus = legacy.AuthorityRequestStatus
	// AuthorityRequestRoutingTarget addresses approvers for a request.
	// Exactly one of (Principal, Capability) is populated.
	AuthorityRequestRoutingTarget = legacy.AuthorityRequestRoutingTarget
	// AuthorityRequestFilter narrows ListAuthorityRequests results.
	AuthorityRequestFilter = legacy.AuthorityRequestFilter
	// ApproveDecision carries the approver's refinements when accepting an
	// authority request. Empty / zero fields mean "inherit from the
	// request" (no narrowing).
	ApproveDecision = legacy.ApproveDecision

	// PrefixLookup is the resource_type → owning_agent resolver consumed by
	// acl.Store.SetPrefixIndex (Phase 5 Stage B). Aliased to the legacy ACL
	// package's interface so callers building registries can pass a
	// concrete impl that satisfies both surfaces.
	PrefixLookup = legacy.PrefixLookup
)

// AuthorityRequest lifecycle status constants.
const (
	AuthorityRequestStatusPending   = legacy.AuthorityRequestStatusPending
	AuthorityRequestStatusApproved  = legacy.AuthorityRequestStatusApproved
	AuthorityRequestStatusDenied    = legacy.AuthorityRequestStatusDenied
	AuthorityRequestStatusExpired   = legacy.AuthorityRequestStatusExpired
	AuthorityRequestStatusCancelled = legacy.AuthorityRequestStatusCancelled
)

// Access levels — hierarchical permission scale used by acl_rules.access_level
// and acl_fallback_policies.fallback_access_level.
const (
	AccessNone       = legacy.AccessNone
	AccessRead       = legacy.AccessRead
	AccessReadWrite  = legacy.AccessReadWrite
	AccessManage     = legacy.AccessManage
	AccessAdmin      = legacy.AccessAdmin
	AccessSuperAdmin = legacy.AccessSuperAdmin
)

// Resource types — acl_rules.resource_type values. Aliased from
// pkg/models.ResourceType* via the legacy package.
const (
	ResourceTypeWorkspace   = legacy.ResourceTypeWorkspace
	ResourceTypeAgent       = legacy.ResourceTypeAgent
	ResourceTypePermission  = legacy.ResourceTypePermission // deprecated: prefer Admin/Capability
	ResourceTypeAdmin       = legacy.ResourceTypeAdmin
	ResourceTypeCapability  = legacy.ResourceTypeCapability
	ResourceTypeTask        = legacy.ResourceTypeTask
	ResourceTypeKVScope     = legacy.ResourceTypeKVScope
	ResourceTypeKVKey       = legacy.ResourceTypeKVKey
	ResourceTypeServiceImpl = legacy.ResourceTypeServiceImpl
)

// Principal types — acl_rules.principal_type values (canonical lowercase).
const (
	PrincipalTypeUser           = legacy.PrincipalTypeUser
	PrincipalTypeAgent          = legacy.PrincipalTypeAgent
	PrincipalTypeTask           = legacy.PrincipalTypeTask
	PrincipalTypeWorkflowEngine = legacy.PrincipalTypeWorkflowEngine
	PrincipalTypeMetricsBridge  = legacy.PrincipalTypeMetricsBridge
	PrincipalTypeOrchestrator   = legacy.PrincipalTypeOrchestrator
	PrincipalTypeBridge         = legacy.PrincipalTypeBridge
	PrincipalTypeService        = legacy.PrincipalTypeService
	PrincipalTypeWildcard       = legacy.PrincipalTypeWildcard
)

// Reserved identifiers — wildcard subjects, system principal, global
// workspace, and the documented "match anything" resource ID.
const (
	WildcardAnyAuthenticatedUser = legacy.WildcardAnyAuthenticatedUser
	WildcardAnyAgent             = legacy.WildcardAnyAgent
	WildcardAnyTask              = legacy.WildcardAnyTask
	WildcardAnyService           = legacy.WildcardAnyService
	SystemPrincipal              = legacy.SystemPrincipal
	GlobalWorkspace              = legacy.GlobalWorkspace
	WildcardAnyResource          = legacy.WildcardAnyResource
)

// Capability and admin resource IDs — paired with ResourceTypeAdmin or
// ResourceTypeCapability. The Go constant NAMES are stable across the
// legacy "_perm:*" → typed "admin/<category>" / "capability/<name>"
// migration; only the underlying string values changed.
const (
	PermissionCreateWorkspace         = legacy.PermissionCreateWorkspace
	PermissionAdminOperations         = legacy.PermissionAdminOperations
	PermissionAdminACL                = legacy.PermissionAdminACL
	PermissionAdminTokens             = legacy.PermissionAdminTokens
	PermissionAdminWorkspaces         = legacy.PermissionAdminWorkspaces
	PermissionAdminAgents             = legacy.PermissionAdminAgents
	PermissionExchangeAuthorityGrants = legacy.PermissionExchangeAuthorityGrants
	PermissionAuthorityIntermediary   = legacy.PermissionAuthorityIntermediary
	PermissionMetricCredit            = legacy.PermissionMetricCredit
	PermissionEventBroadcast          = legacy.PermissionEventBroadcast
	PermissionMetricBroadcast         = legacy.PermissionMetricBroadcast
	PermissionAuditSubmit             = legacy.PermissionAuditSubmit
	PermissionResolveAuthority        = legacy.PermissionResolveAuthority
	PermissionQueryConnections        = legacy.PermissionQueryConnections
)

// WorkspaceScopeSubjectInherited is the magic value for
// AuthorityGrant.WorkspaceScope that documents intent: "delegate the
// workspace decision to the subject's own ACL". See
// internal/acl/types.go for the full rationale.
const WorkspaceScopeSubjectInherited = legacy.WorkspaceScopeSubjectInherited

// Decision constants — values stored in
// comprehensive_audit_log.decision (via the acl_audit_log view).
const (
	DecisionAllow = legacy.DecisionAllow
	DecisionDeny  = legacy.DecisionDeny
)

// Authority audience types — acl_authority_grants.audience_type values.
const (
	AuthorityAudienceSession = legacy.AuthorityAudienceSession
	AuthorityAudienceTask    = legacy.AuthorityAudienceTask
	AuthorityAudienceAgent   = legacy.AuthorityAudienceAgent
	AuthorityAudienceService = legacy.AuthorityAudienceService
)

// Sentinel errors surfaced by the Store contract.
var (
	ErrInvalidAccessLevel             = legacy.ErrInvalidAccessLevel
	ErrRuleNotFound                   = legacy.ErrRuleNotFound
	ErrFallbackPolicyNotFound         = legacy.ErrFallbackPolicyNotFound
	ErrAuthorityGrantNotFound         = legacy.ErrAuthorityGrantNotFound
	ErrAuthorityGrantExpired          = legacy.ErrAuthorityGrantExpired
	ErrAuthorityGrantRevoked          = legacy.ErrAuthorityGrantRevoked
	ErrAuthorityGrantNotActive        = legacy.ErrAuthorityGrantNotActive
	ErrAuthorityGrantRenewal          = legacy.ErrAuthorityGrantRenewal
	ErrAuthorityGrantDelegationDenied = legacy.ErrAuthorityGrantDelegationDenied
	ErrAuthorityGrantScopeEscalation  = legacy.ErrAuthorityGrantScopeEscalation
	ErrInvalidAuthorityContext        = legacy.ErrInvalidAuthorityContext
	ErrAuthorityGrantDelegateMismatch = legacy.ErrAuthorityGrantDelegateMismatch
	ErrAuthorityGrantSubjectMismatch  = legacy.ErrAuthorityGrantSubjectMismatch
	ErrAuthorityGrantAudienceMismatch = legacy.ErrAuthorityGrantAudienceMismatch
	ErrAuthorityGrantOperationDenied  = legacy.ErrAuthorityGrantOperationDenied
	ErrAuthorityGrantWorkspaceDenied  = legacy.ErrAuthorityGrantWorkspaceDenied
	ErrAuthorityGrantResourceDenied   = legacy.ErrAuthorityGrantResourceDenied
	// Phase 2 authority-request lifecycle sentinels.
	ErrAuthorityRequestNotFound        = legacy.ErrAuthorityRequestNotFound
	ErrAuthorityRequestAlreadyResolved = legacy.ErrAuthorityRequestAlreadyResolved
	ErrAuthorityRequestInvalid         = legacy.ErrAuthorityRequestInvalid
)

// Helpers — re-exported so callers can build canonical ACL inputs without
// reaching into the legacy package.
var (
	// AccessLevelName returns the human-readable name for an access
	// level (used by audit dashboards and admin tooling).
	AccessLevelName = legacy.AccessLevelName
	// IsWildcard returns true if the principal ID is one of the
	// well-known wildcard constants (any_authenticated_user, any_agent,
	// any_task, any_service).
	IsWildcard = legacy.IsWildcard
	// RuleCategory returns the fallback-policy category key for a
	// principal/resource combination ("<principal_type>_<resource_type>").
	RuleCategory = legacy.RuleCategory
	// ValidateAccessLevel returns ErrInvalidAccessLevel if the level is
	// outside the canonical 0/10/20/30/40/50 scale.
	ValidateAccessLevel = legacy.ValidateAccessLevel
	// PrincipalTypeForModel converts a models.PrincipalType to the
	// lowercase string used in ACL database operations.
	PrincipalTypeForModel = legacy.PrincipalTypeForModel
)
