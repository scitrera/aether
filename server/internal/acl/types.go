package acl

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

// Access levels - hierarchical permission levels
const (
	AccessNone       = 0  // No access
	AccessRead       = 10 // Read/view access (can see, can invoke agents)
	AccessReadWrite  = 20 // Read + write/execute access (can send messages, create tasks)
	AccessManage     = 30 // Manage access (can modify resources, grant READ/READWRITE to others)
	AccessAdmin      = 40 // Admin access (full control over resource, can grant all levels)
	AccessSuperAdmin = 50 // Super admin access (system-wide admin, can do anything)
)

// Resource types for ACL rules - defined in pkg/models and aliased here for package-local use.
const (
	ResourceTypeWorkspace = models.ResourceTypeWorkspace
	ResourceTypeAgent     = models.ResourceTypeAgent
	// ResourceTypePermission is the legacy "permission" resource family used
	// by the original `_perm:*` capability gates. It remains exposed only so
	// the rewriteLegacyPermission alias layer can translate stored rules and
	// caller-supplied checks to the typed ResourceTypeAdmin / ResourceTypeCapability
	// families. Deprecated: prefer ResourceTypeAdmin or ResourceTypeCapability.
	ResourceTypePermission = models.ResourceTypePermission
	// ResourceTypeAdmin gates administrative operations. Resource IDs use the
	// "admin/<category>" form (e.g. "admin/acl", "admin/*").
	ResourceTypeAdmin = models.ResourceTypeAdmin
	// ResourceTypeCapability gates runtime capabilities (e.g.
	// "capability/metric_credit", "capability/resolve_authority").
	ResourceTypeCapability  = models.ResourceTypeCapability
	ResourceTypeTask        = models.ResourceTypeTask
	ResourceTypeKVScope     = models.ResourceTypeKVScope
	ResourceTypeKVKey       = models.ResourceTypeKVKey
	ResourceTypeServiceImpl = models.ResourceTypeServiceImpl
)

// Principal type strings for ACL database operations. These are the lowercase
// canonical forms used in acl_rules and acl_fallback_policies tables.
// They correspond to models.PrincipalType constants but lowercased for DB convention.
const (
	PrincipalTypeUser           = "user"
	PrincipalTypeAgent          = "agent"
	PrincipalTypeTask           = "task"
	PrincipalTypeWorkflowEngine = "workflow_engine"
	PrincipalTypeMetricsBridge  = "metrics_bridge"
	PrincipalTypeOrchestrator   = "orchestrator"
	PrincipalTypeBridge         = "bridge"
	PrincipalTypeService        = "service"
	PrincipalTypeWildcard       = "wildcard"
)

// Reserved identifiers for ACL system
const (
	WildcardAnyAuthenticatedUser = "_any_authenticated_user" // Matches any authenticated user
	WildcardAnyAgent             = "_any_agent"              // Matches any agent
	WildcardAnyTask              = "_any_task"               // Matches any task
	WildcardAnyService           = "_any_service"            // Matches any Service principal
	SystemPrincipal              = "_system"                 // System principal for internal operations
	GlobalWorkspace              = "_global"                 // Global workspace (accessible to all)
	WildcardAnyResource          = "*"                       // Matches any resource ID for a given resource type
	// Capability and admin resource IDs. The Go constant NAMES are stable
	// (call sites continue to reference acl.PermissionAdminACL etc.); the
	// VALUES were migrated from the legacy "_perm:*" form to the typed
	// "admin/<category>" and "capability/<name>" forms in
	// migration 020_permission_namespace_refactor.sql. The
	// rewriteLegacyPermission alias layer handles backward compatibility for
	// stored rules and callers passing the legacy strings explicitly.
	PermissionCreateWorkspace         = "capability/create_workspace"          // capability gate — create new workspaces
	PermissionAdminOperations         = "admin/*"                              // umbrella admin gate — covers all admin/* categories
	PermissionAdminACL                = "admin/acl"                            // admin gate — ACL management
	PermissionAdminTokens             = "admin/tokens"                         // admin gate — API token management
	PermissionAdminWorkspaces         = "admin/workspaces"                     // admin gate — workspace management
	PermissionAdminAgents             = "admin/agents"                         // admin gate — agent management
	PermissionExchangeAuthorityGrants = "capability/exchange_authority_grants" // capability gate — trusted service/user-session grant exchange
	PermissionAuthorityIntermediary   = "capability/authority_intermediary"    // capability gate — trusted intermediaries minting re-rooted task grants on hop exhaustion
	PermissionMetricCredit            = "capability/metric_credit"             // capability gate — publish negative metric deltas (corrections / credits)
	PermissionEventBroadcast          = "capability/event_broadcast"           // capability gate — publish event::{ws} for a workspace other than the sender's home workspace
	PermissionMetricBroadcast         = "capability/metric_broadcast"          // capability gate — publish metric::{ws} for a workspace other than the sender's home workspace
	PermissionAuditSubmit             = "capability/audit_submit"              // capability gate — submit an audit event into a workspace other than the principal's home workspace via SubmitAuditEvent
	PermissionResolveAuthority        = "capability/resolve_authority"         // capability gate — resolve authority grants for principals other than self (terminator hot-path)
	// WorkspaceScopeSubjectInherited is a magic value for AuthorityGrant.WorkspaceScope
	// that documents intent: "delegate the workspace decision to the subject's own
	// ACL". Behaviorally it matches any workspace, like "*" — the difference is
	// intent-signaling: callers and audit dashboards can distinguish "intentionally
	// inherits from subject" from "accidentally over-broad wildcard". SDK helpers
	// should prefer this constant over a bare "*" entry. The trailing per-workspace
	// access check still applies (CheckAccessWithAuthority calls evaluateAccessNoAudit
	// against the subject), so the subject's own ACL remains the security ceiling.
	WorkspaceScopeSubjectInherited = "_subject_workspaces"
	PermissionQueryConnections     = "capability/query_connections" // capability gate — query the live-connection status of principals other than self
)

// Decision constants
const (
	DecisionAllow = "ALLOW"
	DecisionDeny  = "DENY"
)

// ACLRule represents an access control rule
// Each rule grants a specific access level to a principal for a resource
type ACLRule struct {
	RuleID        string
	PrincipalType string     // Type of principal (agent, task, user, wildcard)
	PrincipalID   string     // Identity of principal or wildcard pattern
	ResourceType  string     // Type of resource (workspace, agent, permission)
	ResourceID    string     // ID of resource
	AccessLevel   int        // Permission level granted
	GrantedBy     string     // Who created this rule
	GrantedAt     time.Time  // When rule was created
	ExpiresAt     *time.Time // Optional expiration time
	Reason        string     // Human-readable reason for this grant
}

// ACLDecision represents the result of an ACL evaluation
type ACLDecision struct {
	Allowed              bool            // Whether access is granted
	EffectiveAccessLevel int             // Actual access level granted
	Decision             string          // "ALLOW" or "DENY"
	RuleApplied          *ACLRule        // Which rule granted access (if any)
	AuthorityGrant       *AuthorityGrant // Authority grant if access was on behalf of a subject
	AuthorityMode        string          // direct or on_behalf_of
	FallbackApplied      bool            // Whether fallback policy was used
	Reason               string          // Human-readable reason for decision
}

// Denied returns true if access was denied
func (d *ACLDecision) Denied() bool {
	return !d.Allowed
}

// HasLevel returns true if the granted access level meets or exceeds the required level
func (d *ACLDecision) HasLevel(requiredLevel int) bool {
	return d.Allowed && d.EffectiveAccessLevel >= requiredLevel
}

// AuditLogEntry represents an ACL decision audit log entry
type AuditLogEntry struct {
	AuditID          int64
	Timestamp        time.Time
	Decision         string // "ALLOW" or "DENY"
	AccessLevel      int    // Effective access level
	PrincipalType    string
	PrincipalID      string
	SubjectType      string
	SubjectID        string
	RootSubjectType  string
	RootSubjectID    string
	RootGrantID      *string
	ResourceType     string
	ResourceID       string
	Operation        string // What operation was attempted
	Workspace        string
	RuleID           *string
	AuthorityMode    string
	AuthorityGrantID *string
	ParentGrantID    *string
	FallbackApplied  bool
	GatewayID        string
	SessionID        uuid.UUID
	Metadata         map[string]interface{}

	// Phase 5 Stage B: owning-agent attribution. When the audited
	// ResourceType falls under a resource_type_prefix declared by a
	// registered agent, OwningAgentImpl is the implementation name of that
	// agent and OwningAgentPrefix is the matched prefix string. The fields
	// are NOT persisted as dedicated columns — they flow into the entry's
	// Metadata bag (keys "owning_agent" + "owning_agent_prefix") via
	// buildACLMetadata so the existing comprehensive_audit_log schema can
	// absorb them without migration churn.
	OwningAgentImpl   string
	OwningAgentPrefix string
}

// FallbackPolicy represents a configurable fallback policy
// When no explicit ACL rule matches, the fallback policy determines the default behavior
type FallbackPolicy struct {
	PolicyID            string
	RuleCategory        string // e.g., "user_workspace", "agent_workspace"
	FallbackAccessLevel int    // Default access level
	UpdatedBy           string
	UpdatedAt           time.Time
}

// AccessLevelName returns the human-readable name for an access level
func AccessLevelName(level int) string {
	switch level {
	case AccessNone:
		return "NONE"
	case AccessRead:
		return "READ"
	case AccessReadWrite:
		return "READWRITE"
	case AccessManage:
		return "MANAGE"
	case AccessAdmin:
		return "ADMIN"
	case AccessSuperAdmin:
		return "SUPERADMIN"
	default:
		return "UNKNOWN"
	}
}

// IsWildcard returns true if the principal ID is a wildcard pattern
func IsWildcard(principalID string) bool {
	return principalID == WildcardAnyAuthenticatedUser ||
		principalID == WildcardAnyAgent ||
		principalID == WildcardAnyTask ||
		principalID == WildcardAnyService
}

// RuleCategory returns the fallback policy category for a principal/resource combination
func RuleCategory(principalType, resourceType string) string {
	return principalType + "_" + resourceType
}

// ValidateAccessLevel returns an error if the access level is invalid
func ValidateAccessLevel(level int) error {
	if level < AccessNone || level > AccessSuperAdmin || level%10 != 0 {
		return ErrInvalidAccessLevel
	}
	return nil
}

// minGlobSegments is the minimum number of dot-separated segments required
// before a wildcard (* or ?) in an ACL principal_id or resource_id.
// For agent identities like "ag::_system::platform-server::*", the prefix
// "ag::_system::platform-server" has 3 segments — enough to scope the grant
// to a specific implementation. "ag::*" (1 segment) would be rejected.
const minGlobSegments = 3

// identityPrefixes enumerates the canonical ID prefixes that produce
// identity-style strings (`<prefix>::...`). These are the only forms whose
// glob patterns get segment-count validation; non-identity resource_ids
// (typed admin/capability families, workspace names, service_impl names,
// kv keys, etc.) carry their own structural rules and pass through.
var identityPrefixes = [...]string{
	"ag" + models.IdentitySep,     // ag::ws::impl::spec
	"tu" + models.IdentitySep,     // tu::ws::impl::spec
	"ta" + models.IdentitySep,     // ta::ws::impl::id
	"sv" + models.IdentitySep,     // sv::impl::spec
	"br" + models.IdentitySep,     // br::impl::spec
	"us" + models.IdentitySep,     // us::user_id::window_id
	"orc" + models.IdentitySep,    // orc::impl[::spec]
	"wfe" + models.IdentitySep,    // wfe::impl
	"metric" + models.IdentitySep, // metric::impl
	"event" + models.IdentitySep,  // event::workspace
}

// isIdentityStyleID reports whether id begins with one of the canonical
// identity prefixes. The segment-count guard only applies to these forms.
func isIdentityStyleID(id string) bool {
	for _, p := range identityPrefixes {
		if strings.HasPrefix(id, p) {
			return true
		}
	}
	return false
}

// validateGlobPattern checks that IDs containing glob wildcards (* or ?)
// have enough specificity to prevent overly broad grants.
// Non-wildcard IDs and well-known wildcard constants (e.g. _any_agent) are
// always allowed.
//
// The segment-count rule applies ONLY to identity-style IDs (those that
// start with one of `ag::`, `tu::`, `ta::`, `sv::`, `br::`, `us::`, `orc::`,
// `wfe::`, `metric::`, `event::`). For those, the rule scales with layout:
//   - 4-segment identities (ag/tu/ta::ws::impl::spec): 3 segments min, so
//     "ag::ws::impl::*" is allowed but "ag::ws::*" is not.
//   - 3-segment identities (sv/br::impl::spec, us::uid::window): 2 segments
//     min, so "sv::impl::*" is allowed but "sv::*" is not.
//
// All other resource_id shapes (admin/*, capability/<name>, workspace names
// like `prod-*`, service_impl names, kv keys, etc.) pass through — their
// hierarchy lives in `/` or `:` separators that this guard wasn't designed
// for, and operators take responsibility for the breadth of those patterns.
func validateGlobPattern(id, fieldName string) error {
	if !strings.ContainsAny(id, "*?") {
		return nil // No glob characters — no restriction
	}

	// Well-known wildcard constants used with principal_type="wildcard"
	// are not subject to segment validation (they're matched via the
	// wildcardSubjects() path, not globMatch).
	switch id {
	case WildcardAnyAgent, WildcardAnyTask, WildcardAnyAuthenticatedUser, WildcardAnyService, WildcardAnyResource:
		return nil
	}

	// Non-identity-style IDs (typed admin/capability resources, workspace
	// names, service_impl names, kv keys, etc.) skip the segment guard
	// entirely — that guard is built around the "::"-separated identity
	// hierarchy and would mis-fire on these shapes.
	if !isIdentityStyleID(id) {
		return nil
	}

	// Count segments before the first wildcard character. Identity strings
	// use "::" as the segment separator (see models.IdentitySep).
	firstWild := strings.IndexAny(id, "*?")
	prefix := id[:firstWild]
	segments := strings.Count(prefix, models.IdentitySep) + 1
	// Don't count a trailing separator as creating an extra segment
	if strings.HasSuffix(prefix, models.IdentitySep) {
		segments--
	}

	required := requiredGlobSegmentsForID(id)
	if segments < required {
		return fmt.Errorf(
			"%s glob pattern too broad: %q has %d segment(s) before wildcard, minimum %d required",
			fieldName, id, segments, required,
		)
	}

	return nil
}

// requiredGlobSegmentsForID returns the minimum number of concrete segments
// that must appear before a wildcard for a given identity-prefixed ID.
// Short-form identities (3 segments total) use a lower minimum so impl-locked
// glob patterns (e.g. "sv::platform-server::*") remain expressible.
func requiredGlobSegmentsForID(id string) int {
	const sep = "::"
	for _, p := range [...]string{
		"sv" + sep,     // sv::impl::spec
		"br" + sep,     // br::impl::spec
		"us" + sep,     // us::user_id::window_id
		"orc" + sep,    // orc::impl[::spec]
		"wfe" + sep,    // wfe::impl
		"metric" + sep, // metric::impl
		"event" + sep,  // event::workspace
	} {
		if strings.HasPrefix(id, p) {
			return 2
		}
	}
	return minGlobSegments
}

// PrincipalTypeForModel converts a models.PrincipalType to the lowercase
// string used in ACL database operations.
func PrincipalTypeForModel(pt models.PrincipalType) string {
	switch pt {
	case models.PrincipalUser:
		return PrincipalTypeUser
	case models.PrincipalAgent:
		return PrincipalTypeAgent
	case models.PrincipalTask:
		return PrincipalTypeTask
	case models.PrincipalWorkflowEngine:
		return PrincipalTypeWorkflowEngine
	case models.PrincipalMetricsBridge:
		return PrincipalTypeMetricsBridge
	case models.PrincipalOrchestrator:
		return PrincipalTypeOrchestrator
	case models.PrincipalBridge:
		return PrincipalTypeBridge
	case models.PrincipalService:
		return PrincipalTypeService
	default:
		return strings.ToLower(string(pt))
	}
}

// Common errors
var (
	ErrInvalidAccessLevel             = fmt.Errorf("invalid access level")
	ErrRuleNotFound                   = fmt.Errorf("ACL rule not found")
	ErrFallbackPolicyNotFound         = fmt.Errorf("fallback policy not found")
	ErrAuthorityGrantNotFound         = fmt.Errorf("authority grant not found")
	ErrAuthorityGrantExpired          = fmt.Errorf("authority grant expired")
	ErrAuthorityGrantRevoked          = fmt.Errorf("authority grant revoked")
	ErrAuthorityGrantNotActive        = fmt.Errorf("authority grant is not active")
	ErrAuthorityGrantRenewal          = fmt.Errorf("authority grant cannot be renewed")
	ErrAuthorityGrantDelegationDenied = fmt.Errorf("authority grant cannot delegate further")
	ErrAuthorityGrantScopeEscalation  = fmt.Errorf("authority grant child would broaden parent scope")
	ErrInvalidAuthorityContext        = fmt.Errorf("invalid authority context")
	ErrAuthorityGrantDelegateMismatch = fmt.Errorf("authority grant delegate does not match actor")
	ErrAuthorityGrantSubjectMismatch  = fmt.Errorf("authority grant subject does not match requested subject")
	ErrAuthorityGrantAudienceMismatch = fmt.Errorf("authority grant audience does not match request context")
	ErrAuthorityGrantOperationDenied  = fmt.Errorf("authority grant does not permit this operation")
	ErrAuthorityGrantWorkspaceDenied  = fmt.Errorf("authority grant does not permit this workspace")
	ErrAuthorityGrantResourceDenied   = fmt.Errorf("authority grant does not permit this resource")
)
