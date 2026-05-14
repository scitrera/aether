// Package acl defines the storage interface for the access-control subsystem
// (acl_rules, acl_fallback_policies, acl_authority_grants, plus the audit-log
// read paths that surface ACL decisions from comprehensive_audit_log).
//
// Stage 1 consumers (callers that depend on this interface today):
//   - cmd/gateway/main.go        — constructs the postgres-backed impl
//   - cmd/aetherlite/main.go     — constructs the postgres-backed impl behind
//     the sqlite_compat translation layer (until
//     Stage 2 introduces a native sqlite sibling)
//   - internal/gateway/server.go — holds the acl.Store handle and threads it
//     through to admin handlers, the message
//     router (connect/send/manage checks), and
//     the workspace lifecycle plumbing
//
// The interface intentionally mirrors the legacy *internal/acl.Service method
// set one-for-one (~40 public methods). This is the mechanical-extraction
// phase of the storage refactor described in
// `.slop/20260513_native-storage-interfaces.md` §2/§4/§13 and the Stage 0
// decisions in `.slop/20260514_storage_interfaces_stage0.md`: the postgres
// impl is byte-for-byte the same logic, just re-homed behind an interface so
// a future sqlite-native sibling (Stage 2) can drop in.
//
// **Service vs Store naming.** The legacy concrete type is named *Service*
// because it owns more than persistence — it embeds a Casbin SyncedEnforcer
// for in-memory rule evaluation, an audit adapter for decision logging, and
// the fallback-policy LRU cache. The new interface in this package is named
// *Store* per the storage-refactor convention (§2 of the plan) — every
// domain's surface is exposed as a `Store` interface regardless of how rich
// the underlying impl is. The postgres-backed implementation is still the
// legacy Service type (re-exported via a type alias from
// internal/storage/acl/postgres) — no behavioral wrapping, no method
// forwarding, no drift risk. Stage 2's sqlite-native sibling will define its
// own Service-equivalent type under the same interface.
//
// **Casbin adapter surface.** Per §9 of the storage-interfaces plan we
// audited whether the Casbin enforcer's policy-load path needed to be
// surfaced on the interface. It does NOT: the Service's persist.Adapter
// (internal/acl/adapter.go) is constructed internally and the only
// reload-from-DB path that the Service exposes runs implicitly inside
// CleanupExpiredRules. There is no public LoadPolicies / ReloadPolicies
// method on *Service today, so the interface follows suit. If a future
// caller needs an explicit reload hook (e.g. for live policy hot-reload
// outside the cleanup loop), it gets added here at that time.
package acl

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

// Store is the ACL surface consumed by the gateway. It performs access
// decisions (CheckAccess + the convenience helpers), persists and queries
// ACL rules and fallback policies, manages the authority-grant lifecycle
// (the on-behalf-of delegation graph), and exposes the audit-log read /
// retention-cleanup hooks for ACL decisions.
//
// Nil-tolerance policy (§14.1 of the storage-interfaces plan): callers MUST
// pass a non-nil implementation. ACL is a load-bearing security surface —
// silent nil-deref hazards (the chat-message SIGSEGV pattern that inspired
// this refactor) and silent typed-nil-via-failed-assertion hazards (the
// cleanup-leader-election degradation pattern) are both unacceptable here.
// There is no defensible "opt-out" mode for ACL: a deployment that wants to
// disable enforcement does so by seeding broad fallback policies on a real
// impl. No NoOp impl is provided in this domain.
//
// Lifecycle: Close() flushes any owned audit writer (compat constructors
// only) and tears down adapter references. The shared-audit construction
// path (NewWithSharedAudit, used in lite + full production) leaves the
// caller's audit writer alone.
type Store interface {
	// =====================================================================
	// Access checks
	// =====================================================================

	// CheckAccess is the primary access-decision entry point. It evaluates
	// the principal's effective access level against the resource (with
	// glob/wildcard specificity priority), applies the fallback policy when
	// no rule matches, and emits an audit row recording the decision.
	//
	// Legacy ("permission", "_perm:*") inputs are transparently rewritten
	// to the typed admin/* and capability/* families inside the impl.
	CheckAccess(ctx context.Context, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*Decision, error)

	// CheckAccessWithAuthority evaluates access under a validated
	// on-behalf-of grant. The grant's constraints (max_access_level,
	// operation_scope, workspace_scope, resource_scope) intersect with the
	// subject's ACL. Pass a nil authority to fall back to direct
	// CheckAccess semantics.
	CheckAccessWithAuthority(ctx context.Context, actor models.Identity, authority *ResolvedAuthority, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*Decision, error)

	// CanConnect is the connect-time convenience: CheckAccess at AccessRead
	// against the target principal (agent or task).
	CanConnect(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*Decision, error)

	// CanSendMessage is the message-routing convenience: CheckAccess at
	// AccessReadWrite against the message target.
	CanSendMessage(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*Decision, error)

	// CanManageWorkspace gates workspace-administration operations
	// (CheckAccess at AccessManage against the workspace resource).
	CanManageWorkspace(ctx context.Context, principal models.Identity, workspace string, sessionID uuid.UUID) (*Decision, error)

	// CanCreateWorkspace gates workspace creation via the
	// capability/create_workspace typed capability resource.
	CanCreateWorkspace(ctx context.Context, principal models.Identity, sessionID uuid.UUID) (*Decision, error)

	// =====================================================================
	// Authority grants (on-behalf-of delegation)
	// =====================================================================

	// ResolveAuthority validates a request-time authority context against
	// the authenticated actor and live audience binding. Returns the
	// resolved envelope (actor + subject + grant) on success, or one of
	// the authority-* sentinel errors on mismatch / revocation / expiry.
	ResolveAuthority(ctx context.Context, actor models.Identity, req RequestAuthorityContext, audience GrantAudienceContext) (*ResolvedAuthority, error)

	// CreateAuthorityGrant persists a new authority grant. For derived
	// grants (req.ParentGrantID != nil) the impl validates that the child
	// scope is a subset of the parent's and that delegation is permitted.
	CreateAuthorityGrant(ctx context.Context, req CreateAuthorityGrantRequest) (*AuthorityGrant, error)

	// GetAuthorityGrant retrieves a single grant by ID. Returns
	// ErrAuthorityGrantNotFound when the grant doesn't exist.
	GetAuthorityGrant(ctx context.Context, grantID string) (*AuthorityGrant, error)

	// ListAuthorityGrants returns grants matching the filter, ordered by
	// created_at DESC. Supports pagination via Limit/Offset.
	ListAuthorityGrants(ctx context.Context, filter AuthorityGrantFilter) ([]*AuthorityGrant, error)

	// RenewAuthorityGrant extends an existing grant's expires_at to the
	// supplied absolute time, clamping against the grant's
	// renewable_until window (and, for derived grants, the parent's
	// expires_at). Convenience wrapper around RenewAuthorityGrantOpts.
	RenewAuthorityGrant(ctx context.Context, grantID string, expiresAt time.Time) (*AuthorityGrant, error)

	// RenewAuthorityGrantOpts is the structured renewal entrypoint.
	// Accepts either an absolute target (ExpiresAt) or a relative
	// extension (ExtendSeconds); ExtendSeconds wins when both are set.
	RenewAuthorityGrantOpts(ctx context.Context, grantID string, opts RenewAuthorityGrantOpts) (*AuthorityGrant, error)

	// RevokeAuthorityGrant revokes a single grant and all not-yet-revoked
	// descendants. Convenience wrapper around RevokeAuthorityGrantCascade
	// that discards the returned descendant list.
	RevokeAuthorityGrant(ctx context.Context, grantID string) error

	// RevokeAuthorityGrantCascade revokes the target grant and its full
	// descendant subtree in a single recursive UPDATE, returning the
	// affected rows so callers can react (e.g. notify connected delegates).
	// Returns ErrAuthorityGrantNotFound when the target doesn't exist or
	// all descendants are already revoked.
	RevokeAuthorityGrantCascade(ctx context.Context, grantID string) ([]RevokedAuthorityGrant, error)

	// ListVisibleGrants returns grants the actor can see in their own
	// role (as delegate, as subject, or both). At least one of
	// ByDelegate / BySubject must be true in the filter; otherwise the
	// impl returns an empty slice without hitting the DB.
	ListVisibleGrants(ctx context.Context, filter VisibleGrantsFilter) ([]*AuthorityGrant, error)

	// FindVisibleDerivedGrant returns an active grant that the caller
	// previously minted matching the (parent, target delegate, audience)
	// tuple, enabling idempotent DERIVE_FOR_TARGET. Returns nil with no
	// error when no matching grant exists.
	FindVisibleDerivedGrant(ctx context.Context, parentGrantID string, target models.Identity, audienceType, audienceID string) (*AuthorityGrant, error)

	// =====================================================================
	// Grants / Rules (direct ACL rule CRUD on acl_rules)
	// =====================================================================

	// GrantAccess upserts an ACL rule. The (principal_type, principal_id,
	// resource_type, resource_id) tuple is the unique key; existing rows
	// are updated in place. The in-memory Casbin model is refreshed
	// best-effort after the persistent write succeeds.
	//
	// Legacy ("permission", "_perm:*") grants are rewritten to typed
	// admin/* and capability/* shapes before persistence so newly issued
	// rules match the post-migration data shape.
	GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*Rule, error)

	// RevokeAccess removes a rule by (principal_type, principal_id,
	// resource_type, resource_id). Returns ErrRuleNotFound when no row
	// matches. Updates the in-memory Casbin model best-effort.
	RevokeAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error

	// GetRule fetches a single rule by its composite key. Returns
	// ErrRuleNotFound on miss.
	GetRule(ctx context.Context, principalType, principalID, resourceType, resourceID string) (*Rule, error)

	// ListRules returns rules matching the filter, ordered by granted_at
	// DESC.
	ListRules(ctx context.Context, filter RuleFilter) ([]*Rule, error)

	// =====================================================================
	// Fallback policy
	// =====================================================================

	// SetFallbackPolicy upserts a fallback policy row and invalidates the
	// in-memory fallback cache so the next CheckAccess sees the new value.
	SetFallbackPolicy(ctx context.Context, ruleCategory string, fallbackAccessLevel int, updatedBy string) error

	// GetFallbackPolicy retrieves a fallback policy by category. Returns
	// ErrFallbackPolicyNotFound on miss.
	GetFallbackPolicy(ctx context.Context, ruleCategory string) (*FallbackPolicy, error)

	// InvalidateFallbackCache clears the in-memory fallback-policy LRU.
	// Call after any out-of-band write to acl_fallback_policies that
	// bypasses SetFallbackPolicy.
	InvalidateFallbackCache()

	// =====================================================================
	// Cleanup
	// =====================================================================

	// CleanupExpiredRules deletes rules whose ExpiresAt has passed and
	// reloads the in-memory Casbin model. Returns the count of deleted
	// rows. Postgres-backed impls invoke the cleanup_expired_acl_rules()
	// stored function; sqlite-native impls (Stage 2) will issue an
	// equivalent parameterized DELETE.
	CleanupExpiredRules(ctx context.Context) (int64, error)

	// =====================================================================
	// Audit
	// =====================================================================

	// QueryAuditLog retrieves ACL-decision rows from the comprehensive
	// audit log (via the acl_audit_log view in postgres), ordered by
	// timestamp DESC. Supports pagination via Limit.
	QueryAuditLog(ctx context.Context, filter AuditLogFilter) ([]*AuditLogEntry, error)

	// CleanupOldAuditLogs deletes audit log rows older than retentionDays
	// and returns the number of rows removed. Postgres-backed impls
	// invoke the cleanup_old_audit_logs(retention_days) stored function;
	// sqlite-native impls (Stage 2) will issue an equivalent
	// parameterized DELETE.
	CleanupOldAuditLogs(ctx context.Context, retentionDays int) (int64, error)

	// =====================================================================
	// Lifecycle
	// =====================================================================

	// Close releases adapter references and, for the legacy compat
	// constructors (NewService / NewServiceWithAuditDB) that own their
	// audit writer goroutine, flushes and stops it. The shared-audit
	// construction path (NewWithSharedAudit) leaves the caller's audit
	// writer alone. Safe to call multiple times.
	Close() error
}
