package acl

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// fallbackCacheTTL is how long a fallback policy result is cached before re-querying the DB.
const fallbackCacheTTL = 30 * time.Second

// fallbackCacheEntry holds a single cached fallback policy value.
type fallbackCacheEntry struct {
	level    int
	found    bool // false means "no row" (deny)
	cachedAt time.Time
}

// PrefixLookup is the narrow lookup surface the ACL service consumes for
// Phase 5 Stage B audit attribution. The full implementation lives in
// internal/registry.PrefixIndex (which satisfies this interface). Defining the
// interface locally keeps the acl package free of a back-edge dependency on
// internal/registry — the gateway wires the concrete type in via SetPrefixIndex.
//
// Lookup returns (implementation, matchedPrefix, true) when resourceType falls
// under a declared prefix, or zero values + ok=false otherwise. The returned
// matchedPrefix is the literal string from the agent's declaration that the
// resource_type matched against, which audit consumers record so an operator
// can see WHICH prefix in the agent's schema caught the access.
type PrefixLookup interface {
	Lookup(resourceType string) (implementation string, matchedPrefix string, ok bool)
}

// Service provides the complete ACL API
type Service struct {
	db        *sql.DB
	enforcer  *CasbinEnforcer
	audit     *AuditLogger
	gatewayID string

	// ownsAudit is true when this Service constructed its own private
	// *audit.AuditLogger (legacy NewService/NewServiceWithAuditDB compat
	// path) and is therefore responsible for Closing it. When the audit
	// writer is shared (NewServiceWithSharedAudit), the caller owns its
	// lifecycle and Close() must not stop the goroutine.
	ownsAudit bool

	// fallbackCache caches per-category fallback policy query results to avoid
	// hitting the database on every unenforced access check.
	fallbackCache sync.Map // key: string (category), value: fallbackCacheEntry

	// prefixIndex resolves a resource_type to its owning agent implementation
	// for Phase 5 Stage B audit attribution. Set via SetPrefixIndex at gateway
	// boot (and on every Register / Delete) by the gateway-side wiring.
	// nil-tolerant: CheckAccess simply skips attribution when the field is nil
	// (no audit metadata noise, no nil-deref).
	prefixIndex PrefixLookup
}

// SetPrefixIndex injects (or replaces) the resource_type → owning_agent
// resolver used for Phase 5 audit attribution. Safe to call before any
// CheckAccess runs; the caller is responsible for invoking Set on the index
// after every Register/Delete. Passing nil disables attribution.
func (s *Service) SetPrefixIndex(p PrefixLookup) {
	if s == nil {
		return
	}
	s.prefixIndex = p
}

// NewService creates a new ACL service whose ACL rules and audit log both
// live in the same database (postgres path, or single-file aetherlite).
//
// COMPAT PATH: builds a private *audit.AuditLogger writer goroutine in
// addition to the gateway's own. Callers that want a single contention-free
// writer should use NewServiceWithSharedAudit instead and pass the same
// audit.AuditLogger that the gateway constructs at startup.
func NewService(db *sql.DB, gatewayID string) *Service {
	return NewServiceWithAuditDB(db, db, gatewayID)
}

// NewServiceWithAuditDB creates a new ACL service backed by `db` for ACL
// state (acl_rules, acl_fallback_policies, acl_authority_grants) and by
// `auditDB` for the comprehensive_audit_log writer.
//
// COMPAT PATH: this constructor builds its own *audit.AuditLogger writer
// goroutine. Two writers (gateway audit + ACL audit) targeting the same
// table re-introduce the WAL writer-vs-writer contention this refactor
// eliminates. Production gateway/aetherlite paths now call
// NewServiceWithSharedAudit so a single writer drains both producers;
// this wrapper exists only for utility tooling (init-secrets, authproxy)
// where the audit batcher is short-lived or contention is not a concern.
func NewServiceWithAuditDB(db, auditDB *sql.DB, gatewayID string) *Service {
	// Build a private audit writer with default config. Lifetime tied to
	// the Service via Close().
	privateAudit := audit.NewAuditLogger(auditDB, gatewayID, audit.DefaultConfig())
	svc := newServiceWithShared(db, privateAudit, auditDB, gatewayID)
	svc.ownsAudit = true
	return svc
}

// NewServiceWithSharedAudit creates an ACL service that funnels audit
// writes through `sharedAudit` (owned by the caller — typically the
// gateway constructed it at startup). `db` carries ACL rule state;
// `auditDB` is the read-side handle for ACL audit queries and must
// point at the same physical file as `sharedAudit` (audit.db in lite,
// aether.db in postgres). Pass the same *sql.DB for db/auditDB in
// single-file deployments.
//
// The sharedAudit parameter is typed as audit.EventSink (the narrow
// write-only interface). Both the legacy *audit.AuditLogger and the
// native-sqlite *auditsqlite.Store satisfy this interface, so Wave 3
// can cut over audit to the native impl without touching this
// constructor again.
//
// This is the contention-free path: only one batched writer goroutine
// (the shared one) ever touches comprehensive_audit_log.
func NewServiceWithSharedAudit(db *sql.DB, sharedAudit audit.EventSink, auditDB *sql.DB, gatewayID string) *Service {
	return newServiceWithShared(db, sharedAudit, auditDB, gatewayID)
}

// newServiceWithShared is the shared body used by both ACL service
// constructors. The enforcer is built from `db`; the audit adapter wraps
// `sharedAudit` (writes) and `auditDB` (reads).
func newServiceWithShared(db *sql.DB, sharedAudit audit.EventSink, auditDB *sql.DB, gatewayID string) *Service {
	enforcer, err := NewCasbinEnforcer(db)
	if err != nil {
		// If the enforcer fails to initialize (e.g., table doesn't exist
		// yet), create a service without it. CheckAccess will deny all
		// requests when the enforcer is nil to prevent unauthorized
		// access (fail-closed).
		return &Service{
			db:        db,
			audit:     NewAuditLogger(sharedAudit, auditDB, gatewayID),
			gatewayID: gatewayID,
		}
	}

	return &Service{
		db:        db,
		enforcer:  enforcer,
		audit:     NewAuditLogger(sharedAudit, auditDB, gatewayID),
		gatewayID: gatewayID,
	}
}

// NOTE: ACL migrations are now handled by migrations/runner.go using embedded SQL files.
// See migrations/003_acl_schema.sql for the schema definition.

// Close shuts down the ACL service and, when this Service owns its
// private audit writer (legacy NewService / NewServiceWithAuditDB
// constructors), flushes and stops the underlying audit goroutine.
// When the writer is shared (NewServiceWithSharedAudit) the caller owns
// it and Close() does not stop the goroutine.
func (s *Service) Close() error {
	if s.ownsAudit && s.audit != nil && s.audit.shared != nil {
		// The ownsAudit path only fires when this Service built a private
		// *audit.AuditLogger via the compat constructors (NewService /
		// NewServiceWithAuditDB). That concrete type implements io.Closer.
		// The shared field is typed as audit.EventSink (narrow interface)
		// so we type-assert to io.Closer for the shutdown path.
		if closer, ok := s.audit.shared.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				return err
			}
		}
	}
	if s.audit != nil {
		_ = s.audit.Close()
	}
	return nil
}

// CheckAccess is the main entry point for access control checks.
// It evaluates, audits, and returns the decision.
//
// Legacy ("permission", "_perm:*") inputs are transparently rewritten to the
// typed ResourceTypeAdmin / ResourceTypeCapability families via
// rewriteLegacyPermission so the Casbin lookup matches the migrated rule
// shape. Audit log entries record the post-rewrite resource so dashboards
// see the canonical typed form.
func (s *Service) CheckAccess(ctx context.Context, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*ACLDecision, error) {
	resourceType, resourceID, _ = rewriteLegacyPermission(resourceType, resourceID)

	decision, err := s.evaluateAccessNoAudit(ctx, principal, resourceType, resourceID, requiredLevel)
	if err != nil {
		return nil, err
	}

	// Phase 5 Stage B: resolve the owning agent for resourceType (if any)
	// and pass it into the audit emitter so the audit row records who
	// "owns" the resource family being accessed. Lookup is nil-tolerant.
	owningImpl, owningPrefix := "", ""
	if s.prefixIndex != nil {
		if impl, prefix, ok := s.prefixIndex.Lookup(resourceType); ok {
			owningImpl = impl
			owningPrefix = prefix
		}
	}

	// Audit the decision (async)
	s.audit.LogDecisionWithAttribution(ctx, decision, principal, resourceType, resourceID, operation, workspace, sessionID, owningImpl, owningPrefix)

	return decision, nil
}

func (s *Service) evaluateAccessNoAudit(ctx context.Context, principal models.Identity, resourceType, resourceID string, requiredLevel int) (*ACLDecision, error) {
	var decision *ACLDecision
	var err error

	if s.enforcer != nil {
		decision, err = s.enforcer.EvaluateAccess(ctx, principal, resourceType, resourceID, requiredLevel)
		if err != nil {
			return nil, err
		}
	} else {
		// Enforcer not initialized — deny all access (fail-closed)
		return &ACLDecision{
			Allowed:              false,
			EffectiveAccessLevel: AccessNone,
			Decision:             DecisionDeny,
			Reason:               "ACL enforcer not initialized — denying by default",
		}, nil
	}

	if decision == nil {
		decision, err = s.applyFallback(ctx, principal, resourceType, requiredLevel)
		if err != nil {
			return nil, err
		}
	}

	return decision, nil
}

// applyFallback applies the fallback policy when no explicit rule exists.
// Results are cached for fallbackCacheTTL to avoid per-call database queries.
func (s *Service) applyFallback(ctx context.Context, principal models.Identity, resourceType string, requiredLevel int) (*ACLDecision, error) {
	principalType := PrincipalTypeForModel(principal.Type)
	category := RuleCategory(principalType, resourceType)

	// Check cache first
	if raw, ok := s.fallbackCache.Load(category); ok {
		entry := raw.(fallbackCacheEntry)
		if time.Since(entry.cachedAt) < fallbackCacheTTL {
			return s.buildFallbackDecision(category, entry.level, entry.found, requiredLevel), nil
		}
	}

	// Cache miss or expired — query the database
	query := `
		SELECT fallback_access_level
		FROM acl_fallback_policies
		WHERE rule_category = $1
	`

	var fallbackLevel int
	err := s.db.QueryRowContext(ctx, query, category).Scan(&fallbackLevel)

	if err == sql.ErrNoRows {
		s.fallbackCache.Store(category, fallbackCacheEntry{level: 0, found: false, cachedAt: time.Now()})
		return s.buildFallbackDecision(category, 0, false, requiredLevel), nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query fallback policy: %w", err)
	}

	s.fallbackCache.Store(category, fallbackCacheEntry{level: fallbackLevel, found: true, cachedAt: time.Now()})
	return s.buildFallbackDecision(category, fallbackLevel, true, requiredLevel), nil
}

// buildFallbackDecision constructs an ACLDecision from a fallback policy value.
func (s *Service) buildFallbackDecision(category string, fallbackLevel int, found bool, requiredLevel int) *ACLDecision {
	if !found {
		return &ACLDecision{
			Allowed:              false,
			EffectiveAccessLevel: AccessNone,
			Decision:             DecisionDeny,
			FallbackApplied:      true,
			Reason:               fmt.Sprintf("No rule or fallback policy for %s", category),
		}
	}
	decision := &ACLDecision{
		Allowed:              fallbackLevel >= requiredLevel,
		EffectiveAccessLevel: fallbackLevel,
		Decision:             DecisionAllow,
		FallbackApplied:      true,
		Reason:               fmt.Sprintf("Fallback policy for %s: %s", category, AccessLevelName(fallbackLevel)),
	}
	if !decision.Allowed {
		decision.Decision = DecisionDeny
	}
	return decision
}

// InvalidateFallbackCache removes all cached fallback policy entries, forcing
// the next applyFallback call to re-query the database. Call this after any
// admin operation that modifies fallback policies.
func (s *Service) InvalidateFallbackCache() {
	s.fallbackCache.Range(func(key, _ interface{}) bool {
		s.fallbackCache.Delete(key)
		return true
	})
}

// GrantAccess creates a new ACL rule granting access.
//
// Legacy ("permission", "_perm:*") grants are transparently rewritten to the
// typed admin/* and capability/* families before persistence so newly-issued
// rules match the post-migration shape.
func (s *Service) GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*ACLRule, error) {
	resourceType, resourceID, _ = rewriteLegacyPermission(resourceType, resourceID)

	if err := ValidateAccessLevel(accessLevel); err != nil {
		return nil, err
	}

	// Validate glob patterns in principal and resource IDs.
	// Patterns with wildcards (* or ?) must have enough specificity to prevent
	// overly broad grants (e.g. "ag.*" matching all agents).
	if err := validateGlobPattern(principalID, "principal_id"); err != nil {
		return nil, err
	}
	if err := validateGlobPattern(resourceID, "resource_id"); err != nil {
		return nil, err
	}

	rule := &ACLRule{
		RuleID:        uuid.New().String(),
		PrincipalType: principalType,
		PrincipalID:   principalID,
		ResourceType:  resourceType,
		ResourceID:    resourceID,
		AccessLevel:   accessLevel,
		GrantedBy:     grantedBy,
		GrantedAt:     time.Now(),
		ExpiresAt:     expiresAt,
		Reason:        reason,
	}

	query := `
		INSERT INTO acl_rules (
			rule_id, principal_type, principal_id, resource_type, resource_id,
			access_level, granted_by, granted_at, expires_at, reason
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (principal_type, principal_id, resource_type, resource_id)
		DO UPDATE SET
			access_level = EXCLUDED.access_level,
			granted_by = EXCLUDED.granted_by,
			granted_at = EXCLUDED.granted_at,
			expires_at = EXCLUDED.expires_at,
			reason = EXCLUDED.reason
		RETURNING rule_id
	`

	err := s.db.QueryRowContext(ctx, query,
		rule.RuleID,
		rule.PrincipalType,
		rule.PrincipalID,
		rule.ResourceType,
		rule.ResourceID,
		rule.AccessLevel,
		rule.GrantedBy,
		rule.GrantedAt,
		rule.ExpiresAt,
		rule.Reason,
	).Scan(&rule.RuleID)

	if err != nil {
		return nil, fmt.Errorf("failed to create ACL rule: %w", err)
	}

	// Update in-memory model
	if s.enforcer != nil {
		sub := principalType + ":" + principalID
		obj := resourceType + ":" + resourceID
		act := strconv.Itoa(accessLevel)
		exp := ""
		if expiresAt != nil {
			exp = expiresAt.Format(time.RFC3339)
		}

		// Remove old policy for this sub+obj (handles upsert), then add new.
		// Both calls touch only the in-memory enforcer model. The persistent
		// row already landed in `acl_rules` above; any in-memory error is
		// logged best-effort rather than rolled back so the persisted state
		// remains authoritative on the next reload.
		if _, err := s.enforcer.RemovePolicy(sub, obj); err != nil {
			logging.Logger.Warn().Err(err).Str("sub", sub).Str("obj", obj).Msg("acl: in-memory RemovePolicy failed; persisted state unchanged")
		}
		if _, err := s.enforcer.AddPolicy(sub, obj, act, exp, rule.RuleID); err != nil {
			logging.Logger.Warn().Err(err).Str("sub", sub).Str("obj", obj).Msg("acl: in-memory AddPolicy failed; persisted state unchanged")
		}
	}

	return rule, nil
}

// RevokeAccess removes an ACL rule. Legacy ("permission", "_perm:*") inputs
// are rewritten to the typed admin/* and capability/* families so a revoke
// targeting either form locates the post-migration row.
func (s *Service) RevokeAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error {
	resourceType, resourceID, _ = rewriteLegacyPermission(resourceType, resourceID)

	query := `
		DELETE FROM acl_rules
		WHERE principal_type = $1 AND principal_id = $2
		  AND resource_type = $3 AND resource_id = $4
	`

	result, err := s.db.ExecContext(ctx, query, principalType, principalID, resourceType, resourceID)
	if err != nil {
		return fmt.Errorf("failed to revoke ACL rule: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return ErrRuleNotFound
	}

	// Update in-memory model. The DB delete above is authoritative; failure
	// to mirror it in the enforcer is logged best-effort and will self-heal
	// on the next ReloadPolicies / process restart.
	if s.enforcer != nil {
		sub := principalType + ":" + principalID
		obj := resourceType + ":" + resourceID
		if _, err := s.enforcer.RemovePolicy(sub, obj); err != nil {
			logging.Logger.Warn().Err(err).Str("sub", sub).Str("obj", obj).Msg("acl: in-memory RemovePolicy failed during revoke; persisted state authoritative")
		}
	}

	return nil
}

// GetRule retrieves a specific ACL rule
func (s *Service) GetRule(ctx context.Context, principalType, principalID, resourceType, resourceID string) (*ACLRule, error) {
	query := `
		SELECT rule_id, principal_type, principal_id, resource_type, resource_id,
		       access_level, granted_by, granted_at, expires_at, reason
		FROM acl_rules
		WHERE principal_type = $1 AND principal_id = $2
		  AND resource_type = $3 AND resource_id = $4
	`

	rule, err := scanACLRule(s.db.QueryRowContext(ctx, query, principalType, principalID, resourceType, resourceID))

	if err == sql.ErrNoRows {
		return nil, ErrRuleNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get ACL rule: %w", err)
	}

	return rule, nil
}

// ListRules retrieves all ACL rules matching the filter
func (s *Service) ListRules(ctx context.Context, filter RuleFilter) ([]*ACLRule, error) {
	query := `
		SELECT rule_id, principal_type, principal_id, resource_type, resource_id,
		       access_level, granted_by, granted_at, expires_at, reason
		FROM acl_rules
		WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	if filter.PrincipalType != "" {
		query += fmt.Sprintf(" AND principal_type = $%d", argIdx)
		args = append(args, filter.PrincipalType)
		argIdx++
	}

	if filter.PrincipalID != "" {
		query += fmt.Sprintf(" AND principal_id = $%d", argIdx)
		args = append(args, filter.PrincipalID)
		argIdx++
	}

	if filter.ResourceType != "" {
		query += fmt.Sprintf(" AND resource_type = $%d", argIdx)
		args = append(args, filter.ResourceType)
		argIdx++
	}

	if filter.ResourceID != "" {
		query += fmt.Sprintf(" AND resource_id = $%d", argIdx)
		args = append(args, filter.ResourceID)
		// argIdx not incremented: this is the last optional clause and no
		// further $N placeholders are appended after it.
	}

	query += " ORDER BY granted_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACL rules: %w", err)
	}
	defer rows.Close()

	rules := []*ACLRule{}

	for rows.Next() {
		rule, err := scanACLRule(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ACL rule: %w", err)
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ACL rules: %w", err)
	}

	return rules, nil
}

// SetFallbackPolicy updates a fallback policy
func (s *Service) SetFallbackPolicy(ctx context.Context, ruleCategory string, fallbackAccessLevel int, updatedBy string) error {
	if err := ValidateAccessLevel(fallbackAccessLevel); err != nil {
		return err
	}

	query := `
		INSERT INTO acl_fallback_policies (rule_category, fallback_access_level, updated_by, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (rule_category)
		DO UPDATE SET
			fallback_access_level = EXCLUDED.fallback_access_level,
			updated_by = EXCLUDED.updated_by,
			updated_at = EXCLUDED.updated_at
	`

	_, err := s.db.ExecContext(ctx, query, ruleCategory, fallbackAccessLevel, updatedBy, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set fallback policy: %w", err)
	}

	// Invalidate the in-memory cache so the updated policy is picked up immediately.
	s.InvalidateFallbackCache()

	return nil
}

// GetFallbackPolicy retrieves a fallback policy
func (s *Service) GetFallbackPolicy(ctx context.Context, ruleCategory string) (*FallbackPolicy, error) {
	query := `
		SELECT policy_id, rule_category, fallback_access_level, updated_by, updated_at
		FROM acl_fallback_policies
		WHERE rule_category = $1
	`

	var policy FallbackPolicy

	err := s.db.QueryRowContext(ctx, query, ruleCategory).Scan(
		&policy.PolicyID,
		&policy.RuleCategory,
		&policy.FallbackAccessLevel,
		&policy.UpdatedBy,
		&policy.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrFallbackPolicyNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get fallback policy: %w", err)
	}

	return &policy, nil
}

// CleanupExpiredRules removes expired ACL rules and reloads the in-memory model.
func (s *Service) CleanupExpiredRules(ctx context.Context) (int64, error) {
	query := `SELECT cleanup_expired_acl_rules()`

	var deletedCount int64
	err := s.db.QueryRowContext(ctx, query).Scan(&deletedCount)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired rules: %w", err)
	}

	if deletedCount > 0 && s.enforcer != nil {
		if err := s.enforcer.ReloadPolicies(); err != nil {
			logging.Logger.Warn().Err(err).Int64("deleted", deletedCount).Msg("acl: ReloadPolicies failed after cleanup; in-memory enforcer may lag DB until next reload")
		}
	}

	return deletedCount, nil
}

// QueryAuditLog retrieves audit log entries
func (s *Service) QueryAuditLog(ctx context.Context, filter AuditLogFilter) ([]*AuditLogEntry, error) {
	return s.audit.QueryAuditLog(ctx, filter)
}

// CleanupOldAuditLogs removes old audit log entries
func (s *Service) CleanupOldAuditLogs(ctx context.Context, retentionDays int) (int64, error) {
	return s.audit.CleanupOldLogs(ctx, retentionDays)
}

// RuleFilter defines filters for querying ACL rules
type RuleFilter struct {
	PrincipalType string
	PrincipalID   string
	ResourceType  string
	ResourceID    string
}

// Convenience methods for common ACL operations

// CanConnect checks if a principal can connect to an agent or task
func (s *Service) CanConnect(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*ACLDecision, error) {
	return s.CheckAccess(ctx, principal, targetType, targetID, "connect", workspace, sessionID, AccessRead)
}

// CanSendMessage checks if a principal can send a message to a target
func (s *Service) CanSendMessage(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*ACLDecision, error) {
	return s.CheckAccess(ctx, principal, targetType, targetID, "send_message", workspace, sessionID, AccessReadWrite)
}

// CanManageWorkspace checks if a principal can manage a workspace
func (s *Service) CanManageWorkspace(ctx context.Context, principal models.Identity, workspace string, sessionID uuid.UUID) (*ACLDecision, error) {
	return s.CheckAccess(ctx, principal, ResourceTypeWorkspace, workspace, "manage", workspace, sessionID, AccessManage)
}

// CanCreateWorkspace checks if a principal can create a new workspace.
// Uses the typed capability/create_workspace gate.
func (s *Service) CanCreateWorkspace(ctx context.Context, principal models.Identity, sessionID uuid.UUID) (*ACLDecision, error) {
	return s.CheckAccess(ctx, principal, ResourceTypeCapability, PermissionCreateWorkspace, "create_workspace", "", sessionID, AccessRead)
}
