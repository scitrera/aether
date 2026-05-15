// Package sqlite provides the native SQLite implementation of acl.Store.
// Stage 2 of the storage-interfaces refactor: this impl uses sqlite-native
// idioms (TEXT timestamps parsed inline, json_extract for JSON columns,
// INTEGER PRIMARY KEY AUTOINCREMENT, native ON CONFLICT ... DO UPDATE)
// and does NOT go through pkg/dbcompat.
//
// Architecture:
//   - Owns its own *sql.DB handle for ACL state (acl_rules,
//     acl_fallback_policies, acl_authority_grants).
//   - Audit writes go through a shared audit.EventSink (the narrow
//     write-only interface, satisfied by both the legacy
//     *audit.AuditLogger and the native *auditsqlite.Store). The
//     auditDB handle is used only for read-side operations
//     (QueryAuditLog, CleanupOldAuditLogs).
//   - Embeds a Casbin SyncedEnforcer for in-memory rule evaluation,
//     with a custom adapter that reads from acl_rules using native
//     SQLite SQL (no dbcompat translation).
//   - Single-writer enforcement: db.SetMaxOpenConns(1) on the ACL state
//     handle avoids SQLITE_BUSY in WAL mode (per section 14.3 of the
//     storage-interfaces plan).
//
// The acl_audit_log VIEW is created over the comprehensive_audit_log table
// in the audit DB so QueryAuditLog can use the same column projection as
// the postgres VIEW. The view definition lives in
// migrations/sqlite_acl/002_audit_view.sql (applied against the audit DB).
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/model"
	"github.com/casbin/casbin/v3/persist"
	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	sqliteaclmigrations "github.com/scitrera/aether/migrations/sqlite_acl"
	"github.com/scitrera/aether/pkg/models"

	// Register the bare "sqlite" driver from modernc.org/sqlite.
	// Stage 2 native impls use the bare driver, not sqlite_compat,
	// because they own all their own SQL and do inline timestamp parsing.
	_ "modernc.org/sqlite"
)

// Compile-time conformance assert: *Store satisfies acl.Store.
var _ aclstore.Store = (*Store)(nil)

// fallbackCacheTTL matches the legacy internal/acl.Service cache TTL.
const fallbackCacheTTL = 30 * time.Second

// fallbackCacheEntry holds a single cached fallback policy value.
type fallbackCacheEntry struct {
	level    int
	found    bool
	cachedAt time.Time
}

// Store is the native SQLite ACL store. It reimplements the full
// acl.Store interface using sqlite-native SQL without dbcompat.
type Store struct {
	db        *sql.DB // ACL state: acl_rules, acl_fallback_policies, acl_authority_grants
	auditDB   *sql.DB // read-side handle for audit queries (comprehensive_audit_log + acl_audit_log view)
	enforcer  *casbin.SyncedEnforcer
	aclAudit  *acl.AuditLogger // thin adapter that funnels decisions to the shared audit writer
	gatewayID string

	fallbackCache sync.Map // key: string (category), value: fallbackCacheEntry
}

// Casbin model text — identical to internal/acl/enforcer.go.
const casbinModelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act, expires, rule_id

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj
`

// Policy field indices within a Casbin policy slice.
const (
	pIdxSub     = 0
	pIdxObj     = 1
	pIdxAct     = 2
	pIdxExpires = 3
	pIdxRuleID  = 4
)

// New constructs a native SQLite ACL store.
//
// Parameters:
//   - db: the *sql.DB for the ACL state file (acl_rules, acl_fallback_policies,
//     acl_authority_grants). Caller retains ownership; Close() does NOT close it.
//   - sharedAudit: the shared audit writer (owned by the caller), typed as
//     audit.EventSink (the narrow write-only interface). Both the legacy
//     *audit.AuditLogger and the native-sqlite *auditsqlite.Store satisfy
//     this interface. May be nil if audit logging is disabled at the
//     platform level.
//   - auditDB: read-side handle for audit queries. Must point at the same
//     physical file the shared writer targets (audit.db in lite).
//   - gatewayID: stamped on every ACL-decision audit row.
func New(db *sql.DB, sharedAudit audit.EventSink, auditDB *sql.DB, gatewayID string) (*Store, error) {
	// Run ACL-domain migrations against the state DB.
	ctx := context.Background()
	if err := applyMigrations(ctx, db, sqliteaclmigrations.MigrationFS, "sqlite_acl"); err != nil {
		return nil, fmt.Errorf("acl sqlite migrations: %w", err)
	}

	// Create the acl_audit_log view on the audit DB if it has
	// comprehensive_audit_log (it may not exist if auditDB == db and audit
	// is elsewhere; the view is best-effort for QueryAuditLog support).
	if auditDB != nil {
		_ = ensureAuditView(ctx, auditDB)
	}

	// Build the Casbin enforcer backed by the native adapter.
	m, err := model.NewModelFromString(casbinModelText)
	if err != nil {
		return nil, fmt.Errorf("acl: casbin model: %w", err)
	}
	adapter := &sqliteAdapter{db: db}
	enforcer, err := casbin.NewSyncedEnforcer(m, adapter)
	if err != nil {
		return nil, fmt.Errorf("acl: casbin enforcer: %w", err)
	}
	enforcer.EnableAutoSave(false)

	return &Store{
		db:        db,
		auditDB:   auditDB,
		enforcer:  enforcer,
		aclAudit:  acl.NewAuditLogger(sharedAudit, auditDB, gatewayID),
		gatewayID: gatewayID,
	}, nil
}

// Close releases adapter references. The *sql.DB handles are owned by
// the caller; this method only cleans up the audit adapter.
func (s *Store) Close() error {
	if s.aclAudit != nil {
		_ = s.aclAudit.Close()
	}
	return nil
}

// =========================================================================
// Access checks
// =========================================================================

func (s *Store) CheckAccess(ctx context.Context, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*aclstore.Decision, error) {
	resourceType, resourceID, _ = acl.RewriteLegacyPermission(resourceType, resourceID)

	decision, err := s.evaluateAccessNoAudit(ctx, principal, resourceType, resourceID, requiredLevel)
	if err != nil {
		return nil, err
	}

	s.aclAudit.LogDecision(ctx, decision, principal, resourceType, resourceID, operation, workspace, sessionID)
	return decision, nil
}

func (s *Store) CheckAccessWithAuthority(ctx context.Context, actor models.Identity, authority *aclstore.ResolvedAuthority, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*aclstore.Decision, error) {
	if authority == nil {
		return s.CheckAccess(ctx, actor, resourceType, resourceID, operation, workspace, sessionID, requiredLevel)
	}

	if decision := validateGrantConstraints(authority.Grant, resourceType, resourceID, operation, workspace, requiredLevel); decision != nil {
		decision.AuthorityGrant = authority.Grant
		decision.AuthorityMode = "on_behalf_of"
		s.aclAudit.LogDecision(ctx, decision, actor, resourceType, resourceID, operation, workspace, sessionID)
		return decision, nil
	}

	decision, err := s.evaluateAccessNoAudit(ctx, authority.Subject, resourceType, resourceID, requiredLevel)
	if err != nil {
		return nil, err
	}
	decision.AuthorityGrant = authority.Grant
	decision.AuthorityMode = "on_behalf_of"

	s.aclAudit.LogDecision(ctx, decision, actor, resourceType, resourceID, operation, workspace, sessionID)
	return decision, nil
}

func (s *Store) CanConnect(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return s.CheckAccess(ctx, principal, targetType, targetID, "connect", workspace, sessionID, aclstore.AccessRead)
}

func (s *Store) CanSendMessage(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return s.CheckAccess(ctx, principal, targetType, targetID, "send_message", workspace, sessionID, aclstore.AccessReadWrite)
}

func (s *Store) CanManageWorkspace(ctx context.Context, principal models.Identity, workspace string, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return s.CheckAccess(ctx, principal, aclstore.ResourceTypeWorkspace, workspace, "manage", workspace, sessionID, aclstore.AccessManage)
}

func (s *Store) CanCreateWorkspace(ctx context.Context, principal models.Identity, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return s.CheckAccess(ctx, principal, aclstore.ResourceTypeCapability, aclstore.PermissionCreateWorkspace, "create_workspace", "", sessionID, aclstore.AccessRead)
}

// =========================================================================
// Authority grants
// =========================================================================

func (s *Store) ResolveAuthority(ctx context.Context, actor models.Identity, req aclstore.RequestAuthorityContext, audience aclstore.GrantAudienceContext) (*aclstore.ResolvedAuthority, error) {
	if req.GrantID == "" {
		return nil, fmt.Errorf("%w: grant_id is required", aclstore.ErrInvalidAuthorityContext)
	}

	subjectRef := req.Subject.PrincipalRef()
	if subjectRef.IsZero() {
		return nil, fmt.Errorf("%w: subject is required", aclstore.ErrInvalidAuthorityContext)
	}

	grant, err := s.GetAuthorityGrant(ctx, req.GrantID)
	if err != nil {
		return nil, err
	}
	if err := grant.ValidateActiveAt(time.Now()); err != nil {
		return nil, err
	}

	actorRef := actor.PrincipalRef()
	if actorRef.IsZero() {
		return nil, fmt.Errorf("%w: actor principal is incomplete", aclstore.ErrInvalidAuthorityContext)
	}
	if aclstore.PrincipalTypeForModel(actorRef.Type) != grant.DelegateType || actorRef.ID != grant.DelegateID {
		return nil, aclstore.ErrAuthorityGrantDelegateMismatch
	}
	if aclstore.PrincipalTypeForModel(subjectRef.Type) != grant.SubjectType || subjectRef.ID != grant.SubjectID {
		return nil, aclstore.ErrAuthorityGrantSubjectMismatch
	}
	if err := validateGrantAudience(grant, actor, audience); err != nil {
		return nil, err
	}

	return &aclstore.ResolvedAuthority{
		Actor:   actor,
		Subject: req.Subject,
		Grant:   grant,
	}, nil
}

func (s *Store) CreateAuthorityGrant(ctx context.Context, req aclstore.CreateAuthorityGrantRequest) (*aclstore.AuthorityGrant, error) {
	if err := aclstore.ValidateAccessLevel(req.MaxAccessLevel); err != nil {
		return nil, err
	}
	if req.ExpiresAt.IsZero() || req.RenewableUntil.IsZero() {
		return nil, fmt.Errorf("authority grant expiry and renewable_until are required")
	}
	if req.RenewableUntil.Before(req.ExpiresAt) {
		return nil, fmt.Errorf("authority grant renewable_until must be on or after expires_at")
	}

	subjectType, subjectID, err := authorityPrincipalRef(req.Subject)
	if err != nil {
		return nil, fmt.Errorf("invalid subject: %w", err)
	}
	delegateType, delegateID, err := authorityPrincipalRef(req.Delegate)
	if err != nil {
		return nil, fmt.Errorf("invalid delegate: %w", err)
	}
	issuedByType, issuedByID, err := authorityPrincipalRef(req.IssuedBy)
	if err != nil {
		return nil, fmt.Errorf("invalid issued_by: %w", err)
	}

	root := req.Subject
	if req.RootSubject != nil {
		root = *req.RootSubject
	}
	rootType, rootID, err := authorityPrincipalRef(root)
	if err != nil {
		return nil, fmt.Errorf("invalid root subject: %w", err)
	}

	if strings.TrimSpace(req.AudienceType) == "" || strings.TrimSpace(req.AudienceID) == "" {
		return nil, fmt.Errorf("authority grant audience_type and audience_id are required")
	}
	if !isValidAuthorityAudienceType(req.AudienceType) {
		return nil, fmt.Errorf("invalid authority grant audience_type %q", req.AudienceType)
	}
	if req.RemainingHops < 0 {
		return nil, fmt.Errorf("authority grant remaining_hops must be >= 0")
	}
	if req.MayDelegate && req.RemainingHops == 0 {
		return nil, fmt.Errorf("authority grant with may_delegate=true must allow at least one remaining hop")
	}
	if !req.MayDelegate && req.RemainingHops != 0 {
		return nil, fmt.Errorf("authority grant with may_delegate=false cannot have remaining hops")
	}

	rootGrantID := ""
	if req.ParentGrantID != nil {
		parent, err := s.GetAuthorityGrant(ctx, *req.ParentGrantID)
		if err != nil {
			return nil, err
		}
		if err := parent.ValidateActiveAt(time.Now()); err != nil {
			return nil, err
		}
		if !parent.CanDelegate() {
			return nil, aclstore.ErrAuthorityGrantDelegationDenied
		}
		if err := ensurePrincipalMatch("subject", subjectType, subjectID, parent.SubjectType, parent.SubjectID); err != nil {
			return nil, err
		}
		if err := ensurePrincipalMatch("root subject", rootType, rootID, parent.RootSubjectType, parent.RootSubjectID); err != nil {
			return nil, err
		}
		if req.MaxAccessLevel > parent.MaxAccessLevel {
			return nil, aclstore.ErrAuthorityGrantScopeEscalation
		}
		if req.ExpiresAt.After(parent.ExpiresAt) || req.RenewableUntil.After(parent.RenewableUntil) {
			return nil, aclstore.ErrAuthorityGrantScopeEscalation
		}
		if !stringSliceSubset(req.WorkspaceScope, parent.WorkspaceScope) {
			return nil, aclstore.ErrAuthorityGrantScopeEscalation
		}
		if !stringSliceSubset(req.OperationScope, parent.OperationScope) {
			return nil, aclstore.ErrAuthorityGrantScopeEscalation
		}
		if !resourceScopeSubset(req.ResourceScope, parent.ResourceScope) {
			return nil, aclstore.ErrAuthorityGrantScopeEscalation
		}
		if req.RemainingHops > parent.RemainingHops-1 {
			return nil, aclstore.ErrAuthorityGrantDelegationDenied
		}
		rootGrantID = parent.RootGrantID
		if rootGrantID == "" {
			rootGrantID = parent.GrantID
		}
	} else if req.RootSubject != nil {
		if err := ensurePrincipalMatch("root subject", rootType, rootID, subjectType, subjectID); err != nil {
			return nil, err
		}
	}

	workspaceJSON, err := json.Marshal(defaultStringSlice(req.WorkspaceScope))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal workspace scope: %w", err)
	}
	resourceJSON, err := json.Marshal(defaultResourceScope(req.ResourceScope))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resource scope: %w", err)
	}
	operationJSON, err := json.Marshal(defaultStringSlice(req.OperationScope))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal operation scope: %w", err)
	}
	metadataJSON, err := json.Marshal(defaultMetadata(req.Metadata))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal authority grant metadata: %w", err)
	}

	now := time.Now()
	grant := &aclstore.AuthorityGrant{
		GrantID:                  uuid.New().String(),
		RootGrantID:              rootGrantID,
		SubjectType:              subjectType,
		SubjectID:                subjectID,
		DelegateType:             delegateType,
		DelegateID:               delegateID,
		IssuedByType:             issuedByType,
		IssuedByID:               issuedByID,
		RootSubjectType:          rootType,
		RootSubjectID:            rootID,
		ParentGrantID:            req.ParentGrantID,
		MayDelegate:              req.MayDelegate,
		RemainingHops:            req.RemainingHops,
		WorkspaceScope:           defaultStringSlice(req.WorkspaceScope),
		ResourceScope:            defaultResourceScope(req.ResourceScope),
		OperationScope:           defaultStringSlice(req.OperationScope),
		MaxAccessLevel:           req.MaxAccessLevel,
		AudienceType:             req.AudienceType,
		AudienceID:               req.AudienceID,
		ValidWhileAudienceActive: req.ValidWhileAudienceActive,
		ExpiresAt:                req.ExpiresAt,
		RenewableUntil:           req.RenewableUntil,
		Reason:                   req.Reason,
		Metadata:                 defaultMetadata(req.Metadata),
		CreatedAt:                now,
	}
	if grant.RootGrantID == "" {
		grant.RootGrantID = grant.GrantID
	}

	query := `
		INSERT INTO acl_authority_grants (
			grant_id, root_grant_id, subject_type, subject_id, delegate_type, delegate_id,
			issued_by_type, issued_by_id, root_subject_type, root_subject_id,
			parent_grant_id, may_delegate, remaining_hops, workspace_scope,
			resource_scope, operation_scope, max_access_level, audience_type,
			audience_id, valid_while_audience_active, expires_at, renewable_until,
			reason, metadata, created_at
		) VALUES (
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?
		)
	`

	if _, err := s.db.ExecContext(ctx, query,
		grant.GrantID,
		grant.RootGrantID,
		grant.SubjectType,
		grant.SubjectID,
		grant.DelegateType,
		grant.DelegateID,
		grant.IssuedByType,
		grant.IssuedByID,
		grant.RootSubjectType,
		grant.RootSubjectID,
		grant.ParentGrantID,
		grant.MayDelegate,
		grant.RemainingHops,
		string(workspaceJSON),
		string(resourceJSON),
		string(operationJSON),
		grant.MaxAccessLevel,
		grant.AudienceType,
		grant.AudienceID,
		grant.ValidWhileAudienceActive,
		formatTime(grant.ExpiresAt),
		formatTime(grant.RenewableUntil),
		grant.Reason,
		string(metadataJSON),
		formatTime(grant.CreatedAt),
	); err != nil {
		return nil, fmt.Errorf("failed to create authority grant: %w", err)
	}

	return grant, nil
}

func (s *Store) GetAuthorityGrant(ctx context.Context, grantID string) (*aclstore.AuthorityGrant, error) {
	query := `
		SELECT grant_id, root_grant_id, subject_type, subject_id, delegate_type, delegate_id,
		       issued_by_type, issued_by_id, root_subject_type, root_subject_id,
		       parent_grant_id, may_delegate, remaining_hops, workspace_scope,
		       resource_scope, operation_scope, max_access_level, audience_type,
		       audience_id, valid_while_audience_active, expires_at, renewable_until,
		       renewed_at, revoked, revoked_at, reason, metadata, created_at
		FROM acl_authority_grants
		WHERE grant_id = ?
	`

	grant, err := scanAuthorityGrant(s.db.QueryRowContext(ctx, query, grantID))
	if err == sql.ErrNoRows {
		return nil, aclstore.ErrAuthorityGrantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get authority grant: %w", err)
	}
	return grant, nil
}

func (s *Store) ListAuthorityGrants(ctx context.Context, filter aclstore.AuthorityGrantFilter) ([]*aclstore.AuthorityGrant, error) {
	query := `
		SELECT grant_id, root_grant_id, subject_type, subject_id, delegate_type, delegate_id,
		       issued_by_type, issued_by_id, root_subject_type, root_subject_id,
		       parent_grant_id, may_delegate, remaining_hops, workspace_scope,
		       resource_scope, operation_scope, max_access_level, audience_type,
		       audience_id, valid_while_audience_active, expires_at, renewable_until,
		       renewed_at, revoked, revoked_at, reason, metadata, created_at
		FROM acl_authority_grants
		WHERE 1=1
	`
	var args []interface{}

	if filter.SubjectType != "" {
		query += " AND subject_type = ?"
		args = append(args, filter.SubjectType)
	}
	if filter.RootGrantID != "" {
		query += " AND root_grant_id = ?"
		args = append(args, filter.RootGrantID)
	}
	if filter.SubjectID != "" {
		query += " AND subject_id = ?"
		args = append(args, filter.SubjectID)
	}
	if filter.DelegateType != "" {
		query += " AND delegate_type = ?"
		args = append(args, filter.DelegateType)
	}
	if filter.DelegateID != "" {
		query += " AND delegate_id = ?"
		args = append(args, filter.DelegateID)
	}
	if filter.AudienceType != "" {
		query += " AND audience_type = ?"
		args = append(args, filter.AudienceType)
	}
	if filter.AudienceID != "" {
		query += " AND audience_id = ?"
		args = append(args, filter.AudienceID)
	}
	if !filter.IncludeRevoked {
		query += " AND revoked = 0"
	}
	if filter.ActiveOnly {
		query += " AND revoked = 0 AND expires_at > ?"
		args = append(args, formatTime(time.Now()))
	}

	query += " ORDER BY created_at DESC"

	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}
	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list authority grants: %w", err)
	}
	defer rows.Close()

	grants := make([]*aclstore.AuthorityGrant, 0)
	for rows.Next() {
		grant, err := scanAuthorityGrant(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan authority grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating authority grants: %w", err)
	}
	return grants, nil
}

func (s *Store) RenewAuthorityGrant(ctx context.Context, grantID string, expiresAt time.Time) (*aclstore.AuthorityGrant, error) {
	return s.RenewAuthorityGrantOpts(ctx, grantID, aclstore.RenewAuthorityGrantOpts{ExpiresAt: expiresAt})
}

func (s *Store) RenewAuthorityGrantOpts(ctx context.Context, grantID string, opts aclstore.RenewAuthorityGrantOpts) (*aclstore.AuthorityGrant, error) {
	grant, err := s.GetAuthorityGrant(ctx, grantID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	switch err := grant.ValidateActiveAt(now); err {
	case nil:
	case aclstore.ErrAuthorityGrantRevoked, aclstore.ErrAuthorityGrantExpired:
		return nil, err
	default:
		return nil, err
	}

	expiresAt := computeRenewedExpiry(opts, grant.RenewableUntil, now)

	switch {
	case expiresAt.IsZero():
		return nil, fmt.Errorf("renewed expires_at must be specified (set ExpiresAt or ExtendSeconds)")
	case expiresAt.After(grant.RenewableUntil):
		return nil, aclstore.ErrAuthorityGrantRenewal
	case expiresAt.Before(now):
		return nil, fmt.Errorf("renewed expires_at must be in the future")
	}

	if grant.ParentGrantID != nil {
		parent, err := s.GetAuthorityGrant(ctx, *grant.ParentGrantID)
		if err != nil {
			return nil, err
		}
		if err := parent.ValidateActiveAt(now); err != nil {
			return nil, err
		}
		if opts.ExtendSeconds > 0 && expiresAt.After(parent.ExpiresAt) {
			expiresAt = parent.ExpiresAt
		}
		if expiresAt.After(parent.ExpiresAt) || grant.RenewableUntil.After(parent.RenewableUntil) {
			return nil, aclstore.ErrAuthorityGrantRenewal
		}
	}

	renewedAt := now
	query := `
		UPDATE acl_authority_grants
		SET expires_at = ?, renewed_at = ?
		WHERE grant_id = ?
	`
	result, err := s.db.ExecContext(ctx, query, formatTime(expiresAt), formatTime(renewedAt), grantID)
	if err != nil {
		return nil, fmt.Errorf("failed to renew authority grant: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return nil, aclstore.ErrAuthorityGrantNotFound
	}

	grant.ExpiresAt = expiresAt
	grant.RenewedAt = &renewedAt
	return grant, nil
}

func (s *Store) RevokeAuthorityGrant(ctx context.Context, grantID string) error {
	_, err := s.RevokeAuthorityGrantCascade(ctx, grantID)
	return err
}

// RevokeAuthorityGrantCascade revokes the target grant and its full
// descendant subtree using a recursive CTE (SQLite supports WITH RECURSIVE
// natively since 3.8.3). Returns ErrAuthorityGrantNotFound when the target
// doesn't exist or all descendants are already revoked.
func (s *Store) RevokeAuthorityGrantCascade(ctx context.Context, grantID string) ([]aclstore.RevokedAuthorityGrant, error) {
	now := formatTime(time.Now())

	// SQLite supports UPDATE ... FROM ... but not UPDATE ... WHERE grant_id IN (CTE).
	// Use a two-step approach: collect descendants, then update.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin revoke tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Collect all descendant grant_ids (including the root).
	collectQuery := `
		WITH RECURSIVE descendants AS (
			SELECT grant_id
			FROM acl_authority_grants
			WHERE grant_id = ?
			UNION ALL
			SELECT child.grant_id
			FROM acl_authority_grants child
			JOIN descendants d ON child.parent_grant_id = d.grant_id
		)
		SELECT grant_id FROM descendants
	`
	rows, err := tx.QueryContext(ctx, collectQuery, grantID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect descendant grants: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan descendant grant id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating descendant grants: %w", err)
	}
	if len(ids) == 0 {
		return nil, aclstore.ErrAuthorityGrantNotFound
	}

	// Build placeholders for the UPDATE.
	placeholders := make([]string, len(ids))
	updateArgs := make([]interface{}, 0, len(ids)+1)
	updateArgs = append(updateArgs, now)
	for i, id := range ids {
		placeholders[i] = "?"
		updateArgs = append(updateArgs, id)
	}

	updateQuery := fmt.Sprintf(`
		UPDATE acl_authority_grants
		SET revoked = 1, revoked_at = ?
		WHERE grant_id IN (%s)
		  AND revoked = 0
	`, strings.Join(placeholders, ","))

	if _, err := tx.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
		return nil, fmt.Errorf("failed to revoke grants: %w", err)
	}

	// Read back the affected rows.
	readPlaceholders := make([]string, len(ids))
	readArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		readPlaceholders[i] = "?"
		readArgs[i] = id
	}
	readQuery := fmt.Sprintf(`
		SELECT grant_id, root_grant_id, delegate_type, delegate_id
		FROM acl_authority_grants
		WHERE grant_id IN (%s)
		  AND revoked = 1
	`, strings.Join(readPlaceholders, ","))

	readRows, err := tx.QueryContext(ctx, readQuery, readArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to read revoked grants: %w", err)
	}
	var revoked []aclstore.RevokedAuthorityGrant
	for readRows.Next() {
		var r aclstore.RevokedAuthorityGrant
		if err := readRows.Scan(&r.GrantID, &r.RootGrantID, &r.DelegateType, &r.DelegateID); err != nil {
			readRows.Close()
			return nil, fmt.Errorf("failed to scan revoked grant: %w", err)
		}
		r.IsRoot = r.GrantID == grantID
		revoked = append(revoked, r)
	}
	readRows.Close()
	if err := readRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating revoked grants: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit revoke tx: %w", err)
	}

	if len(revoked) == 0 {
		return nil, aclstore.ErrAuthorityGrantNotFound
	}
	return revoked, nil
}

func (s *Store) ListVisibleGrants(ctx context.Context, filter aclstore.VisibleGrantsFilter) ([]*aclstore.AuthorityGrant, error) {
	if !filter.ByDelegate && !filter.BySubject {
		return []*aclstore.AuthorityGrant{}, nil
	}

	actorType, actorID, err := authorityPrincipalRef(filter.Actor)
	if err != nil {
		return nil, fmt.Errorf("invalid actor: %w", err)
	}

	query := `
		SELECT grant_id, root_grant_id, subject_type, subject_id, delegate_type, delegate_id,
		       issued_by_type, issued_by_id, root_subject_type, root_subject_id,
		       parent_grant_id, may_delegate, remaining_hops, workspace_scope,
		       resource_scope, operation_scope, max_access_level, audience_type,
		       audience_id, valid_while_audience_active, expires_at, renewable_until,
		       renewed_at, revoked, revoked_at, reason, metadata, created_at
		FROM acl_authority_grants
		WHERE 1=1
	`
	var args []interface{}

	switch {
	case filter.ByDelegate && filter.BySubject:
		query += " AND ((delegate_type = ? AND delegate_id = ?) OR (subject_type = ? AND subject_id = ?))"
		args = append(args, actorType, actorID, actorType, actorID)
	case filter.ByDelegate:
		query += " AND delegate_type = ? AND delegate_id = ?"
		args = append(args, actorType, actorID)
	case filter.BySubject:
		query += " AND subject_type = ? AND subject_id = ?"
		args = append(args, actorType, actorID)
	}

	if filter.AudienceType != "" {
		query += " AND audience_type = ?"
		args = append(args, filter.AudienceType)
	}
	if filter.AudienceID != "" {
		query += " AND audience_id = ?"
		args = append(args, filter.AudienceID)
	}
	if !filter.IncludeRevoked {
		query += " AND revoked = 0"
	}

	query += " ORDER BY created_at DESC"

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += " LIMIT ?"
	args = append(args, limit)

	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list visible authority grants: %w", err)
	}
	defer rows.Close()

	grants := make([]*aclstore.AuthorityGrant, 0)
	for rows.Next() {
		grant, err := scanAuthorityGrant(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan authority grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating visible authority grants: %w", err)
	}
	return grants, nil
}

func (s *Store) FindVisibleDerivedGrant(ctx context.Context, parentGrantID string, target models.Identity, audienceType, audienceID string) (*aclstore.AuthorityGrant, error) {
	if parentGrantID == "" {
		return nil, fmt.Errorf("parent_grant_id is required")
	}
	delegateType, delegateID, err := authorityPrincipalRef(target)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	query := `
		SELECT grant_id, root_grant_id, subject_type, subject_id, delegate_type, delegate_id,
		       issued_by_type, issued_by_id, root_subject_type, root_subject_id,
		       parent_grant_id, may_delegate, remaining_hops, workspace_scope,
		       resource_scope, operation_scope, max_access_level, audience_type,
		       audience_id, valid_while_audience_active, expires_at, renewable_until,
		       renewed_at, revoked, revoked_at, reason, metadata, created_at
		FROM acl_authority_grants
		WHERE parent_grant_id = ?
		  AND delegate_type = ?
		  AND delegate_id = ?
		  AND audience_type = ?
		  AND audience_id = ?
		  AND revoked = 0
		  AND expires_at > ?
		ORDER BY created_at DESC
		LIMIT 1
	`
	row := s.db.QueryRowContext(ctx, query, parentGrantID, delegateType, delegateID, audienceType, audienceID, formatTime(time.Now()))
	grant, err := scanAuthorityGrant(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find visible derived grant: %w", err)
	}
	return grant, nil
}

// =========================================================================
// Grants / Rules (direct ACL rule CRUD)
// =========================================================================

func (s *Store) GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*aclstore.Rule, error) {
	resourceType, resourceID, _ = acl.RewriteLegacyPermission(resourceType, resourceID)

	if err := aclstore.ValidateAccessLevel(accessLevel); err != nil {
		return nil, err
	}
	if err := acl.ValidateGlobPattern(principalID, "principal_id"); err != nil {
		return nil, err
	}
	if err := acl.ValidateGlobPattern(resourceID, "resource_id"); err != nil {
		return nil, err
	}

	rule := &aclstore.Rule{
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

	var expiresAtStr *string
	if expiresAt != nil {
		s := formatTime(*expiresAt)
		expiresAtStr = &s
	}

	// Native ON CONFLICT ... DO UPDATE — no dbcompat needed.
	query := `
		INSERT INTO acl_rules (
			rule_id, principal_type, principal_id, resource_type, resource_id,
			access_level, granted_by, granted_at, expires_at, reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (principal_type, principal_id, resource_type, resource_id)
		DO UPDATE SET
			access_level = excluded.access_level,
			granted_by = excluded.granted_by,
			granted_at = excluded.granted_at,
			expires_at = excluded.expires_at,
			reason = excluded.reason
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
		formatTime(rule.GrantedAt),
		expiresAtStr,
		rule.Reason,
	).Scan(&rule.RuleID)

	if err != nil {
		return nil, fmt.Errorf("failed to create ACL rule: %w", err)
	}

	// Update in-memory Casbin model.
	sub := principalType + ":" + principalID
	obj := resourceType + ":" + resourceID
	act := strconv.Itoa(accessLevel)
	exp := ""
	if expiresAt != nil {
		exp = expiresAt.Format(time.RFC3339)
	}

	if _, err := s.enforcer.RemoveFilteredPolicy(pIdxSub, sub, obj); err != nil {
		logging.Logger.Warn().Err(err).Str("sub", sub).Str("obj", obj).Msg("acl sqlite: in-memory RemovePolicy failed; persisted state unchanged")
	}
	if _, err := s.enforcer.AddPolicy(sub, obj, act, exp, rule.RuleID); err != nil {
		logging.Logger.Warn().Err(err).Str("sub", sub).Str("obj", obj).Msg("acl sqlite: in-memory AddPolicy failed; persisted state unchanged")
	}

	return rule, nil
}

func (s *Store) RevokeAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error {
	resourceType, resourceID, _ = acl.RewriteLegacyPermission(resourceType, resourceID)

	query := `
		DELETE FROM acl_rules
		WHERE principal_type = ? AND principal_id = ?
		  AND resource_type = ? AND resource_id = ?
	`

	result, err := s.db.ExecContext(ctx, query, principalType, principalID, resourceType, resourceID)
	if err != nil {
		return fmt.Errorf("failed to revoke ACL rule: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return aclstore.ErrRuleNotFound
	}

	// Update in-memory model.
	sub := principalType + ":" + principalID
	obj := resourceType + ":" + resourceID
	if _, err := s.enforcer.RemoveFilteredPolicy(pIdxSub, sub, obj); err != nil {
		logging.Logger.Warn().Err(err).Str("sub", sub).Str("obj", obj).Msg("acl sqlite: in-memory RemovePolicy failed during revoke; persisted state authoritative")
	}

	return nil
}

func (s *Store) GetRule(ctx context.Context, principalType, principalID, resourceType, resourceID string) (*aclstore.Rule, error) {
	query := `
		SELECT rule_id, principal_type, principal_id, resource_type, resource_id,
		       access_level, granted_by, granted_at, expires_at, reason
		FROM acl_rules
		WHERE principal_type = ? AND principal_id = ?
		  AND resource_type = ? AND resource_id = ?
	`

	rule, err := scanACLRule(s.db.QueryRowContext(ctx, query, principalType, principalID, resourceType, resourceID))
	if err == sql.ErrNoRows {
		return nil, aclstore.ErrRuleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL rule: %w", err)
	}
	return rule, nil
}

func (s *Store) ListRules(ctx context.Context, filter aclstore.RuleFilter) ([]*aclstore.Rule, error) {
	query := `
		SELECT rule_id, principal_type, principal_id, resource_type, resource_id,
		       access_level, granted_by, granted_at, expires_at, reason
		FROM acl_rules
		WHERE 1=1
	`
	var args []interface{}

	if filter.PrincipalType != "" {
		query += " AND principal_type = ?"
		args = append(args, filter.PrincipalType)
	}
	if filter.PrincipalID != "" {
		query += " AND principal_id = ?"
		args = append(args, filter.PrincipalID)
	}
	if filter.ResourceType != "" {
		query += " AND resource_type = ?"
		args = append(args, filter.ResourceType)
	}
	if filter.ResourceID != "" {
		query += " AND resource_id = ?"
		args = append(args, filter.ResourceID)
	}

	query += " ORDER BY granted_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACL rules: %w", err)
	}
	defer rows.Close()

	rules := []*aclstore.Rule{}
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

// =========================================================================
// Fallback policy
// =========================================================================

func (s *Store) SetFallbackPolicy(ctx context.Context, ruleCategory string, fallbackAccessLevel int, updatedBy string) error {
	if err := aclstore.ValidateAccessLevel(fallbackAccessLevel); err != nil {
		return err
	}

	// Native ON CONFLICT ... DO UPDATE with Go-side UUID generation —
	// closes the Stage 1 gap where the legacy SetFallbackPolicy omitted
	// policy_id and relied on gen_random_uuid().
	query := `
		INSERT INTO acl_fallback_policies (policy_id, rule_category, fallback_access_level, updated_by, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (rule_category)
		DO UPDATE SET
			fallback_access_level = excluded.fallback_access_level,
			updated_by = excluded.updated_by,
			updated_at = excluded.updated_at
	`

	_, err := s.db.ExecContext(ctx, query, uuid.New().String(), ruleCategory, fallbackAccessLevel, updatedBy, formatTime(time.Now()))
	if err != nil {
		return fmt.Errorf("failed to set fallback policy: %w", err)
	}

	s.InvalidateFallbackCache()
	return nil
}

func (s *Store) GetFallbackPolicy(ctx context.Context, ruleCategory string) (*aclstore.FallbackPolicy, error) {
	query := `
		SELECT policy_id, rule_category, fallback_access_level, updated_by, updated_at
		FROM acl_fallback_policies
		WHERE rule_category = ?
	`

	var policy aclstore.FallbackPolicy
	var updatedAtStr string

	err := s.db.QueryRowContext(ctx, query, ruleCategory).Scan(
		&policy.PolicyID,
		&policy.RuleCategory,
		&policy.FallbackAccessLevel,
		&policy.UpdatedBy,
		&updatedAtStr,
	)
	if err == sql.ErrNoRows {
		return nil, aclstore.ErrFallbackPolicyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get fallback policy: %w", err)
	}

	policy.UpdatedAt = parseTime(updatedAtStr)
	return &policy, nil
}

func (s *Store) InvalidateFallbackCache() {
	s.fallbackCache.Range(func(key, _ interface{}) bool {
		s.fallbackCache.Delete(key)
		return true
	})
}

// =========================================================================
// Cleanup
// =========================================================================

// CleanupExpiredRules deletes expired rules using a native parameterized
// DELETE (replacing the postgres cleanup_expired_acl_rules() stored function).
// Reloads the in-memory Casbin model after deletion.
func (s *Store) CleanupExpiredRules(ctx context.Context) (int64, error) {
	now := formatTime(time.Now())

	result, err := s.db.ExecContext(ctx, `
		DELETE FROM acl_rules
		WHERE expires_at IS NOT NULL
		  AND expires_at < ?
	`, now)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired rules: %w", err)
	}

	deletedCount, _ := result.RowsAffected()

	if deletedCount > 0 {
		if err := s.enforcer.LoadPolicy(); err != nil {
			logging.Logger.Warn().Err(err).Int64("deleted", deletedCount).Msg("acl sqlite: LoadPolicy failed after cleanup; in-memory enforcer may lag DB until next reload")
		}
	}

	return deletedCount, nil
}

// =========================================================================
// Audit
// =========================================================================

// QueryAuditLog retrieves ACL-decision rows from the acl_audit_log view
// (a native SQLite view over comprehensive_audit_log) in the audit DB.
func (s *Store) QueryAuditLog(ctx context.Context, filter aclstore.AuditLogFilter) ([]*aclstore.AuditLogEntry, error) {
	if s.auditDB == nil {
		return nil, fmt.Errorf("acl audit logger has no read database handle")
	}
	query := `
		SELECT audit_id, timestamp, decision, access_level, principal_type, principal_id,
		       subject_type, subject_id, root_subject_type, root_subject_id,
		       authority_mode, root_authority_grant_id, authority_grant_id, parent_authority_grant_id,
		       resource_type, resource_id, operation, workspace,
		       rule_id, fallback_applied, gateway_id, session_id, metadata
		FROM acl_audit_log
		WHERE 1=1
	`
	var args []interface{}

	if filter.StartTime != nil {
		query += " AND timestamp >= ?"
		args = append(args, formatTime(*filter.StartTime))
	}
	if filter.EndTime != nil {
		query += " AND timestamp <= ?"
		args = append(args, formatTime(*filter.EndTime))
	}
	if filter.PrincipalType != "" {
		query += " AND principal_type = ?"
		args = append(args, filter.PrincipalType)
	}
	if filter.PrincipalID != "" {
		query += " AND principal_id = ?"
		args = append(args, filter.PrincipalID)
	}
	if filter.ResourceType != "" {
		query += " AND resource_type = ?"
		args = append(args, filter.ResourceType)
	}
	if filter.ResourceID != "" {
		query += " AND resource_id = ?"
		args = append(args, filter.ResourceID)
	}
	if filter.Decision != "" {
		query += " AND decision = ?"
		args = append(args, filter.Decision)
	}
	if filter.Workspace != "" {
		query += " AND workspace = ?"
		args = append(args, filter.Workspace)
	}

	query += " ORDER BY timestamp DESC"

	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	rows, err := s.auditDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit log: %w", err)
	}
	defer rows.Close()

	entries := []*aclstore.AuditLogEntry{}
	for rows.Next() {
		entry, err := scanAuditLogEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan audit entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating audit log: %w", err)
	}
	return entries, nil
}

// CleanupOldAuditLogs deletes audit log rows older than retentionDays
// using a native parameterized DELETE (replacing the postgres
// cleanup_old_audit_logs() stored function).
func (s *Store) CleanupOldAuditLogs(ctx context.Context, retentionDays int) (int64, error) {
	if s.auditDB == nil {
		return 0, fmt.Errorf("acl audit logger has no read database handle")
	}

	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	result, err := s.auditDB.ExecContext(ctx, `
		DELETE FROM comprehensive_audit_log
		WHERE timestamp < ?
	`, formatTime(cutoff))
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup old audit logs: %w", err)
	}

	deletedCount, _ := result.RowsAffected()
	return deletedCount, nil
}

// =========================================================================
// Internal: access evaluation (Casbin-backed, same semantics as legacy)
// =========================================================================

func (s *Store) evaluateAccessNoAudit(ctx context.Context, principal models.Identity, resourceType, resourceID string, requiredLevel int) (*aclstore.Decision, error) {
	if s.enforcer != nil {
		decision, err := s.evaluateAccess(ctx, principal, resourceType, resourceID, requiredLevel)
		if err != nil {
			return nil, err
		}
		if decision != nil {
			return decision, nil
		}
	} else {
		return &aclstore.Decision{
			Allowed:              false,
			EffectiveAccessLevel: aclstore.AccessNone,
			Decision:             aclstore.DecisionDeny,
			Reason:               "ACL enforcer not initialized — denying by default",
		}, nil
	}

	return s.applyFallback(ctx, principal, resourceType, requiredLevel)
}

func (s *Store) evaluateAccess(_ context.Context, principal models.Identity, resourceType, resourceID string, requiredLevel int) (*aclstore.Decision, error) {
	principalType := aclstore.PrincipalTypeForModel(principal.Type)
	principalID := principal.CanonicalPrincipalID()

	sub := principalType + ":" + principalID
	obj := resourceType + ":" + resourceID

	// Step 1: Exact principal + exact resource
	if decision := findAndEvaluate(s.enforcer, sub, obj, requiredLevel, "Explicit rule"); decision != nil {
		return decision, nil
	}

	// Step 2: Wildcard principal + exact resource
	for _, wSub := range wildcardSubjects(principalType) {
		if decision := findAndEvaluate(s.enforcer, wSub, obj, requiredLevel, "Wildcard rule"); decision != nil {
			return decision, nil
		}
	}

	// Step 3: Exact principal + wildcard resource
	if resourceID != aclstore.WildcardAnyResource {
		wObj := resourceType + ":" + aclstore.WildcardAnyResource

		if decision := findAndEvaluate(s.enforcer, sub, wObj, requiredLevel, "Any-resource rule"); decision != nil {
			return decision, nil
		}

		// Step 4: Wildcard principal + wildcard resource
		for _, wSub := range wildcardSubjects(principalType) {
			if decision := findAndEvaluate(s.enforcer, wSub, wObj, requiredLevel, "Wildcard any-resource rule"); decision != nil {
				return decision, nil
			}
		}
	}

	// Step 5: Glob-pattern rules
	if decision := findGlobMatch(s.enforcer, sub, obj, requiredLevel); decision != nil {
		return decision, nil
	}

	// Step 6: No match — caller applies fallback
	return nil, nil
}

func (s *Store) applyFallback(ctx context.Context, principal models.Identity, resourceType string, requiredLevel int) (*aclstore.Decision, error) {
	principalType := aclstore.PrincipalTypeForModel(principal.Type)
	category := aclstore.RuleCategory(principalType, resourceType)

	if raw, ok := s.fallbackCache.Load(category); ok {
		entry := raw.(fallbackCacheEntry)
		if time.Since(entry.cachedAt) < fallbackCacheTTL {
			return buildFallbackDecision(category, entry.level, entry.found, requiredLevel), nil
		}
	}

	var fallbackLevel int
	err := s.db.QueryRowContext(ctx, `
		SELECT fallback_access_level
		FROM acl_fallback_policies
		WHERE rule_category = ?
	`, category).Scan(&fallbackLevel)

	if err == sql.ErrNoRows {
		s.fallbackCache.Store(category, fallbackCacheEntry{level: 0, found: false, cachedAt: time.Now()})
		return buildFallbackDecision(category, 0, false, requiredLevel), nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query fallback policy: %w", err)
	}

	s.fallbackCache.Store(category, fallbackCacheEntry{level: fallbackLevel, found: true, cachedAt: time.Now()})
	return buildFallbackDecision(category, fallbackLevel, true, requiredLevel), nil
}

// =========================================================================
// Internal: Casbin evaluation helpers
// =========================================================================

func findAndEvaluate(e *casbin.SyncedEnforcer, sub, obj string, requiredLevel int, label string) *aclstore.Decision {
	policies, _ := e.GetFilteredPolicy(pIdxSub, sub, obj)
	if len(policies) == 0 {
		return nil
	}

	bestLevel := -1
	bestRuleID := ""
	for _, p := range policies {
		if len(p) > pIdxExpires && p[pIdxExpires] != "" {
			expiresAt, err := time.Parse(time.RFC3339, p[pIdxExpires])
			if err == nil && time.Now().After(expiresAt) {
				continue
			}
		}
		level, err := strconv.Atoi(p[pIdxAct])
		if err != nil {
			continue
		}
		if level > bestLevel {
			bestLevel = level
			if len(p) > pIdxRuleID {
				bestRuleID = p[pIdxRuleID]
			}
		}
	}

	if bestLevel < 0 {
		return nil
	}

	decision := &aclstore.Decision{
		Allowed:              bestLevel >= requiredLevel,
		EffectiveAccessLevel: bestLevel,
		Decision:             aclstore.DecisionDeny,
		Reason:               fmt.Sprintf("%s: %s", label, aclstore.AccessLevelName(bestLevel)),
	}
	if decision.Allowed {
		decision.Decision = aclstore.DecisionAllow
	}
	if bestRuleID != "" {
		decision.RuleApplied = &aclstore.Rule{
			RuleID:      bestRuleID,
			AccessLevel: bestLevel,
		}
	}
	return decision
}

func findGlobMatch(e *casbin.SyncedEnforcer, sub, obj string, requiredLevel int) *aclstore.Decision {
	policies, _ := e.GetPolicy()
	if len(policies) == 0 {
		return nil
	}

	bestLevel := -1
	bestRuleID := ""
	for _, p := range policies {
		if len(p) < 3 {
			continue
		}
		pSub, pObj := p[pIdxSub], p[pIdxObj]
		if !strings.ContainsAny(pSub, "*?") && !strings.ContainsAny(pObj, "*?") {
			continue
		}
		if !acl.GlobMatch(sub, pSub) || !acl.GlobMatch(obj, pObj) {
			continue
		}
		if len(p) > pIdxExpires && p[pIdxExpires] != "" {
			expiresAt, err := time.Parse(time.RFC3339, p[pIdxExpires])
			if err == nil && time.Now().After(expiresAt) {
				continue
			}
		}
		level, err := strconv.Atoi(p[pIdxAct])
		if err != nil {
			continue
		}
		if level > bestLevel {
			bestLevel = level
			if len(p) > pIdxRuleID {
				bestRuleID = p[pIdxRuleID]
			}
		}
	}

	if bestLevel < 0 {
		return nil
	}

	decision := &aclstore.Decision{
		Allowed:              bestLevel >= requiredLevel,
		EffectiveAccessLevel: bestLevel,
		Decision:             aclstore.DecisionDeny,
		Reason:               fmt.Sprintf("Glob pattern rule: %s", aclstore.AccessLevelName(bestLevel)),
	}
	if decision.Allowed {
		decision.Decision = aclstore.DecisionAllow
	}
	if bestRuleID != "" {
		decision.RuleApplied = &aclstore.Rule{
			RuleID:      bestRuleID,
			AccessLevel: bestLevel,
		}
	}
	return decision
}

func wildcardSubjects(principalType string) []string {
	switch principalType {
	case aclstore.PrincipalTypeUser:
		return []string{aclstore.PrincipalTypeWildcard + ":" + aclstore.WildcardAnyAuthenticatedUser}
	case aclstore.PrincipalTypeAgent:
		return []string{aclstore.PrincipalTypeWildcard + ":" + aclstore.WildcardAnyAgent}
	case aclstore.PrincipalTypeTask:
		return []string{aclstore.PrincipalTypeWildcard + ":" + aclstore.WildcardAnyTask}
	case aclstore.PrincipalTypeService:
		return []string{aclstore.PrincipalTypeWildcard + ":" + aclstore.WildcardAnyService}
	default:
		return nil
	}
}

func buildFallbackDecision(category string, fallbackLevel int, found bool, requiredLevel int) *aclstore.Decision {
	if !found {
		return &aclstore.Decision{
			Allowed:              false,
			EffectiveAccessLevel: aclstore.AccessNone,
			Decision:             aclstore.DecisionDeny,
			FallbackApplied:      true,
			Reason:               fmt.Sprintf("No rule or fallback policy for %s", category),
		}
	}
	decision := &aclstore.Decision{
		Allowed:              fallbackLevel >= requiredLevel,
		EffectiveAccessLevel: fallbackLevel,
		Decision:             aclstore.DecisionAllow,
		FallbackApplied:      true,
		Reason:               fmt.Sprintf("Fallback policy for %s: %s", category, aclstore.AccessLevelName(fallbackLevel)),
	}
	if !decision.Allowed {
		decision.Decision = aclstore.DecisionDeny
	}
	return decision
}

// =========================================================================
// Internal: authority constraint / audience validation
// =========================================================================

func validateGrantConstraints(grant *aclstore.AuthorityGrant, resourceType, resourceID, operation, workspace string, requiredLevel int) *aclstore.Decision {
	switch {
	case requiredLevel > grant.MaxAccessLevel:
		return authorityDenyDecision(grant, aclstore.ErrAuthorityGrantScopeEscalation.Error())
	case !matchesConstraintValue(operation, grant.OperationScope):
		return authorityDenyDecision(grant, aclstore.ErrAuthorityGrantOperationDenied.Error())
	case workspace != "" && !matchesWorkspaceConstraint(workspace, grant.WorkspaceScope):
		return authorityDenyDecision(grant, aclstore.ErrAuthorityGrantWorkspaceDenied.Error())
	}

	if len(grant.ResourceScope) > 0 {
		patterns, ok := grant.ResourceScope[resourceType]
		if !ok || !matchesConstraintValue(resourceID, patterns) {
			return authorityDenyDecision(grant, aclstore.ErrAuthorityGrantResourceDenied.Error())
		}
	}
	return nil
}

func authorityDenyDecision(grant *aclstore.AuthorityGrant, reason string) *aclstore.Decision {
	return &aclstore.Decision{
		Allowed:              false,
		EffectiveAccessLevel: grant.MaxAccessLevel,
		Decision:             aclstore.DecisionDeny,
		AuthorityGrant:       grant,
		AuthorityMode:        "on_behalf_of",
		Reason:               reason,
	}
}

func validateGrantAudience(grant *aclstore.AuthorityGrant, actor models.Identity, audience aclstore.GrantAudienceContext) error {
	switch grant.AudienceType {
	case aclstore.AuthorityAudienceSession:
		currentSessionID := ""
		if audience.SessionID != uuid.Nil {
			currentSessionID = audience.SessionID.String()
		}
		strictMatch := currentSessionID != "" && grant.AudienceID == currentSessionID
		if strictMatch {
			if grant.ValidWhileAudienceActive && audience.SessionActive != nil {
				if !audience.SessionActive(audience.SessionID) {
					return aclstore.ErrAuthorityGrantAudienceMismatch
				}
			}
			break
		}
		if !grant.ValidWhileAudienceActive || audience.SessionActive == nil {
			return aclstore.ErrAuthorityGrantAudienceMismatch
		}
		boundUUID, err := uuid.Parse(grant.AudienceID)
		if err != nil || !audience.SessionActive(boundUUID) {
			return aclstore.ErrAuthorityGrantAudienceMismatch
		}
	case aclstore.AuthorityAudienceTask:
		if audience.AssociatedTaskID == "" || grant.AudienceID != audience.AssociatedTaskID {
			return aclstore.ErrAuthorityGrantAudienceMismatch
		}
		if grant.ValidWhileAudienceActive && audience.TaskActive != nil {
			if !audience.TaskActive(grant.AudienceID) {
				return aclstore.ErrAuthorityGrantAudienceMismatch
			}
		}
	case aclstore.AuthorityAudienceAgent:
		if actor.Type != models.PrincipalAgent || actor.CanonicalPrincipalID() != grant.AudienceID {
			return aclstore.ErrAuthorityGrantAudienceMismatch
		}
	case aclstore.AuthorityAudienceService:
		if actor.Type != models.PrincipalService || actor.CanonicalPrincipalID() != grant.AudienceID {
			return aclstore.ErrAuthorityGrantAudienceMismatch
		}
	default:
		return aclstore.ErrAuthorityGrantAudienceMismatch
	}
	return nil
}

func matchesConstraintValue(value string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if pattern == value {
			return true
		}
		if matched, _ := acl.GlobMatchPath(pattern, value); matched {
			return true
		}
	}
	return false
}

func matchesWorkspaceConstraint(workspace string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if pattern == aclstore.WorkspaceScopeSubjectInherited {
			return true
		}
	}
	return matchesConstraintValue(workspace, patterns)
}

func computeRenewedExpiry(opts aclstore.RenewAuthorityGrantOpts, renewableUntil, now time.Time) time.Time {
	if opts.ExtendSeconds > 0 {
		expiresAt := now.Add(time.Duration(opts.ExtendSeconds) * time.Second)
		if !renewableUntil.IsZero() && expiresAt.After(renewableUntil) {
			expiresAt = renewableUntil
		}
		return expiresAt
	}
	return opts.ExpiresAt
}

// =========================================================================
// Internal: helpers
// =========================================================================

func authorityPrincipalRef(identity models.Identity) (string, string, error) {
	ref := identity.PrincipalRef()
	if ref.IsZero() {
		return "", "", fmt.Errorf("principal reference is incomplete")
	}
	return aclstore.PrincipalTypeForModel(ref.Type), ref.ID, nil
}

func isValidAuthorityAudienceType(audienceType string) bool {
	switch audienceType {
	case aclstore.AuthorityAudienceSession, aclstore.AuthorityAudienceTask,
		aclstore.AuthorityAudienceAgent, aclstore.AuthorityAudienceService:
		return true
	default:
		return false
	}
}

func defaultStringSlice(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func defaultResourceScope(values map[string][]string) map[string][]string {
	if values == nil {
		return map[string][]string{}
	}
	return values
}

func defaultMetadata(values map[string]interface{}) map[string]interface{} {
	if values == nil {
		return map[string]interface{}{}
	}
	return values
}

func ensurePrincipalMatch(label, gotType, gotID, wantType, wantID string) error {
	if gotType != wantType || gotID != wantID {
		return fmt.Errorf("%s must remain %s:%s", label, wantType, wantID)
	}
	return nil
}

func stringSliceSubset(child, parent []string) bool {
	if len(parent) == 0 {
		return true
	}
	if len(child) == 0 {
		return false
	}
	allowed := make(map[string]struct{}, len(parent))
	for _, v := range parent {
		allowed[v] = struct{}{}
	}
	for _, v := range child {
		if _, ok := allowed[v]; !ok {
			return false
		}
	}
	return true
}

func resourceScopeSubset(child, parent map[string][]string) bool {
	if len(parent) == 0 {
		return true
	}
	if len(child) == 0 {
		return false
	}
	for rt, childValues := range child {
		parentValues, ok := parent[rt]
		if !ok {
			return false
		}
		if !stringSliceSubset(childValues, parentValues) {
			return false
		}
	}
	return true
}

// =========================================================================
// Internal: time helpers — inline ISO-8601 parsing (no driver coercion)
// =========================================================================

// timeFormats are the candidate formats for parsing SQLite TEXT timestamps.
// The first is the canonical strftime format used by the migration DDL;
// additional formats handle edge cases from legacy data or CURRENT_TIMESTAMP.
var timeFormats = []string{
	"2006-01-02T15:04:05.000Z",       // canonical: strftime('%Y-%m-%dT%H:%M:%fZ','now')
	time.RFC3339Nano,                 // Go's default high-precision format
	time.RFC3339,                     // common interchange format
	"2006-01-02 15:04:05",            // SQLite CURRENT_TIMESTAMP
	"2006-01-02T15:04:05Z",           // ISO-8601 without fractional seconds
	"2006-01-02T15:04:05.999999999Z", // nanosecond precision
}

// formatTime produces the canonical ISO-8601 timestamp used for all INSERT/UPDATE.
func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// parseTime attempts each candidate format in order.
func parseTime(s string) time.Time {
	for _, layout := range timeFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseNullableTime parses a nullable TEXT timestamp. Returns nil for NULL
// or empty strings.
func parseNullableTime(s sql.NullString) *time.Time {
	if !s.Valid || s.String == "" {
		return nil
	}
	t := parseTime(s.String)
	if t.IsZero() {
		return nil
	}
	return &t
}

// =========================================================================
// Internal: row scanners — inline timestamp parsing
// =========================================================================

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...interface{}) error
}

func scanACLRule(s scanner) (*aclstore.Rule, error) {
	var rule aclstore.Rule
	var grantedAtStr string
	var expiresAtStr sql.NullString

	err := s.Scan(
		&rule.RuleID,
		&rule.PrincipalType,
		&rule.PrincipalID,
		&rule.ResourceType,
		&rule.ResourceID,
		&rule.AccessLevel,
		&rule.GrantedBy,
		&grantedAtStr,
		&expiresAtStr,
		&rule.Reason,
	)
	if err != nil {
		return nil, err
	}

	rule.GrantedAt = parseTime(grantedAtStr)
	rule.ExpiresAt = parseNullableTime(expiresAtStr)
	return &rule, nil
}

func scanAuthorityGrant(s scanner) (*aclstore.AuthorityGrant, error) {
	var grant aclstore.AuthorityGrant
	var parentGrantID sql.NullString
	var renewedAtStr sql.NullString
	var revokedAtStr sql.NullString
	var expiresAtStr string
	var renewableUntilStr string
	var createdAtStr string
	var workspaceJSON string
	var resourceJSON string
	var operationJSON string
	var metadataJSON string

	err := s.Scan(
		&grant.GrantID,
		&grant.RootGrantID,
		&grant.SubjectType,
		&grant.SubjectID,
		&grant.DelegateType,
		&grant.DelegateID,
		&grant.IssuedByType,
		&grant.IssuedByID,
		&grant.RootSubjectType,
		&grant.RootSubjectID,
		&parentGrantID,
		&grant.MayDelegate,
		&grant.RemainingHops,
		&workspaceJSON,
		&resourceJSON,
		&operationJSON,
		&grant.MaxAccessLevel,
		&grant.AudienceType,
		&grant.AudienceID,
		&grant.ValidWhileAudienceActive,
		&expiresAtStr,
		&renewableUntilStr,
		&renewedAtStr,
		&grant.Revoked,
		&revokedAtStr,
		&grant.Reason,
		&metadataJSON,
		&createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	if parentGrantID.Valid {
		grant.ParentGrantID = &parentGrantID.String
	}
	grant.ExpiresAt = parseTime(expiresAtStr)
	grant.RenewableUntil = parseTime(renewableUntilStr)
	grant.RenewedAt = parseNullableTime(renewedAtStr)
	grant.RevokedAt = parseNullableTime(revokedAtStr)
	grant.CreatedAt = parseTime(createdAtStr)

	grant.WorkspaceScope = []string{}
	if workspaceJSON != "" {
		if err := json.Unmarshal([]byte(workspaceJSON), &grant.WorkspaceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workspace scope: %w", err)
		}
	}

	grant.ResourceScope = map[string][]string{}
	if resourceJSON != "" {
		if err := json.Unmarshal([]byte(resourceJSON), &grant.ResourceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal resource scope: %w", err)
		}
	}

	grant.OperationScope = []string{}
	if operationJSON != "" {
		if err := json.Unmarshal([]byte(operationJSON), &grant.OperationScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal operation scope: %w", err)
		}
	}

	grant.Metadata = map[string]interface{}{}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &grant.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &grant, nil
}

func scanAuditLogEntry(s scanner) (*aclstore.AuditLogEntry, error) {
	var entry aclstore.AuditLogEntry
	var timestampStr string
	var metadataJSON string
	var ruleID sql.NullString
	var rootGrantID sql.NullString
	var authorityGrantID sql.NullString
	var parentGrantID sql.NullString
	var sessionIDStr sql.NullString

	err := s.Scan(
		&entry.AuditID,
		&timestampStr,
		&entry.Decision,
		&entry.AccessLevel,
		&entry.PrincipalType,
		&entry.PrincipalID,
		&entry.SubjectType,
		&entry.SubjectID,
		&entry.RootSubjectType,
		&entry.RootSubjectID,
		&entry.AuthorityMode,
		&rootGrantID,
		&authorityGrantID,
		&parentGrantID,
		&entry.ResourceType,
		&entry.ResourceID,
		&entry.Operation,
		&entry.Workspace,
		&ruleID,
		&entry.FallbackApplied,
		&entry.GatewayID,
		&sessionIDStr,
		&metadataJSON,
	)
	if err != nil {
		return nil, err
	}

	entry.Timestamp = parseTime(timestampStr)
	if ruleID.Valid {
		entry.RuleID = &ruleID.String
	}
	if rootGrantID.Valid {
		entry.RootGrantID = &rootGrantID.String
	}
	if authorityGrantID.Valid {
		entry.AuthorityGrantID = &authorityGrantID.String
	}
	if parentGrantID.Valid {
		entry.ParentGrantID = &parentGrantID.String
	}
	if sessionIDStr.Valid && sessionIDStr.String != "" {
		if parsed, err := uuid.Parse(sessionIDStr.String); err == nil {
			entry.SessionID = parsed
		}
	}

	entry.Metadata = make(map[string]interface{})
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &entry.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &entry, nil
}

// =========================================================================
// Internal: Casbin adapter (native SQLite SQL)
// =========================================================================

// sqliteAdapter implements the Casbin persist.Adapter interface by reading
// from acl_rules using native SQLite SQL. Write methods are no-ops because
// the Store manages database writes and then updates the in-memory model.
type sqliteAdapter struct {
	db *sql.DB
}

var _ persist.Adapter = (*sqliteAdapter)(nil)

func (a *sqliteAdapter) LoadPolicy(m model.Model) error {
	query := `
		SELECT principal_type, principal_id, resource_type, resource_id,
		       access_level, expires_at, rule_id
		FROM acl_rules
		WHERE expires_at IS NULL OR expires_at > ?
	`

	rows, err := a.db.Query(query, formatTime(time.Now()))
	if err != nil {
		return fmt.Errorf("failed to load ACL policies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var principalType, principalID, resourceType, resourceID string
		var accessLevel int
		var expiresAtStr sql.NullString
		var ruleID string

		if err := rows.Scan(&principalType, &principalID, &resourceType, &resourceID,
			&accessLevel, &expiresAtStr, &ruleID); err != nil {
			return fmt.Errorf("failed to scan ACL rule: %w", err)
		}

		resourceType, resourceID, _ = acl.RewriteLegacyPermission(resourceType, resourceID)

		sub := principalType + ":" + principalID
		obj := resourceType + ":" + resourceID
		act := strconv.Itoa(accessLevel)

		exp := ""
		if expiresAtStr.Valid && expiresAtStr.String != "" {
			t := parseTime(expiresAtStr.String)
			if !t.IsZero() {
				exp = t.Format(time.RFC3339)
			}
		}

		_ = persist.LoadPolicyArray([]string{"p", sub, obj, act, exp, ruleID}, m)
	}

	return rows.Err()
}

func (a *sqliteAdapter) SavePolicy(m model.Model) error                            { return nil }
func (a *sqliteAdapter) AddPolicy(string, string, []string) error                  { return nil }
func (a *sqliteAdapter) RemovePolicy(string, string, []string) error               { return nil }
func (a *sqliteAdapter) RemoveFilteredPolicy(string, string, int, ...string) error { return nil }

// =========================================================================
// Internal: migration runner
// =========================================================================

func applyMigrations(ctx context.Context, db *sql.DB, fs embed.FS, label string) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations (%s): %w", label, err)
	}

	entries, err := fs.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read embed fs (%s): %w", label, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version,
		).Scan(&count); err != nil {
			return fmt.Errorf("check migration %s (%s): %w", version, label, err)
		}
		if count > 0 {
			continue
		}
		content, err := fs.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read %s (%s): %w", entry.Name(), label, err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("exec %s (%s): %w", entry.Name(), label, err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("record %s (%s): %w", version, label, err)
		}
	}
	return nil
}

// ensureAuditView creates the acl_audit_log view over comprehensive_audit_log
// in the audit DB. This is the SQLite equivalent of postgres migration
// 018_drop_delegation_chains.sql's view definition. Best-effort: if the
// comprehensive_audit_log table doesn't exist (e.g. auditDB == aclDB and
// audit domain hasn't been set up), the CREATE VIEW will fail harmlessly.
func ensureAuditView(ctx context.Context, auditDB *sql.DB) error {
	_, err := auditDB.ExecContext(ctx, `
		CREATE VIEW IF NOT EXISTS acl_audit_log AS
		SELECT audit_id,
		       timestamp,
		       COALESCE(json_extract(metadata, '$.decision'), CASE WHEN success THEN 'ALLOW' ELSE 'DENY' END) AS decision,
		       CAST(json_extract(metadata, '$.access_level') AS INTEGER) AS access_level,
		       actor_type AS principal_type,
		       actor_id AS principal_id,
		       subject_type,
		       subject_id,
		       root_subject_type,
		       root_subject_id,
		       authority_mode,
		       root_authority_grant_id,
		       authority_grant_id,
		       parent_authority_grant_id,
		       resource_type,
		       resource_id,
		       operation,
		       workspace,
		       json_extract(metadata, '$.rule_id') AS rule_id,
		       COALESCE(CAST(json_extract(metadata, '$.fallback_applied') AS INTEGER), 0) AS fallback_applied,
		       gateway_id,
		       session_id,
		       metadata
		FROM comprehensive_audit_log
		WHERE event_type = 'authorization'
	`)
	return err
}
