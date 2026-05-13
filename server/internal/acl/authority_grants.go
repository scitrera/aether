package acl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

const (
	AuthorityAudienceSession = "session"
	AuthorityAudienceTask    = "task"
	AuthorityAudienceAgent   = "agent"
	AuthorityAudienceService = "service"
)

// AuthorityGrant is the persisted delegated-authorization capability used by
// the on-behalf-of model.
type AuthorityGrant struct {
	GrantID         string
	RootGrantID     string
	SubjectType     string
	SubjectID       string
	DelegateType    string
	DelegateID      string
	IssuedByType    string
	IssuedByID      string
	RootSubjectType string
	RootSubjectID   string
	ParentGrantID   *string
	MayDelegate     bool
	RemainingHops   int

	WorkspaceScope []string
	ResourceScope  map[string][]string
	OperationScope []string

	MaxAccessLevel           int
	AudienceType             string
	AudienceID               string
	ValidWhileAudienceActive bool

	ExpiresAt      time.Time
	RenewableUntil time.Time
	RenewedAt      *time.Time

	Revoked   bool
	RevokedAt *time.Time

	Reason    string
	Metadata  map[string]interface{}
	CreatedAt time.Time
}

type CreateAuthorityGrantRequest struct {
	Subject     models.Identity
	Delegate    models.Identity
	IssuedBy    models.Identity
	RootSubject *models.Identity

	ParentGrantID *string
	MayDelegate   bool
	RemainingHops int

	WorkspaceScope []string
	ResourceScope  map[string][]string
	OperationScope []string

	MaxAccessLevel           int
	AudienceType             string
	AudienceID               string
	ValidWhileAudienceActive bool

	ExpiresAt      time.Time
	RenewableUntil time.Time

	Reason   string
	Metadata map[string]interface{}
}

type AuthorityGrantFilter struct {
	RootGrantID  string
	SubjectType  string
	SubjectID    string
	DelegateType string
	DelegateID   string
	AudienceType string
	AudienceID   string

	IncludeRevoked bool
	ActiveOnly     bool
	Limit          int
	Offset         int
}

// VisibleGrantsFilter scopes a runtime list query to the calling actor.
// At least one of ByDelegate / BySubject is set by the caller; the resulting
// SQL filters grants where the actor matches the requested role(s).
type VisibleGrantsFilter struct {
	Actor          models.Identity
	ByDelegate     bool // include grants where actor is delegate
	BySubject      bool // include grants where actor is subject
	AudienceType   string
	AudienceID     string
	IncludeRevoked bool
	Limit          int
	Offset         int
}

func (s *Service) CreateAuthorityGrant(ctx context.Context, req CreateAuthorityGrantRequest) (*AuthorityGrant, error) {
	if err := ValidateAccessLevel(req.MaxAccessLevel); err != nil {
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
			return nil, ErrAuthorityGrantDelegationDenied
		}
		if err := ensurePrincipalMatch("subject", subjectType, subjectID, parent.SubjectType, parent.SubjectID); err != nil {
			return nil, err
		}
		if err := ensurePrincipalMatch("root subject", rootType, rootID, parent.RootSubjectType, parent.RootSubjectID); err != nil {
			return nil, err
		}
		if req.MaxAccessLevel > parent.MaxAccessLevel {
			return nil, ErrAuthorityGrantScopeEscalation
		}
		if req.ExpiresAt.After(parent.ExpiresAt) || req.RenewableUntil.After(parent.RenewableUntil) {
			return nil, ErrAuthorityGrantScopeEscalation
		}
		if !stringSliceSubset(req.WorkspaceScope, parent.WorkspaceScope) {
			return nil, ErrAuthorityGrantScopeEscalation
		}
		if !stringSliceSubset(req.OperationScope, parent.OperationScope) {
			return nil, ErrAuthorityGrantScopeEscalation
		}
		if !resourceScopeSubset(req.ResourceScope, parent.ResourceScope) {
			return nil, ErrAuthorityGrantScopeEscalation
		}
		if req.RemainingHops > parent.RemainingHops-1 {
			return nil, ErrAuthorityGrantDelegationDenied
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
	grant := &AuthorityGrant{
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
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12, $13, $14,
			$15, $16, $17, $18,
			$19, $20, $21, $22,
			$23, $24, $25
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
		workspaceJSON,
		resourceJSON,
		operationJSON,
		grant.MaxAccessLevel,
		grant.AudienceType,
		grant.AudienceID,
		grant.ValidWhileAudienceActive,
		grant.ExpiresAt,
		grant.RenewableUntil,
		grant.Reason,
		metadataJSON,
		grant.CreatedAt,
	); err != nil {
		return nil, fmt.Errorf("failed to create authority grant: %w", err)
	}

	return grant, nil
}

func (s *Service) GetAuthorityGrant(ctx context.Context, grantID string) (*AuthorityGrant, error) {
	query := `
		SELECT grant_id, root_grant_id, subject_type, subject_id, delegate_type, delegate_id,
		       issued_by_type, issued_by_id, root_subject_type, root_subject_id,
		       parent_grant_id, may_delegate, remaining_hops, workspace_scope,
		       resource_scope, operation_scope, max_access_level, audience_type,
		       audience_id, valid_while_audience_active, expires_at, renewable_until,
		       renewed_at, revoked, revoked_at, reason, metadata, created_at
		FROM acl_authority_grants
		WHERE grant_id = $1
	`

	grant, err := scanAuthorityGrant(s.db.QueryRowContext(ctx, query, grantID))
	if err == sql.ErrNoRows {
		return nil, ErrAuthorityGrantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get authority grant: %w", err)
	}

	return grant, nil
}

func (s *Service) ListAuthorityGrants(ctx context.Context, filter AuthorityGrantFilter) ([]*AuthorityGrant, error) {
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
	args := []interface{}{}
	argIdx := 1

	if filter.SubjectType != "" {
		query += fmt.Sprintf(" AND subject_type = $%d", argIdx)
		args = append(args, filter.SubjectType)
		argIdx++
	}
	if filter.RootGrantID != "" {
		query += fmt.Sprintf(" AND root_grant_id = $%d", argIdx)
		args = append(args, filter.RootGrantID)
		argIdx++
	}
	if filter.SubjectID != "" {
		query += fmt.Sprintf(" AND subject_id = $%d", argIdx)
		args = append(args, filter.SubjectID)
		argIdx++
	}
	if filter.DelegateType != "" {
		query += fmt.Sprintf(" AND delegate_type = $%d", argIdx)
		args = append(args, filter.DelegateType)
		argIdx++
	}
	if filter.DelegateID != "" {
		query += fmt.Sprintf(" AND delegate_id = $%d", argIdx)
		args = append(args, filter.DelegateID)
		argIdx++
	}
	if filter.AudienceType != "" {
		query += fmt.Sprintf(" AND audience_type = $%d", argIdx)
		args = append(args, filter.AudienceType)
		argIdx++
	}
	if filter.AudienceID != "" {
		query += fmt.Sprintf(" AND audience_id = $%d", argIdx)
		args = append(args, filter.AudienceID)
		argIdx++
	}
	if !filter.IncludeRevoked {
		query += " AND revoked = FALSE"
	}
	if filter.ActiveOnly {
		query += fmt.Sprintf(" AND revoked = FALSE AND expires_at > $%d", argIdx)
		args = append(args, time.Now())
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, filter.Limit)
		argIdx++
	}
	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list authority grants: %w", err)
	}
	defer rows.Close()

	grants := make([]*AuthorityGrant, 0)
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

// RenewAuthorityGrant extends a grant's expires_at to the supplied absolute
// time. Convenience wrapper around RenewAuthorityGrantOpts; see that helper
// for clamping semantics (RenewableUntil and parent grant ExpiresAt).
func (s *Service) RenewAuthorityGrant(ctx context.Context, grantID string, expiresAt time.Time) (*AuthorityGrant, error) {
	return s.RenewAuthorityGrantOpts(ctx, grantID, RenewAuthorityGrantOpts{ExpiresAt: expiresAt})
}

// RenewAuthorityGrantOpts groups the renewal inputs so callers can express
// either an absolute target (ExpiresAt) or a relative extension
// (ExtendSeconds). When both are provided, ExtendSeconds wins because it is
// the more recent ergonomic addition (callers can compute server-side time
// without a second round-trip).
type RenewAuthorityGrantOpts struct {
	ExpiresAt     time.Time
	ExtendSeconds int
}

// RenewAuthorityGrantOpts extends a grant's lease, clamping the resulting
// ExpiresAt to the grant's RenewableUntil window and (for derived grants)
// the parent grant's ExpiresAt. This avoids the historical foot-gun where
// callers had to know server time to compute a sane absolute expiry.
func (s *Service) RenewAuthorityGrantOpts(ctx context.Context, grantID string, opts RenewAuthorityGrantOpts) (*AuthorityGrant, error) {
	grant, err := s.GetAuthorityGrant(ctx, grantID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	switch err := grant.ValidateActiveAt(now); err {
	case nil:
	case ErrAuthorityGrantRevoked, ErrAuthorityGrantExpired:
		return nil, err
	default:
		return nil, err
	}

	expiresAt := computeRenewedExpiry(opts, grant.RenewableUntil, now)

	switch {
	case expiresAt.IsZero():
		return nil, fmt.Errorf("renewed expires_at must be specified (set ExpiresAt or ExtendSeconds)")
	case expiresAt.After(grant.RenewableUntil):
		return nil, ErrAuthorityGrantRenewal
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
		// Clamp against parent's expiry when an ExtendSeconds bump would
		// push past it; absolute callers still get the strict error so they
		// know their value was rejected.
		if opts.ExtendSeconds > 0 && expiresAt.After(parent.ExpiresAt) {
			expiresAt = parent.ExpiresAt
		}
		if expiresAt.After(parent.ExpiresAt) || grant.RenewableUntil.After(parent.RenewableUntil) {
			return nil, ErrAuthorityGrantRenewal
		}
	}

	renewedAt := now
	query := `
		UPDATE acl_authority_grants
		SET expires_at = $2, renewed_at = $3
		WHERE grant_id = $1
	`
	result, err := s.db.ExecContext(ctx, query, grantID, expiresAt, renewedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to renew authority grant: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return nil, ErrAuthorityGrantNotFound
	}

	grant.ExpiresAt = expiresAt
	grant.RenewedAt = &renewedAt
	return grant, nil
}

// RevokedAuthorityGrant is a lightweight projection of an authority grant
// returned alongside RevokeAuthorityGrantCascade so callers (e.g., gateway
// push-event publishers) can address the affected delegates without a
// follow-up GetAuthorityGrant per row.
type RevokedAuthorityGrant struct {
	GrantID      string
	RootGrantID  string
	DelegateType string
	DelegateID   string
	IsRoot       bool // true if this is the explicitly revoked grant; false if cascade descendant
}

func (s *Service) RevokeAuthorityGrant(ctx context.Context, grantID string) error {
	_, err := s.RevokeAuthorityGrantCascade(ctx, grantID)
	return err
}

// RevokeAuthorityGrantCascade revokes the specified grant and all
// not-yet-revoked descendants in a single statement, returning the affected
// rows so callers can react (e.g., notify connected delegates). Returns
// ErrAuthorityGrantNotFound when the target grant doesn't exist or all
// descendants are already revoked.
func (s *Service) RevokeAuthorityGrantCascade(ctx context.Context, grantID string) ([]RevokedAuthorityGrant, error) {
	now := time.Now()
	query := `
		WITH RECURSIVE descendants AS (
			SELECT grant_id
			FROM acl_authority_grants
			WHERE grant_id = $1
			UNION ALL
			SELECT child.grant_id
			FROM acl_authority_grants child
			JOIN descendants d ON child.parent_grant_id = d.grant_id
		)
		UPDATE acl_authority_grants
		SET revoked = TRUE, revoked_at = $2
		WHERE grant_id IN (SELECT grant_id FROM descendants)
		  AND revoked = FALSE
		RETURNING grant_id, root_grant_id, delegate_type, delegate_id
	`

	rows, err := s.db.QueryContext(ctx, query, grantID, now)
	if err != nil {
		return nil, fmt.Errorf("failed to revoke authority grant: %w", err)
	}
	defer rows.Close()

	var revoked []RevokedAuthorityGrant
	for rows.Next() {
		var r RevokedAuthorityGrant
		if err := rows.Scan(&r.GrantID, &r.RootGrantID, &r.DelegateType, &r.DelegateID); err != nil {
			return nil, fmt.Errorf("failed to scan revoked grant: %w", err)
		}
		r.IsRoot = r.GrantID == grantID
		revoked = append(revoked, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating revoked grants: %w", err)
	}
	if len(revoked) == 0 {
		return nil, ErrAuthorityGrantNotFound
	}

	return revoked, nil
}

// ListVisibleGrants returns grants matching the supplied actor-scoped filter.
// At least one of ByDelegate or BySubject must be true; otherwise an empty
// slice is returned without hitting the DB. Uses the existing
// idx_authority_grants_delegate_active / _subject_active indexes.
func (s *Service) ListVisibleGrants(ctx context.Context, filter VisibleGrantsFilter) ([]*AuthorityGrant, error) {
	if !filter.ByDelegate && !filter.BySubject {
		return []*AuthorityGrant{}, nil
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
	args := []interface{}{}
	argIdx := 1

	switch {
	case filter.ByDelegate && filter.BySubject:
		query += fmt.Sprintf(" AND ((delegate_type = $%d AND delegate_id = $%d) OR (subject_type = $%d AND subject_id = $%d))",
			argIdx, argIdx+1, argIdx, argIdx+1)
		args = append(args, actorType, actorID)
		argIdx += 2
	case filter.ByDelegate:
		query += fmt.Sprintf(" AND delegate_type = $%d AND delegate_id = $%d", argIdx, argIdx+1)
		args = append(args, actorType, actorID)
		argIdx += 2
	case filter.BySubject:
		query += fmt.Sprintf(" AND subject_type = $%d AND subject_id = $%d", argIdx, argIdx+1)
		args = append(args, actorType, actorID)
		argIdx += 2
	}

	if filter.AudienceType != "" {
		query += fmt.Sprintf(" AND audience_type = $%d", argIdx)
		args = append(args, filter.AudienceType)
		argIdx++
	}
	if filter.AudienceID != "" {
		query += fmt.Sprintf(" AND audience_id = $%d", argIdx)
		args = append(args, filter.AudienceID)
		argIdx++
	}
	if !filter.IncludeRevoked {
		query += " AND revoked = FALSE"
	}

	query += " ORDER BY created_at DESC"

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit)
	argIdx++

	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list visible authority grants: %w", err)
	}
	defer rows.Close()

	grants := make([]*AuthorityGrant, 0)
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

// FindVisibleDerivedGrant returns an active grant that the actor (caller of
// derive) previously minted matching the (parent, target delegate, audience)
// tuple, enabling idempotent DERIVE_FOR_TARGET. Returns nil with no error
// when no matching grant exists. Revoked or expired grants are filtered out.
func (s *Service) FindVisibleDerivedGrant(ctx context.Context, parentGrantID string, target models.Identity, audienceType, audienceID string) (*AuthorityGrant, error) {
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
		WHERE parent_grant_id = $1
		  AND delegate_type = $2
		  AND delegate_id = $3
		  AND audience_type = $4
		  AND audience_id = $5
		  AND revoked = FALSE
		  AND expires_at > $6
		ORDER BY created_at DESC
		LIMIT 1
	`
	row := s.db.QueryRowContext(ctx, query, parentGrantID, delegateType, delegateID, audienceType, audienceID, time.Now())
	grant, err := scanAuthorityGrant(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find visible derived grant: %w", err)
	}
	return grant, nil
}

// computeRenewedExpiry resolves the desired post-renewal expiry from
// RenewAuthorityGrantOpts. ExtendSeconds wins when both fields are set
// (callers can compute server-side time without a round-trip), and the
// resulting timestamp is clamped against the grant's renewable_until
// window. Note: parent-grant clamping is applied separately by the caller
// because it requires an additional DB lookup.
func computeRenewedExpiry(opts RenewAuthorityGrantOpts, renewableUntil, now time.Time) time.Time {
	if opts.ExtendSeconds > 0 {
		expiresAt := now.Add(time.Duration(opts.ExtendSeconds) * time.Second)
		if !renewableUntil.IsZero() && expiresAt.After(renewableUntil) {
			expiresAt = renewableUntil
		}
		return expiresAt
	}
	return opts.ExpiresAt
}

func (g *AuthorityGrant) CanDelegate() bool {
	return g.MayDelegate && g.RemainingHops > 0
}

func (g *AuthorityGrant) ValidateActiveAt(now time.Time) error {
	switch {
	case g.Revoked:
		return ErrAuthorityGrantRevoked
	case now.After(g.ExpiresAt):
		return ErrAuthorityGrantExpired
	default:
		return nil
	}
}

func authorityPrincipalRef(identity models.Identity) (string, string, error) {
	ref := identity.PrincipalRef()
	if ref.IsZero() {
		return "", "", fmt.Errorf("principal reference is incomplete")
	}
	return PrincipalTypeForModel(ref.Type), ref.ID, nil
}

func isValidAuthorityAudienceType(audienceType string) bool {
	switch audienceType {
	case AuthorityAudienceSession, AuthorityAudienceTask, AuthorityAudienceAgent, AuthorityAudienceService:
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

func scanAuthorityGrant(s scanner) (*AuthorityGrant, error) {
	var grant AuthorityGrant
	var parentGrantID sql.NullString
	var renewedAt sql.NullTime
	var revokedAt sql.NullTime
	var workspaceJSON []byte
	var resourceJSON []byte
	var operationJSON []byte
	var metadataJSON []byte

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
		&grant.ExpiresAt,
		&grant.RenewableUntil,
		&renewedAt,
		&grant.Revoked,
		&revokedAt,
		&grant.Reason,
		&metadataJSON,
		&grant.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	if parentGrantID.Valid {
		grant.ParentGrantID = &parentGrantID.String
	}
	if renewedAt.Valid {
		grant.RenewedAt = &renewedAt.Time
	}
	if revokedAt.Valid {
		grant.RevokedAt = &revokedAt.Time
	}

	grant.WorkspaceScope = []string{}
	if len(workspaceJSON) > 0 {
		if err := json.Unmarshal(workspaceJSON, &grant.WorkspaceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workspace scope: %w", err)
		}
	}

	grant.ResourceScope = map[string][]string{}
	if len(resourceJSON) > 0 {
		if err := json.Unmarshal(resourceJSON, &grant.ResourceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal resource scope: %w", err)
		}
	}

	grant.OperationScope = []string{}
	if len(operationJSON) > 0 {
		if err := json.Unmarshal(operationJSON, &grant.OperationScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal operation scope: %w", err)
		}
	}

	grant.Metadata = map[string]interface{}{}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &grant.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &grant, nil
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
	for _, value := range parent {
		allowed[value] = struct{}{}
	}
	for _, value := range child {
		if _, ok := allowed[value]; !ok {
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
	for resourceType, childValues := range child {
		parentValues, ok := parent[resourceType]
		if !ok {
			return false
		}
		if !stringSliceSubset(childValues, parentValues) {
			return false
		}
	}
	return true
}
