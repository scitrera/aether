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

// CreateAuthorityRequest persists a new authority-request row in PENDING
// state. The caller is expected to supply a non-empty RoutingTarget; a
// request that nobody can resolve is a bug, not a valid state.
//
// Defaulting:
//   - TargetSubject is zero-valued when the requester is asking for
//     elevation in their own name (the common case).
//   - DurationSeconds <= 0 is rejected; the ExpiresAt timestamp is server-
//     computed and persisted alongside the row.
//   - Metadata is normalized to an empty map when nil so the JSON column
//     never contains SQL NULL.
//
// Stage A: this method writes the row but does not emit audit events or
// notify approvers. Stage B layers the lifecycle service on top.
func (s *Service) CreateAuthorityRequest(ctx context.Context, req *AuthorityRequest) error {
	if req == nil {
		return fmt.Errorf("%w: request is nil", ErrAuthorityRequestInvalid)
	}
	if req.RoutingTarget.IsEmpty() {
		return fmt.Errorf("%w: routing target is required", ErrAuthorityRequestInvalid)
	}
	if req.DurationSeconds <= 0 {
		return fmt.Errorf("%w: duration_seconds must be > 0", ErrAuthorityRequestInvalid)
	}
	if req.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at is required", ErrAuthorityRequestInvalid)
	}

	requestingType, requestingID, err := authorityPrincipalRef(req.RequestingActor)
	if err != nil {
		return fmt.Errorf("%w: invalid requesting_actor: %v", ErrAuthorityRequestInvalid, err)
	}

	// TargetSubject is optional; zero-valued means "same as requesting_actor".
	var targetType, targetID sql.NullString
	if subjectRef := req.TargetSubject.PrincipalRef(); !subjectRef.IsZero() {
		targetType = sql.NullString{String: PrincipalTypeForModel(subjectRef.Type), Valid: true}
		targetID = sql.NullString{String: subjectRef.ID, Valid: true}
	}

	// Routing target: exactly one populated.
	var routingPrincipalType, routingPrincipalID, routingCapability sql.NullString
	if req.RoutingTarget.Principal != nil {
		if ref := req.RoutingTarget.Principal.PrincipalRef(); !ref.IsZero() {
			routingPrincipalType = sql.NullString{String: PrincipalTypeForModel(ref.Type), Valid: true}
			routingPrincipalID = sql.NullString{String: ref.ID, Valid: true}
		}
	}
	if req.RoutingTarget.Capability != "" {
		routingCapability = sql.NullString{String: req.RoutingTarget.Capability, Valid: true}
	}

	workspaceJSON, err := json.Marshal(defaultStringSlice(req.WorkspaceScope))
	if err != nil {
		return fmt.Errorf("failed to marshal workspace scope: %w", err)
	}
	resourceJSON, err := json.Marshal(defaultResourceScope(req.ResourceScope))
	if err != nil {
		return fmt.Errorf("failed to marshal resource scope: %w", err)
	}
	operationJSON, err := json.Marshal(defaultStringSlice(req.OperationScope))
	if err != nil {
		return fmt.Errorf("failed to marshal operation scope: %w", err)
	}
	metadataJSON, err := json.Marshal(defaultMetadata(req.Metadata))
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if req.RequestID == "" {
		req.RequestID = uuid.New().String()
	}
	if req.Status == "" {
		req.Status = AuthorityRequestStatusPending
	}
	if req.CreatedAt.IsZero() {
		req.CreatedAt = time.Now()
	}

	var taskID sql.NullString
	if strings.TrimSpace(req.TaskID) != "" {
		taskID = sql.NullString{String: req.TaskID, Valid: true}
	}

	var audienceType, audienceID sql.NullString
	if req.AudienceType != "" {
		audienceType = sql.NullString{String: req.AudienceType, Valid: true}
	}
	if req.AudienceID != "" {
		audienceID = sql.NullString{String: req.AudienceID, Valid: true}
	}

	query := `
		INSERT INTO acl_authority_requests (
			request_id, status,
			requesting_actor_type, requesting_actor_id,
			target_subject_type, target_subject_id,
			workspace_scope, resource_scope, operation_scope,
			requested_access, duration_seconds,
			audience_type, audience_id,
			routing_principal_type, routing_principal_id, routing_capability,
			reason, task_id, metadata,
			created_at, expires_at
		) VALUES (
			$1, $2,
			$3, $4,
			$5, $6,
			$7, $8, $9,
			$10, $11,
			$12, $13,
			$14, $15, $16,
			$17, $18, $19,
			$20, $21
		)
	`

	if _, err := s.db.ExecContext(ctx, query,
		req.RequestID, string(req.Status),
		requestingType, requestingID,
		targetType, targetID,
		workspaceJSON, resourceJSON, operationJSON,
		req.RequestedAccess, req.DurationSeconds,
		audienceType, audienceID,
		routingPrincipalType, routingPrincipalID, routingCapability,
		nullableString(req.Reason), taskID, metadataJSON,
		req.CreatedAt, req.ExpiresAt,
	); err != nil {
		return fmt.Errorf("failed to create authority request: %w", err)
	}

	return nil
}

// GetAuthorityRequest fetches a single row by request_id. Returns
// ErrAuthorityRequestNotFound when no row matches.
func (s *Service) GetAuthorityRequest(ctx context.Context, requestID string) (*AuthorityRequest, error) {
	query := authorityRequestSelectColumns() + " FROM acl_authority_requests WHERE request_id = $1"

	req, err := scanAuthorityRequest(s.db.QueryRowContext(ctx, query, requestID))
	if err == sql.ErrNoRows {
		return nil, ErrAuthorityRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get authority request: %w", err)
	}
	return req, nil
}

// ListAuthorityRequests returns rows matching the filter, ordered by
// created_at DESC. Resolver-targeting (ResolverPrincipal /
// ResolverCapabilities) is ORed: a request matches when either the
// principal addressing or the capability gate addresses the caller.
func (s *Service) ListAuthorityRequests(ctx context.Context, filter AuthorityRequestFilter) ([]*AuthorityRequest, error) {
	query := authorityRequestSelectColumns() + " FROM acl_authority_requests WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if filter.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, string(filter.Status))
		argIdx++
	}
	// Workspace filter scans the workspace_scope JSON array for the value.
	if filter.Workspace != "" {
		query += fmt.Sprintf(" AND workspace_scope @> to_jsonb($%d::text)", argIdx)
		args = append(args, filter.Workspace)
		argIdx++
	}

	// Resolver targeting: principal AND/OR capability gates, joined with OR.
	resolverClauses := []string{}
	if filter.ResolverPrincipal != nil {
		ref := filter.ResolverPrincipal.PrincipalRef()
		if !ref.IsZero() {
			resolverClauses = append(resolverClauses, fmt.Sprintf(
				"(routing_principal_type = $%d AND routing_principal_id = $%d)",
				argIdx, argIdx+1,
			))
			args = append(args, PrincipalTypeForModel(ref.Type), ref.ID)
			argIdx += 2
		}
	}
	if len(filter.ResolverCapabilities) > 0 {
		placeholders := make([]string, 0, len(filter.ResolverCapabilities))
		for _, cap := range filter.ResolverCapabilities {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, cap)
			argIdx++
		}
		resolverClauses = append(resolverClauses,
			fmt.Sprintf("routing_capability IN (%s)", strings.Join(placeholders, ",")))
	}
	if len(resolverClauses) > 0 {
		query += " AND (" + strings.Join(resolverClauses, " OR ") + ")"
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
		return nil, fmt.Errorf("failed to list authority requests: %w", err)
	}
	defer rows.Close()

	out := make([]*AuthorityRequest, 0)
	for rows.Next() {
		req, err := scanAuthorityRequest(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan authority request: %w", err)
		}
		out = append(out, req)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating authority requests: %w", err)
	}
	return out, nil
}

// ResolveAuthorityRequest atomically flips a PENDING row to a terminal
// state (APPROVED, DENIED, or CANCELLED). Returns
// ErrAuthorityRequestAlreadyResolved when the row is already terminal and
// ErrAuthorityRequestNotFound when no row matches.
//
// Stage A: callers supply the freshly minted grant ID (when status is
// approved) -- the lifecycle service in Stage B will sequence the
// CreateAuthorityGrant call and pass the result here.
func (s *Service) ResolveAuthorityRequest(ctx context.Context, requestID string, status AuthorityRequestStatus, resolvedBy models.Identity, resolutionReason string, grantedGrantID string, resolvedAt time.Time) error {
	if status == AuthorityRequestStatusPending || status == "" {
		return fmt.Errorf("%w: target status must be terminal", ErrAuthorityRequestInvalid)
	}
	if status == AuthorityRequestStatusApproved && strings.TrimSpace(grantedGrantID) == "" {
		return fmt.Errorf("%w: approved resolution requires granted_grant_id", ErrAuthorityRequestInvalid)
	}

	var resolvedByType, resolvedByID sql.NullString
	if ref := resolvedBy.PrincipalRef(); !ref.IsZero() {
		resolvedByType = sql.NullString{String: PrincipalTypeForModel(ref.Type), Valid: true}
		resolvedByID = sql.NullString{String: ref.ID, Valid: true}
	}

	var grantedID sql.NullString
	if strings.TrimSpace(grantedGrantID) != "" {
		grantedID = sql.NullString{String: grantedGrantID, Valid: true}
	}

	if resolvedAt.IsZero() {
		resolvedAt = time.Now()
	}

	// Idempotency guard: only flip from PENDING. Distinguish "no row" from
	// "already resolved" by re-reading on RowsAffected == 0.
	query := `
		UPDATE acl_authority_requests
		SET status = $2,
		    resolved_at = $3,
		    granted_grant_id = $4,
		    resolved_by_type = $5,
		    resolved_by_id = $6,
		    resolution_reason = $7
		WHERE request_id = $1
		  AND status = 'pending'
	`
	result, err := s.db.ExecContext(ctx, query,
		requestID, string(status), resolvedAt, grantedID,
		resolvedByType, resolvedByID, nullableString(resolutionReason),
	)
	if err != nil {
		return fmt.Errorf("failed to resolve authority request: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		// Re-read to distinguish not-found from already-resolved.
		existing, getErr := s.GetAuthorityRequest(ctx, requestID)
		if getErr != nil {
			return getErr
		}
		if existing.Status.IsTerminal() {
			return ErrAuthorityRequestAlreadyResolved
		}
		// Should be unreachable -- if it's not pending and not terminal,
		// the row's status is corrupt; surface that loudly.
		return fmt.Errorf("authority request %s in unexpected state %q", requestID, existing.Status)
	}
	return nil
}

// CancelAuthorityRequest withdraws a pending request. Convenience wrapper
// around ResolveAuthorityRequest with status=cancelled and an empty grant.
func (s *Service) CancelAuthorityRequest(ctx context.Context, requestID string, reason string) error {
	return s.ResolveAuthorityRequest(ctx, requestID,
		AuthorityRequestStatusCancelled,
		models.Identity{}, // resolved_by recorded by lifecycle layer
		reason, "", time.Now())
}

// ExpireAuthorityRequests atomically flips PENDING rows whose expires_at
// is at or before `before` to EXPIRED, returning the affected rows so the
// caller can emit events and wake any tasks waiting on them. `limit`
// bounds the scan to keep batches predictable; pass 0 for "no limit"
// (small deployments) or a reasonable value (e.g. 256) in production.
//
// The two-step (collect then update) shape mirrors the cascade-revoke
// pattern used by acl_authority_grants and keeps the operation atomic
// inside a single transaction: rows returned to the caller have already
// been flipped, so the caller does not need to filter for terminal status.
func (s *Service) ExpireAuthorityRequests(ctx context.Context, before time.Time, limit int) ([]*AuthorityRequest, error) {
	if before.IsZero() {
		return nil, fmt.Errorf("%w: before must be non-zero", ErrAuthorityRequestInvalid)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin expire tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Collect candidate IDs. ORDER BY expires_at ASC so the oldest rows
	// drain first; bounded by `limit` (0 = unlimited).
	collectQuery := `
		SELECT request_id FROM acl_authority_requests
		WHERE status = 'pending' AND expires_at <= $1
		ORDER BY expires_at ASC
	`
	args := []interface{}{before}
	if limit > 0 {
		collectQuery += " LIMIT $2"
		args = append(args, limit)
	}
	rows, err := tx.QueryContext(ctx, collectQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to collect expired authority requests: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan expired request id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating expired requests: %w", err)
	}
	if len(ids) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit expire tx: %w", err)
		}
		return nil, nil
	}

	now := time.Now()
	// Build placeholders for the UPDATE ... IN (...) form.
	placeholders := make([]string, len(ids))
	updateArgs := make([]interface{}, 0, len(ids)+1)
	updateArgs = append(updateArgs, now)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		updateArgs = append(updateArgs, id)
	}
	updateQuery := fmt.Sprintf(`
		UPDATE acl_authority_requests
		SET status = 'expired', resolved_at = $1
		WHERE request_id IN (%s) AND status = 'pending'
	`, strings.Join(placeholders, ","))
	if _, err := tx.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
		return nil, fmt.Errorf("failed to expire authority requests: %w", err)
	}

	// Read back the affected rows. Re-number placeholders to $1..$N so the
	// SELECT does not collide with the UPDATE's $1 (now timestamp).
	readBackArgs := make([]interface{}, len(ids))
	rbPlaceholders := make([]string, len(ids))
	for i, id := range ids {
		readBackArgs[i] = id
		rbPlaceholders[i] = fmt.Sprintf("$%d", i+1)
	}
	readBackQuery := authorityRequestSelectColumns() +
		" FROM acl_authority_requests WHERE request_id IN (" +
		strings.Join(rbPlaceholders, ",") + ") ORDER BY expires_at ASC"

	rbRows, err := tx.QueryContext(ctx, readBackQuery, readBackArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to read back expired requests: %w", err)
	}
	out := make([]*AuthorityRequest, 0, len(ids))
	for rbRows.Next() {
		req, err := scanAuthorityRequest(rbRows)
		if err != nil {
			rbRows.Close()
			return nil, fmt.Errorf("failed to scan expired request: %w", err)
		}
		out = append(out, req)
	}
	rbRows.Close()
	if err := rbRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating expired requests: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit expire tx: %w", err)
	}
	return out, nil
}

// =========================================================================
// Internal helpers
// =========================================================================

// authorityRequestSelectColumns is the SELECT column list shared by Get /
// List / ExpireAuthorityRequests so they all scan into the same shape.
func authorityRequestSelectColumns() string {
	return `SELECT request_id, status,
		requesting_actor_type, requesting_actor_id,
		target_subject_type, target_subject_id,
		workspace_scope, resource_scope, operation_scope,
		requested_access, duration_seconds,
		audience_type, audience_id,
		routing_principal_type, routing_principal_id, routing_capability,
		reason, task_id, metadata,
		created_at, expires_at, resolved_at,
		granted_grant_id, resolved_by_type, resolved_by_id, resolution_reason`
}

func scanAuthorityRequest(s scanner) (*AuthorityRequest, error) {
	var (
		req                AuthorityRequest
		statusStr          string
		targetSubjectType  sql.NullString
		targetSubjectID    sql.NullString
		workspaceJSON      []byte
		resourceJSON       []byte
		operationJSON      []byte
		metadataJSON       []byte
		audienceType       sql.NullString
		audienceID         sql.NullString
		routingPrincType   sql.NullString
		routingPrincID     sql.NullString
		routingCapability  sql.NullString
		reasonStr          sql.NullString
		taskID             sql.NullString
		resolvedAt         sql.NullTime
		grantedGrantID     sql.NullString
		resolvedByType     sql.NullString
		resolvedByID       sql.NullString
		resolutionReason   sql.NullString
		requestingActorTyp string
		requestingActorID  string
	)

	err := s.Scan(
		&req.RequestID, &statusStr,
		&requestingActorTyp, &requestingActorID,
		&targetSubjectType, &targetSubjectID,
		&workspaceJSON, &resourceJSON, &operationJSON,
		&req.RequestedAccess, &req.DurationSeconds,
		&audienceType, &audienceID,
		&routingPrincType, &routingPrincID, &routingCapability,
		&reasonStr, &taskID, &metadataJSON,
		&req.CreatedAt, &req.ExpiresAt, &resolvedAt,
		&grantedGrantID, &resolvedByType, &resolvedByID, &resolutionReason,
	)
	if err != nil {
		return nil, err
	}

	req.Status = AuthorityRequestStatus(statusStr)
	req.RequestingActor = identityFromTypeID(requestingActorTyp, requestingActorID)
	if targetSubjectType.Valid && targetSubjectID.Valid {
		req.TargetSubject = identityFromTypeID(targetSubjectType.String, targetSubjectID.String)
	}
	if audienceType.Valid {
		req.AudienceType = audienceType.String
	}
	if audienceID.Valid {
		req.AudienceID = audienceID.String
	}
	if routingPrincType.Valid && routingPrincID.Valid {
		ident := identityFromTypeID(routingPrincType.String, routingPrincID.String)
		req.RoutingTarget.Principal = &ident
	}
	if routingCapability.Valid {
		req.RoutingTarget.Capability = routingCapability.String
	}
	if reasonStr.Valid {
		req.Reason = reasonStr.String
	}
	if taskID.Valid {
		req.TaskID = taskID.String
	}
	if resolvedAt.Valid {
		t := resolvedAt.Time
		req.ResolvedAt = &t
	}
	if grantedGrantID.Valid {
		req.GrantedGrantID = grantedGrantID.String
	}
	if resolvedByType.Valid && resolvedByID.Valid {
		req.ResolvedBy = identityFromTypeID(resolvedByType.String, resolvedByID.String)
	}
	if resolutionReason.Valid {
		req.ResolutionReason = resolutionReason.String
	}

	req.WorkspaceScope = []string{}
	if len(workspaceJSON) > 0 {
		if err := json.Unmarshal(workspaceJSON, &req.WorkspaceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workspace scope: %w", err)
		}
	}
	req.ResourceScope = map[string][]string{}
	if len(resourceJSON) > 0 {
		if err := json.Unmarshal(resourceJSON, &req.ResourceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal resource scope: %w", err)
		}
	}
	req.OperationScope = []string{}
	if len(operationJSON) > 0 {
		if err := json.Unmarshal(operationJSON, &req.OperationScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal operation scope: %w", err)
		}
	}
	req.Metadata = map[string]interface{}{}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &req.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &req, nil
}

// identityFromTypeID rebuilds a models.Identity from a (type, id) pair as
// persisted in the routing / target / resolved_by columns. The principal
// type in storage is the lowercase ACL form ("agent", "task", ...); the
// reverse mapping flips it back to the canonical models.PrincipalType
// constant so callers can use the rebuilt Identity in the rest of the
// stack without ad-hoc case normalization.
//
// Unknown principal-type strings round-trip as-is (cast to PrincipalType)
// so they remain debuggable rather than silently rewritten to a zero
// value.
func identityFromTypeID(principalType, principalID string) models.Identity {
	return models.Identity{
		Type: modelPrincipalTypeFromACL(principalType),
		ID:   principalID,
	}
}

// modelPrincipalTypeFromACL is the inverse of PrincipalTypeForModel: it
// translates a lowercase ACL principal-type string back to the canonical
// models.PrincipalType constant. Unknown values are returned cast as-is.
func modelPrincipalTypeFromACL(s string) models.PrincipalType {
	switch s {
	case PrincipalTypeUser:
		return models.PrincipalUser
	case PrincipalTypeAgent:
		return models.PrincipalAgent
	case PrincipalTypeTask:
		return models.PrincipalTask
	case PrincipalTypeWorkflowEngine:
		return models.PrincipalWorkflowEngine
	case PrincipalTypeMetricsBridge:
		return models.PrincipalMetricsBridge
	case PrincipalTypeOrchestrator:
		return models.PrincipalOrchestrator
	case PrincipalTypeBridge:
		return models.PrincipalBridge
	case PrincipalTypeService:
		return models.PrincipalService
	default:
		return models.PrincipalType(s)
	}
}

// nullableString returns sql.NullString{Valid:false} when s is empty so
// the inserted column carries SQL NULL rather than the empty string.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
