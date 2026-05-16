package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/acl"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	"github.com/scitrera/aether/pkg/models"
)

// CreateAuthorityRequest persists a new authority-request row in PENDING
// state into the SQLite-native acl_authority_requests table. Mirrors the
// validation rules in the legacy *acl.Service path.
func (s *Store) CreateAuthorityRequest(ctx context.Context, req *acl.AuthorityRequest) error {
	if req == nil {
		return fmt.Errorf("%w: request is nil", acl.ErrAuthorityRequestInvalid)
	}
	if req.RoutingTarget.IsEmpty() {
		return fmt.Errorf("%w: routing target is required", acl.ErrAuthorityRequestInvalid)
	}
	if req.DurationSeconds <= 0 {
		return fmt.Errorf("%w: duration_seconds must be > 0", acl.ErrAuthorityRequestInvalid)
	}
	if req.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at is required", acl.ErrAuthorityRequestInvalid)
	}

	requestingRef := req.RequestingActor.PrincipalRef()
	if requestingRef.IsZero() {
		return fmt.Errorf("%w: invalid requesting_actor", acl.ErrAuthorityRequestInvalid)
	}
	requestingActorType := aclstore.PrincipalTypeForModel(requestingRef.Type)
	requestingActorID := requestingRef.ID

	var targetSubjectType, targetSubjectID sql.NullString
	if subjectRef := req.TargetSubject.PrincipalRef(); !subjectRef.IsZero() {
		targetSubjectType = sql.NullString{String: aclstore.PrincipalTypeForModel(subjectRef.Type), Valid: true}
		targetSubjectID = sql.NullString{String: subjectRef.ID, Valid: true}
	}

	var routingPrincipalType, routingPrincipalID, routingCapability sql.NullString
	if req.RoutingTarget.Principal != nil {
		if ref := req.RoutingTarget.Principal.PrincipalRef(); !ref.IsZero() {
			routingPrincipalType = sql.NullString{String: aclstore.PrincipalTypeForModel(ref.Type), Valid: true}
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
		req.Status = acl.AuthorityRequestStatusPending
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
			?, ?,
			?, ?,
			?, ?,
			?, ?, ?,
			?, ?,
			?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?
		)
	`

	if _, err := s.db.ExecContext(ctx, query,
		req.RequestID, string(req.Status),
		requestingActorType, requestingActorID,
		targetSubjectType, targetSubjectID,
		string(workspaceJSON), string(resourceJSON), string(operationJSON),
		req.RequestedAccess, req.DurationSeconds,
		audienceType, audienceID,
		routingPrincipalType, routingPrincipalID, routingCapability,
		nullableString(req.Reason), taskID, string(metadataJSON),
		formatTime(req.CreatedAt), formatTime(req.ExpiresAt),
	); err != nil {
		return fmt.Errorf("failed to create authority request: %w", err)
	}

	return nil
}

// GetAuthorityRequest fetches a single row by request_id.
func (s *Store) GetAuthorityRequest(ctx context.Context, requestID string) (*acl.AuthorityRequest, error) {
	query := authorityRequestSelectColumns() + " FROM acl_authority_requests WHERE request_id = ?"

	req, err := scanAuthorityRequestSqlite(s.db.QueryRowContext(ctx, query, requestID))
	if err == sql.ErrNoRows {
		return nil, acl.ErrAuthorityRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get authority request: %w", err)
	}
	return req, nil
}

// ListAuthorityRequests returns rows matching the filter, ordered by
// created_at DESC.
func (s *Store) ListAuthorityRequests(ctx context.Context, filter acl.AuthorityRequestFilter) ([]*acl.AuthorityRequest, error) {
	query := authorityRequestSelectColumns() + " FROM acl_authority_requests WHERE 1=1"
	var args []interface{}

	if filter.Status != "" {
		query += " AND status = ?"
		args = append(args, string(filter.Status))
	}
	// SQLite workspace filter uses json_each — match any element of
	// workspace_scope against the supplied value. EXISTS short-circuits.
	if filter.Workspace != "" {
		query += ` AND EXISTS (
			SELECT 1 FROM json_each(workspace_scope) WHERE json_each.value = ?
		)`
		args = append(args, filter.Workspace)
	}

	resolverClauses := []string{}
	if filter.ResolverPrincipal != nil {
		ref := filter.ResolverPrincipal.PrincipalRef()
		if !ref.IsZero() {
			resolverClauses = append(resolverClauses,
				"(routing_principal_type = ? AND routing_principal_id = ?)")
			args = append(args, aclstore.PrincipalTypeForModel(ref.Type), ref.ID)
		}
	}
	if len(filter.ResolverCapabilities) > 0 {
		placeholders := make([]string, len(filter.ResolverCapabilities))
		for i, cap := range filter.ResolverCapabilities {
			placeholders[i] = "?"
			args = append(args, cap)
		}
		resolverClauses = append(resolverClauses,
			fmt.Sprintf("routing_capability IN (%s)", strings.Join(placeholders, ",")))
	}
	if len(resolverClauses) > 0 {
		query += " AND (" + strings.Join(resolverClauses, " OR ") + ")"
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
		return nil, fmt.Errorf("failed to list authority requests: %w", err)
	}
	defer rows.Close()

	out := make([]*acl.AuthorityRequest, 0)
	for rows.Next() {
		req, err := scanAuthorityRequestSqlite(rows)
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
// state. Returns ErrAuthorityRequestAlreadyResolved on idempotent re-call.
func (s *Store) ResolveAuthorityRequest(ctx context.Context, requestID string, status acl.AuthorityRequestStatus, resolvedBy models.Identity, resolutionReason string, grantedGrantID string, resolvedAt time.Time) error {
	if status == acl.AuthorityRequestStatusPending || status == "" {
		return fmt.Errorf("%w: target status must be terminal", acl.ErrAuthorityRequestInvalid)
	}
	if status == acl.AuthorityRequestStatusApproved && strings.TrimSpace(grantedGrantID) == "" {
		return fmt.Errorf("%w: approved resolution requires granted_grant_id", acl.ErrAuthorityRequestInvalid)
	}

	var resolvedByType, resolvedByID sql.NullString
	if ref := resolvedBy.PrincipalRef(); !ref.IsZero() {
		resolvedByType = sql.NullString{String: aclstore.PrincipalTypeForModel(ref.Type), Valid: true}
		resolvedByID = sql.NullString{String: ref.ID, Valid: true}
	}

	var grantedID sql.NullString
	if strings.TrimSpace(grantedGrantID) != "" {
		grantedID = sql.NullString{String: grantedGrantID, Valid: true}
	}

	if resolvedAt.IsZero() {
		resolvedAt = time.Now()
	}

	query := `
		UPDATE acl_authority_requests
		SET status = ?,
		    resolved_at = ?,
		    granted_grant_id = ?,
		    resolved_by_type = ?,
		    resolved_by_id = ?,
		    resolution_reason = ?
		WHERE request_id = ?
		  AND status = 'pending'
	`
	result, err := s.db.ExecContext(ctx, query,
		string(status), formatTime(resolvedAt), grantedID,
		resolvedByType, resolvedByID, nullableString(resolutionReason),
		requestID,
	)
	if err != nil {
		return fmt.Errorf("failed to resolve authority request: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		existing, getErr := s.GetAuthorityRequest(ctx, requestID)
		if getErr != nil {
			return getErr
		}
		if existing.Status.IsTerminal() {
			return acl.ErrAuthorityRequestAlreadyResolved
		}
		return fmt.Errorf("authority request %s in unexpected state %q", requestID, existing.Status)
	}
	return nil
}

// CancelAuthorityRequest withdraws a pending request. Convenience wrapper.
func (s *Store) CancelAuthorityRequest(ctx context.Context, requestID string, reason string) error {
	return s.ResolveAuthorityRequest(ctx, requestID,
		acl.AuthorityRequestStatusCancelled,
		models.Identity{},
		reason, "", time.Now())
}

// ExpireAuthorityRequests atomically marks PENDING rows past `before` as
// EXPIRED. Returns the affected rows. Bounded by `limit`; pass 0 for no
// limit.
func (s *Store) ExpireAuthorityRequests(ctx context.Context, before time.Time, limit int) ([]*acl.AuthorityRequest, error) {
	if before.IsZero() {
		return nil, fmt.Errorf("%w: before must be non-zero", acl.ErrAuthorityRequestInvalid)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin expire tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	collectQuery := `
		SELECT request_id FROM acl_authority_requests
		WHERE status = 'pending' AND expires_at <= ?
		ORDER BY expires_at ASC
	`
	args := []interface{}{formatTime(before)}
	if limit > 0 {
		collectQuery += " LIMIT ?"
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
	placeholders := make([]string, len(ids))
	updateArgs := make([]interface{}, 0, len(ids)+2)
	updateArgs = append(updateArgs, formatTime(now))
	for i, id := range ids {
		placeholders[i] = "?"
		updateArgs = append(updateArgs, id)
	}
	updateQuery := fmt.Sprintf(`
		UPDATE acl_authority_requests
		SET status = 'expired', resolved_at = ?
		WHERE request_id IN (%s) AND status = 'pending'
	`, strings.Join(placeholders, ","))
	if _, err := tx.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
		return nil, fmt.Errorf("failed to expire authority requests: %w", err)
	}

	readBackArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		readBackArgs[i] = id
	}
	readBackQuery := authorityRequestSelectColumns() +
		" FROM acl_authority_requests WHERE request_id IN (" +
		strings.Join(placeholders, ",") + ") ORDER BY expires_at ASC"

	rbRows, err := tx.QueryContext(ctx, readBackQuery, readBackArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to read back expired requests: %w", err)
	}
	out := make([]*acl.AuthorityRequest, 0, len(ids))
	for rbRows.Next() {
		req, err := scanAuthorityRequestSqlite(rbRows)
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

// authorityRequestSelectColumns is the SELECT column list used by Get /
// List / Expire so the row order matches scanAuthorityRequestSqlite.
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

func scanAuthorityRequestSqlite(s scanner) (*acl.AuthorityRequest, error) {
	var (
		req                acl.AuthorityRequest
		statusStr          string
		requestingActorTyp string
		requestingActorID  string
		targetSubjectType  sql.NullString
		targetSubjectID    sql.NullString
		workspaceJSON      string
		resourceJSON       string
		operationJSON      string
		metadataJSON       sql.NullString
		audienceType       sql.NullString
		audienceID         sql.NullString
		routingPrincType   sql.NullString
		routingPrincID     sql.NullString
		routingCapability  sql.NullString
		reasonStr          sql.NullString
		taskID             sql.NullString
		createdAtStr       string
		expiresAtStr       string
		resolvedAtStr      sql.NullString
		grantedGrantID     sql.NullString
		resolvedByType     sql.NullString
		resolvedByID       sql.NullString
		resolutionReason   sql.NullString
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
		&createdAtStr, &expiresAtStr, &resolvedAtStr,
		&grantedGrantID, &resolvedByType, &resolvedByID, &resolutionReason,
	)
	if err != nil {
		return nil, err
	}

	req.Status = acl.AuthorityRequestStatus(statusStr)
	req.RequestingActor = identityFromTypeIDSqlite(requestingActorTyp, requestingActorID)
	if targetSubjectType.Valid && targetSubjectID.Valid {
		req.TargetSubject = identityFromTypeIDSqlite(targetSubjectType.String, targetSubjectID.String)
	}
	if audienceType.Valid {
		req.AudienceType = audienceType.String
	}
	if audienceID.Valid {
		req.AudienceID = audienceID.String
	}
	if routingPrincType.Valid && routingPrincID.Valid {
		ident := identityFromTypeIDSqlite(routingPrincType.String, routingPrincID.String)
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
	req.CreatedAt = parseTime(createdAtStr)
	req.ExpiresAt = parseTime(expiresAtStr)
	req.ResolvedAt = parseNullableTime(resolvedAtStr)
	if grantedGrantID.Valid {
		req.GrantedGrantID = grantedGrantID.String
	}
	if resolvedByType.Valid && resolvedByID.Valid {
		req.ResolvedBy = identityFromTypeIDSqlite(resolvedByType.String, resolvedByID.String)
	}
	if resolutionReason.Valid {
		req.ResolutionReason = resolutionReason.String
	}

	req.WorkspaceScope = []string{}
	if workspaceJSON != "" {
		if err := json.Unmarshal([]byte(workspaceJSON), &req.WorkspaceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workspace scope: %w", err)
		}
	}
	req.ResourceScope = map[string][]string{}
	if resourceJSON != "" {
		if err := json.Unmarshal([]byte(resourceJSON), &req.ResourceScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal resource scope: %w", err)
		}
	}
	req.OperationScope = []string{}
	if operationJSON != "" {
		if err := json.Unmarshal([]byte(operationJSON), &req.OperationScope); err != nil {
			return nil, fmt.Errorf("failed to unmarshal operation scope: %w", err)
		}
	}
	req.Metadata = map[string]interface{}{}
	if metadataJSON.Valid && metadataJSON.String != "" {
		if err := json.Unmarshal([]byte(metadataJSON.String), &req.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &req, nil
}

func identityFromTypeIDSqlite(principalType, principalID string) models.Identity {
	return models.Identity{
		Type: modelPrincipalTypeFromACL(principalType),
		ID:   principalID,
	}
}

// modelPrincipalTypeFromACL inverts aclstore.PrincipalTypeForModel.
func modelPrincipalTypeFromACL(s string) models.PrincipalType {
	switch s {
	case aclstore.PrincipalTypeUser:
		return models.PrincipalUser
	case aclstore.PrincipalTypeAgent:
		return models.PrincipalAgent
	case aclstore.PrincipalTypeTask:
		return models.PrincipalTask
	case aclstore.PrincipalTypeWorkflowEngine:
		return models.PrincipalWorkflowEngine
	case aclstore.PrincipalTypeMetricsBridge:
		return models.PrincipalMetricsBridge
	case aclstore.PrincipalTypeOrchestrator:
		return models.PrincipalOrchestrator
	case aclstore.PrincipalTypeBridge:
		return models.PrincipalBridge
	case aclstore.PrincipalTypeService:
		return models.PrincipalService
	default:
		return models.PrincipalType(s)
	}
}

func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
