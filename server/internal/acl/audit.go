package acl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/pkg/models"
)

const (
	aclChannelBuffer = 1000
	aclBatchSize     = 100
	aclFlushPeriod   = 5 * time.Second
)

// AuditLogger records ACL decisions to the audit log
// Uses buffered channel for async writes to avoid blocking ACL checks
type AuditLogger struct {
	base      *audit.BaseLogger[*AuditLogEntry]
	gatewayID string
}

// NewAuditLogger creates a new audit logger with buffered async writes
func NewAuditLogger(db *sql.DB, gatewayID string) *AuditLogger {
	logger := &AuditLogger{
		gatewayID: gatewayID,
	}

	logger.base = audit.NewBaseLogger[*AuditLogEntry](
		db,
		aclBatchSize,
		aclFlushPeriod,
		aclChannelBuffer,
		aclEntryBatchWriter,
	)

	return logger
}

// buildEntry constructs an AuditLogEntry from a decision and principal.
func (a *AuditLogger) buildEntry(decision *ACLDecision, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID) *AuditLogEntry {
	entry := &AuditLogEntry{
		Timestamp:       time.Now(),
		Decision:        decision.Decision,
		AccessLevel:     decision.EffectiveAccessLevel,
		PrincipalType:   PrincipalTypeForModel(principal.Type),
		PrincipalID:     principal.CanonicalPrincipalID(),
		SubjectType:     PrincipalTypeForModel(principal.Type),
		SubjectID:       principal.CanonicalPrincipalID(),
		RootSubjectType: PrincipalTypeForModel(principal.Type),
		RootSubjectID:   principal.CanonicalPrincipalID(),
		AuthorityMode:   audit.AuthorityModeDirect,
		ResourceType:    resourceType,
		ResourceID:      resourceID,
		Operation:       operation,
		Workspace:       workspace,
		FallbackApplied: decision.FallbackApplied,
		GatewayID:       a.gatewayID,
		SessionID:       sessionID,
		Metadata:        make(map[string]interface{}),
	}

	if decision.RuleApplied != nil {
		ruleID := decision.RuleApplied.RuleID
		entry.RuleID = &ruleID
	}

	if decision.AuthorityGrant != nil {
		entry.SubjectType = decision.AuthorityGrant.SubjectType
		entry.SubjectID = decision.AuthorityGrant.SubjectID
		entry.RootSubjectType = decision.AuthorityGrant.RootSubjectType
		entry.RootSubjectID = decision.AuthorityGrant.RootSubjectID
		entry.AuthorityMode = audit.AuthorityModeOnBehalfOf
		rootGrantID := decision.AuthorityGrant.RootGrantID
		if rootGrantID == "" {
			rootGrantID = decision.AuthorityGrant.GrantID
		}
		entry.RootGrantID = &rootGrantID
		grantID := decision.AuthorityGrant.GrantID
		entry.AuthorityGrantID = &grantID
		entry.ParentGrantID = decision.AuthorityGrant.ParentGrantID
	}

	return entry
}

// LogDecision records an ACL decision to the audit log (async)
func (a *AuditLogger) LogDecision(ctx context.Context, decision *ACLDecision, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID) {
	entry := a.buildEntry(decision, principal, resourceType, resourceID, operation, workspace, sessionID)
	// Non-blocking; drop if channel full (performance safety valve)
	a.base.Enqueue(entry)
}

// buildACLMetadata merges the ACL-specific fields into the metadata JSONB.
func buildACLMetadata(entry *AuditLogEntry) ([]byte, error) {
	merged := make(map[string]interface{})
	for k, v := range entry.Metadata {
		merged[k] = v
	}
	merged["decision"] = entry.Decision
	merged["access_level"] = entry.AccessLevel
	merged["fallback_applied"] = entry.FallbackApplied
	if entry.RuleID != nil {
		merged["rule_id"] = *entry.RuleID
	}
	return json.Marshal(merged)
}

// aclEntryBatchWriter writes a batch of AuditLogEntries in a single transaction.
// It is used as the BatchWriter for BaseLogger[*AuditLogEntry].
func aclEntryBatchWriter(ctx context.Context, db *sql.DB, entries []*AuditLogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO comprehensive_audit_log (
			timestamp, event_type, actor_type, actor_id, subject_type, subject_id,
			root_subject_type, root_subject_id, authority_mode, root_authority_grant_id,
			authority_grant_id, parent_authority_grant_id, resource_type, resource_id,
			operation, workspace, session_id, gateway_id, success, error_message, metadata, source
		) VALUES ($1, 'authorization', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		metadataJSON, err := buildACLMetadata(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		success := entry.Decision == "ALLOW"
		var errorMsg *string
		if !success {
			reason := "access denied"
			errorMsg = &reason
		}

		_, err = stmt.ExecContext(ctx,
			entry.Timestamp,
			entry.PrincipalType,
			entry.PrincipalID,
			entry.SubjectType,
			entry.SubjectID,
			entry.RootSubjectType,
			entry.RootSubjectID,
			entry.AuthorityMode,
			entry.RootGrantID,
			entry.AuthorityGrantID,
			entry.ParentGrantID,
			entry.ResourceType,
			entry.ResourceID,
			entry.Operation,
			entry.Workspace,
			entry.SessionID,
			entry.GatewayID,
			success,
			errorMsg,
			metadataJSON,
			audit.SourceGateway,
		)
		if err != nil {
			return fmt.Errorf("failed to insert audit entry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close stops the audit logger and flushes remaining entries
func (a *AuditLogger) Close() error {
	return a.base.Close()
}

// QueryAuditLog retrieves audit log entries matching the given filters
func (a *AuditLogger) QueryAuditLog(ctx context.Context, filter AuditLogFilter) ([]*AuditLogEntry, error) {
	query := `
		SELECT audit_id, timestamp, decision, access_level, principal_type, principal_id,
		       subject_type, subject_id, root_subject_type, root_subject_id,
		       authority_mode, root_authority_grant_id, authority_grant_id, parent_authority_grant_id,
		       resource_type, resource_id, operation, workspace,
		       rule_id, fallback_applied, gateway_id, session_id, metadata
		FROM acl_audit_log
		WHERE 1=1
	`
	// Note: acl_audit_log is now a view over comprehensive_audit_log (see migration 010)
	args := []interface{}{}
	argIdx := 1

	// Apply filters
	if filter.StartTime != nil {
		query += fmt.Sprintf(" AND timestamp >= $%d", argIdx)
		args = append(args, *filter.StartTime)
		argIdx++
	}

	if filter.EndTime != nil {
		query += fmt.Sprintf(" AND timestamp <= $%d", argIdx)
		args = append(args, *filter.EndTime)
		argIdx++
	}

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
		argIdx++
	}

	if filter.Decision != "" {
		query += fmt.Sprintf(" AND decision = $%d", argIdx)
		args = append(args, filter.Decision)
		argIdx++
	}

	if filter.Workspace != "" {
		query += fmt.Sprintf(" AND workspace = $%d", argIdx)
		args = append(args, filter.Workspace)
		argIdx++
	}

	query += " ORDER BY timestamp DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, filter.Limit)
	}

	rows, err := a.base.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit log: %w", err)
	}
	defer rows.Close()

	entries := []*AuditLogEntry{}

	for rows.Next() {
		var entry AuditLogEntry
		var metadataJSON []byte
		var ruleID sql.NullString
		var rootGrantID sql.NullString
		var authorityGrantID sql.NullString
		var parentGrantID sql.NullString

		err := rows.Scan(
			&entry.AuditID,
			&entry.Timestamp,
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
			&entry.SessionID,
			&metadataJSON,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan audit entry: %w", err)
		}

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

		if err := json.Unmarshal(metadataJSON, &entry.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		entries = append(entries, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating audit log: %w", err)
	}

	return entries, nil
}

// AuditLogFilter defines filters for querying the audit log
type AuditLogFilter struct {
	StartTime     *time.Time
	EndTime       *time.Time
	PrincipalType string
	PrincipalID   string
	ResourceType  string
	ResourceID    string
	Decision      string // "ALLOW" or "DENY"
	Workspace     string
	Limit         int
}

// CleanupOldLogs removes audit logs older than the retention period
func (a *AuditLogger) CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error) {
	query := `SELECT cleanup_old_audit_logs($1)`

	var deletedCount int64
	err := a.base.DB().QueryRowContext(ctx, query, retentionDays).Scan(&deletedCount)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup old audit logs: %w", err)
	}

	return deletedCount, nil
}
