package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// AuditLogger records comprehensive audit events to the database
// Uses buffered channel for async writes to avoid blocking operations
type AuditLogger struct {
	base      *BaseLogger[*AuditEvent]
	config    *Config
	gatewayID string
}

// NewAuditLogger creates a new audit logger with buffered async writes
func NewAuditLogger(db *sql.DB, gatewayID string, config *Config) *AuditLogger {
	if config == nil {
		config = DefaultConfig()
	}

	logger := &AuditLogger{
		config:    config,
		gatewayID: gatewayID,
	}

	// Start background writer if audit logging is enabled
	if config.Enabled {
		logger.base = NewBaseLogger[*AuditEvent](
			db,
			config.BatchSize,
			config.FlushPeriod,
			config.ChannelBuffer,
			auditEventBatchWriter,
		)
	}

	return logger
}

// LogEvent records an audit event to the log (async)
// If the event type is not enabled, this is a no-op
func (a *AuditLogger) LogEvent(ctx context.Context, event *AuditEvent) {
	if !a.config.Enabled {
		return
	}

	if !a.config.IsEventTypeEnabled(event.EventType) {
		return
	}

	a.prepareEvent(event)

	// Try to send to channel (non-blocking); drop if full (performance safety valve)
	a.base.Enqueue(event)
}

// LogEventSync records an audit event synchronously (blocks until written)
// Use sparingly - only for critical audit events that must be persisted immediately
func (a *AuditLogger) LogEventSync(ctx context.Context, event *AuditEvent) error {
	if !a.config.Enabled {
		return nil
	}

	if !a.config.IsEventTypeEnabled(event.EventType) {
		return ErrEventNotEnabled
	}

	a.prepareEvent(event)

	return a.writeEntry(ctx, event)
}

// prepareEvent sets gateway ID, timestamp, source, and metadata defaults on an
// event. When high-verbosity mode is active, metadata is passed through
// SanitizeMetadata to redact any credential-shaped keys before the event
// reaches the write queue.
func (a *AuditLogger) prepareEvent(event *AuditEvent) {
	if event.GatewayID == "" {
		event.GatewayID = a.gatewayID
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if event.Metadata == nil {
		event.Metadata = make(map[string]interface{})
	}
	if event.Source == "" {
		event.Source = SourceGateway
	}
	if a.config.VerbosityLevel == VerbosityHigh {
		event.Metadata = SanitizeMetadata(event.Metadata)
	}
}

// writeEntry writes a single audit entry to the database
func (a *AuditLogger) writeEntry(ctx context.Context, event *AuditEvent) error {
	applyDirectAuthority(event)

	metadataJSON, err := json.Marshal(event.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	var errorMsg *string
	if event.ErrorMessage != "" {
		errorMsg = &event.ErrorMessage
	}

	_, err = a.base.DB().ExecContext(ctx, `
		INSERT INTO comprehensive_audit_log (
			timestamp, event_type, actor_type, actor_id, subject_type, subject_id,
			root_subject_type, root_subject_id, authority_mode, root_authority_grant_id,
			authority_grant_id, parent_authority_grant_id, resource_type, resource_id,
			operation, workspace, session_id, gateway_id, success, error_message, metadata, source
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
	`,
		event.Timestamp,
		event.EventType,
		event.ActorType,
		event.ActorID,
		event.SubjectType,
		event.SubjectID,
		event.RootSubjectType,
		event.RootSubjectID,
		event.AuthorityMode,
		event.RootAuthorityGrantID,
		event.AuthorityGrantID,
		event.ParentAuthorityGrantID,
		event.ResourceType,
		event.ResourceID,
		event.Operation,
		event.Workspace,
		event.SessionID,
		event.GatewayID,
		event.Success,
		errorMsg,
		metadataJSON,
		event.Source,
	)
	if err != nil {
		return fmt.Errorf("failed to insert audit log entry: %w", err)
	}

	return nil
}

// auditEventBatchWriter writes a batch of AuditEvents in a single transaction.
// It is used as the BatchWriter for BaseLogger[*AuditEvent].
func auditEventBatchWriter(ctx context.Context, db *sql.DB, events []*AuditEvent) error {
	if len(events) == 0 {
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
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		applyDirectAuthority(event)

		metadataJSON, err := json.Marshal(event.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		var errorMsg *string
		if event.ErrorMessage != "" {
			errorMsg = &event.ErrorMessage
		}

		_, err = stmt.ExecContext(ctx,
			event.Timestamp,
			event.EventType,
			event.ActorType,
			event.ActorID,
			event.SubjectType,
			event.SubjectID,
			event.RootSubjectType,
			event.RootSubjectID,
			event.AuthorityMode,
			event.RootAuthorityGrantID,
			event.AuthorityGrantID,
			event.ParentAuthorityGrantID,
			event.ResourceType,
			event.ResourceID,
			event.Operation,
			event.Workspace,
			event.SessionID,
			event.GatewayID,
			event.Success,
			errorMsg,
			metadataJSON,
			event.Source,
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
	if !a.config.Enabled {
		return nil
	}

	return a.base.Close()
}

// QueryAuditLog retrieves audit log entries matching the given filters
func (a *AuditLogger) QueryAuditLog(ctx context.Context, filter EventFilter) ([]*AuditEvent, error) {
	query := `
		SELECT audit_id, timestamp, event_type, actor_type, actor_id, subject_type,
		       subject_id, root_subject_type, root_subject_id, authority_mode,
		       root_authority_grant_id, authority_grant_id, parent_authority_grant_id,
		       resource_type, resource_id, operation, workspace, session_id,
		       gateway_id, success, error_message, metadata, source
		FROM comprehensive_audit_log
		WHERE 1=1
	`
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

	if filter.EventType != "" {
		query += fmt.Sprintf(" AND event_type = $%d", argIdx)
		args = append(args, filter.EventType)
		argIdx++
	}

	if filter.ActorType != "" {
		query += fmt.Sprintf(" AND actor_type = $%d", argIdx)
		args = append(args, filter.ActorType)
		argIdx++
	}

	if filter.ActorID != "" {
		query += fmt.Sprintf(" AND actor_id = $%d", argIdx)
		args = append(args, filter.ActorID)
		argIdx++
	}

	if filter.SubjectType != "" {
		query += fmt.Sprintf(" AND subject_type = $%d", argIdx)
		args = append(args, filter.SubjectType)
		argIdx++
	}

	if filter.SubjectID != "" {
		query += fmt.Sprintf(" AND subject_id = $%d", argIdx)
		args = append(args, filter.SubjectID)
		argIdx++
	}

	if filter.AuthorityMode != "" {
		query += fmt.Sprintf(" AND authority_mode = $%d", argIdx)
		args = append(args, filter.AuthorityMode)
		argIdx++
	}

	if filter.AuthorityGrantID != "" {
		query += fmt.Sprintf(" AND authority_grant_id = $%d", argIdx)
		args = append(args, filter.AuthorityGrantID)
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

	if filter.Operation != "" {
		query += fmt.Sprintf(" AND operation = $%d", argIdx)
		args = append(args, filter.Operation)
		argIdx++
	}

	if filter.Workspace != "" {
		query += fmt.Sprintf(" AND workspace = $%d", argIdx)
		args = append(args, filter.Workspace)
		argIdx++
	}

	if filter.SessionID != nil {
		query += fmt.Sprintf(" AND session_id = $%d", argIdx)
		args = append(args, *filter.SessionID)
		argIdx++
	}

	if filter.Success != nil {
		query += fmt.Sprintf(" AND success = $%d", argIdx)
		args = append(args, *filter.Success)
		argIdx++
	}

	if len(filter.ExcludeActorTypes) > 0 {
		placeholders := make([]string, 0, len(filter.ExcludeActorTypes))
		for _, v := range filter.ExcludeActorTypes {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, v)
			argIdx++
		}
		query += " AND actor_type NOT IN (" + strings.Join(placeholders, ",") + ")"
	}

	if len(filter.ExcludeWorkspaces) > 0 {
		placeholders := make([]string, 0, len(filter.ExcludeWorkspaces))
		for _, v := range filter.ExcludeWorkspaces {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, v)
			argIdx++
		}
		query += " AND workspace NOT IN (" + strings.Join(placeholders, ",") + ")"
	}

	if filter.ExcludeServiceDirect {
		// Strip rows where a Service principal acted as itself (platform
		// bookkeeping), while keeping on-behalf-of service rows visible.
		query += " AND NOT (actor_type = 'service' AND authority_mode = 'direct')"
	}

	query += " ORDER BY timestamp DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, filter.Limit)
		argIdx++
	}

	if filter.Offset > 0 {
		if filter.Limit <= 0 {
			query += " LIMIT ALL"
		}
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, filter.Offset)
	}

	rows, err := a.base.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit log: %w", err)
	}
	defer rows.Close()

	events := []*AuditEvent{}

	for rows.Next() {
		var event AuditEvent
		var metadataJSON []byte
		var errorMsg sql.NullString
		var rootAuthorityGrantID sql.NullString
		var authorityGrantID sql.NullString
		var parentAuthorityGrantID sql.NullString

		err := rows.Scan(
			&event.AuditID,
			&event.Timestamp,
			&event.EventType,
			&event.ActorType,
			&event.ActorID,
			&event.SubjectType,
			&event.SubjectID,
			&event.RootSubjectType,
			&event.RootSubjectID,
			&event.AuthorityMode,
			&rootAuthorityGrantID,
			&authorityGrantID,
			&parentAuthorityGrantID,
			&event.ResourceType,
			&event.ResourceID,
			&event.Operation,
			&event.Workspace,
			&event.SessionID,
			&event.GatewayID,
			&event.Success,
			&errorMsg,
			&metadataJSON,
			&event.Source,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan audit entry: %w", err)
		}

		if errorMsg.Valid {
			event.ErrorMessage = errorMsg.String
		}
		if rootAuthorityGrantID.Valid {
			event.RootAuthorityGrantID = &rootAuthorityGrantID.String
		}
		if authorityGrantID.Valid {
			event.AuthorityGrantID = &authorityGrantID.String
		}
		if parentAuthorityGrantID.Valid {
			event.ParentAuthorityGrantID = &parentAuthorityGrantID.String
		}

		if err := json.Unmarshal(metadataJSON, &event.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		events = append(events, &event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating audit log: %w", err)
	}

	return events, nil
}

// CleanupOldLogs removes audit logs older than the retention period
func (a *AuditLogger) CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error) {
	query := `SELECT cleanup_old_comprehensive_audit_logs($1)`

	var deletedCount int64
	err := a.base.DB().QueryRowContext(ctx, query, retentionDays).Scan(&deletedCount)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup old audit logs: %w", err)
	}

	return deletedCount, nil
}

// GetConfig returns the current audit configuration
func (a *AuditLogger) GetConfig() *Config {
	return a.config
}

// Helper methods for creating common audit events

// NewConnectionEvent creates an audit event for connection lifecycle operations
func NewConnectionEvent(actorType, actorID, operation string, sessionID uuid.UUID, success bool, errorMsg string, metadata map[string]interface{}) *AuditEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &AuditEvent{
		EventType:       EventTypeConnection,
		ActorType:       actorType,
		ActorID:         actorID,
		SubjectType:     actorType,
		SubjectID:       actorID,
		RootSubjectType: actorType,
		RootSubjectID:   actorID,
		AuthorityMode:   AuthorityModeDirect,
		ResourceType:    ResourceTypeSession,
		ResourceID:      sessionID.String(),
		Operation:       operation,
		SessionID:       sessionID,
		Success:         success,
		ErrorMessage:    errorMsg,
		Metadata:        metadata,
		Source:          SourceGateway,
	}
}

// NewAuthEvent creates an audit event for authentication operations
func NewAuthEvent(actorType, actorID, operation, workspace string, sessionID uuid.UUID, success bool, errorMsg string, metadata map[string]interface{}) *AuditEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &AuditEvent{
		EventType:       EventTypeAuth,
		ActorType:       actorType,
		ActorID:         actorID,
		SubjectType:     actorType,
		SubjectID:       actorID,
		RootSubjectType: actorType,
		RootSubjectID:   actorID,
		AuthorityMode:   AuthorityModeDirect,
		ResourceType:    ResourceTypeWorkspace,
		ResourceID:      workspace,
		Operation:       operation,
		Workspace:       workspace,
		SessionID:       sessionID,
		Success:         success,
		ErrorMessage:    errorMsg,
		Metadata:        metadata,
		Source:          SourceGateway,
	}
}

// NewMessageEvent creates an audit event for message routing operations
func NewMessageEvent(actorType, actorID, operation, topic, workspace string, sessionID uuid.UUID, success bool, errorMsg string, metadata map[string]interface{}) *AuditEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &AuditEvent{
		EventType:       EventTypeMessage,
		ActorType:       actorType,
		ActorID:         actorID,
		SubjectType:     actorType,
		SubjectID:       actorID,
		RootSubjectType: actorType,
		RootSubjectID:   actorID,
		AuthorityMode:   AuthorityModeDirect,
		ResourceType:    ResourceTypeTopic,
		ResourceID:      topic,
		Operation:       operation,
		Workspace:       workspace,
		SessionID:       sessionID,
		Success:         success,
		ErrorMessage:    errorMsg,
		Metadata:        metadata,
		Source:          SourceGateway,
	}
}

// NewKVEvent creates an audit event for KV operations
func NewKVEvent(actorType, actorID, operation, key, workspace string, sessionID uuid.UUID, success bool, errorMsg string, metadata map[string]interface{}) *AuditEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &AuditEvent{
		EventType:       EventTypeKV,
		ActorType:       actorType,
		ActorID:         actorID,
		SubjectType:     actorType,
		SubjectID:       actorID,
		RootSubjectType: actorType,
		RootSubjectID:   actorID,
		AuthorityMode:   AuthorityModeDirect,
		ResourceType:    ResourceTypeKVKey,
		ResourceID:      key,
		Operation:       operation,
		Workspace:       workspace,
		SessionID:       sessionID,
		Success:         success,
		ErrorMessage:    errorMsg,
		Metadata:        metadata,
		Source:          SourceGateway,
	}
}

// NewTaskEvent creates an audit event for task lifecycle operations.
func NewTaskEvent(actorType, actorID, operation, taskID, workspace string, sessionID uuid.UUID, success bool, errorMsg string, metadata map[string]interface{}) *AuditEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &AuditEvent{
		EventType:       EventTypeTask,
		ActorType:       actorType,
		ActorID:         actorID,
		SubjectType:     actorType,
		SubjectID:       actorID,
		RootSubjectType: actorType,
		RootSubjectID:   actorID,
		AuthorityMode:   AuthorityModeDirect,
		ResourceType:    ResourceTypeTask,
		ResourceID:      taskID,
		Operation:       operation,
		Workspace:       workspace,
		SessionID:       sessionID,
		Success:         success,
		ErrorMessage:    errorMsg,
		Metadata:        metadata,
		Source:          SourceGateway,
	}
}

// NewAdminEvent creates an audit event for administrative operations
func NewAdminEvent(actorType, actorID, operation, resourceType, resourceID, workspace string, sessionID uuid.UUID, success bool, errorMsg string, metadata map[string]interface{}) *AuditEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &AuditEvent{
		EventType:       EventTypeAdmin,
		ActorType:       actorType,
		ActorID:         actorID,
		SubjectType:     actorType,
		SubjectID:       actorID,
		RootSubjectType: actorType,
		RootSubjectID:   actorID,
		AuthorityMode:   AuthorityModeDirect,
		ResourceType:    resourceType,
		ResourceID:      resourceID,
		Operation:       operation,
		Workspace:       workspace,
		SessionID:       sessionID,
		Success:         success,
		ErrorMessage:    errorMsg,
		Metadata:        metadata,
		Source:          SourceGateway,
	}
}
