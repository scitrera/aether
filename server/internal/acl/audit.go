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

// AuditLogger records ACL decisions to the audit log.
//
// Historically this type owned its own batched writer goroutine flushing
// INSERTs to comprehensive_audit_log. Two batchers writing to the same
// SQLite WAL writer lock (one here, one in internal/audit) caused
// SQLITE_BUSY contention in lite mode. The writer goroutine has been
// removed; this type is now a thin adapter that translates ACL decisions
// into audit.AuditEvent values and hands them to the shared audit writer
// (which owns the single producer→writer goroutine).
//
// The shared writer is accepted as audit.EventSink (the narrow
// write-only interface) rather than the legacy *audit.AuditLogger
// concrete. Both the legacy *audit.AuditLogger and the native-sqlite
// *auditsqlite.Store satisfy EventSink, so Wave 3 can cut over audit
// to the native impl without touching ACL constructors again.
//
// Reads (QueryAuditLog, CleanupOldLogs) still use a *sql.DB handle
// directly — they don't contend with the writer.
type AuditLogger struct {
	shared    audit.EventSink // shared writer for all comprehensive_audit_log INSERTs
	db        *sql.DB         // read-side handle (audit.db in lite; aether.db in postgres)
	gatewayID string
}

// NewAuditLogger creates an ACL audit-log adapter that funnels writes
// through the shared audit writer. The `db` handle is used only for
// read-side operations (QueryAuditLog, CleanupOldLogs) and must point at
// the same physical file the shared writer is targeting — audit.db in
// the lite split layout, aether.db otherwise.
//
// shared may be nil if audit logging is disabled at the platform level;
// LogDecision becomes a no-op in that case.
//
// The shared parameter is typed as audit.EventSink (the narrow
// write-only interface). Both the legacy *audit.AuditLogger and the
// native-sqlite *auditsqlite.Store satisfy this interface, so the ACL
// layer is decoupled from the concrete audit implementation chosen at
// the construction site.
//
// The "authorization" event type (audit.EventTypeAuthorization) is the
// canonical event_type for ACL decisions in comprehensive_audit_log
// (per migrations 008–018 and the acl_audit_log view). It is included
// in audit.DefaultConfig().EnabledEventTypes so ACL decisions are not
// silently dropped by the shared writer's gating logic — no extra
// configuration shim is needed here.
func NewAuditLogger(shared audit.EventSink, db *sql.DB, gatewayID string) *AuditLogger {
	return &AuditLogger{
		shared:    shared,
		db:        db,
		gatewayID: gatewayID,
	}
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

// LogDecision records an ACL decision through the shared audit writer.
// Non-blocking: drops if the shared queue is full (same performance safety
// valve as audit.AuditLogger.LogEvent).
func (a *AuditLogger) LogDecision(ctx context.Context, decision *ACLDecision, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID) {
	if a.shared == nil {
		return
	}
	entry := a.buildEntry(decision, principal, resourceType, resourceID, operation, workspace, sessionID)
	event := a.entryToEvent(entry)
	a.shared.LogEvent(ctx, event)
}

// LogAuthorityRequestEvent emits a Phase 2 authority-request lifecycle audit
// event (EventTypeAuthorityRequest) through the shared writer. This is the
// generic-event sibling to LogDecision: it skips the ACL-decision plumbing
// (no rule_applied / fallback_applied / access-level), embedding the request
// shape in the AuditEvent's metadata map instead.
//
// Non-blocking: drops if the shared queue is full (same performance safety
// valve as audit.AuditLogger.LogEvent) — preserve the fire-and-forget contract
// the existing audit pipeline relies on. If `a.shared` is nil (audit disabled
// at the platform level) this is a no-op.
//
// Field shape:
//   - EventType    = audit.EventTypeAuthorityRequest
//   - ActorType/ID = the actor performing the lifecycle action (the requester
//     on Submit; the approver on Approve/Deny; the requester on Cancel; the
//     system on Sweep — caller passes models.Identity{} for system-initiated
//     events and the helper leaves the actor fields blank).
//   - Subject*     = the request's requesting_actor (the principal whose
//     authority would be elevated). For approval flows the resulting grant
//     subject is recorded separately in metadata.granted_grant_id.
//   - Operation    = lifecycleOp (e.g. "authority_request_created").
//   - Workspace    = first entry of req.WorkspaceScope when present, else "".
//   - Metadata     = {request_id, status, requesting_actor_type,
//     requesting_actor_id, routing_capability,
//     routing_principal_type/_id, requested_access,
//     duration_seconds, task_id, audience_type, audience_id,
//     resolution_reason, granted_grant_id (when set),
//     expires_at (RFC3339)}, merged with caller-supplied extras.
//
// `lifecycleOp` is the operation string used both as the event's Operation
// column and as a metadata.request_lifecycle_event hint. Callers should pass
// the canonical constant strings ("authority_request_created",
// "authority_request_approved", "authority_request_denied",
// "authority_request_expired", "authority_request_cancelled").
func (a *AuditLogger) LogAuthorityRequestEvent(
	ctx context.Context,
	req *AuthorityRequest,
	lifecycleOp string,
	actor models.Identity,
	extras map[string]interface{},
) {
	if a.shared == nil || req == nil {
		return
	}

	actorType, actorID := "", ""
	if ref := actor.PrincipalRef(); !ref.IsZero() {
		actorType = PrincipalTypeForModel(ref.Type)
		actorID = ref.ID
	}

	subjectType, subjectID := "", ""
	if ref := req.RequestingActor.PrincipalRef(); !ref.IsZero() {
		subjectType = PrincipalTypeForModel(ref.Type)
		subjectID = ref.ID
	}

	workspace := ""
	if len(req.WorkspaceScope) > 0 {
		workspace = req.WorkspaceScope[0]
	}

	metadata := make(map[string]interface{}, 16+len(extras))
	for k, v := range extras {
		metadata[k] = v
	}
	metadata["request_lifecycle_event"] = lifecycleOp
	metadata["request_id"] = req.RequestID
	metadata["status"] = string(req.Status)
	if subjectType != "" {
		metadata["requesting_actor_type"] = subjectType
		metadata["requesting_actor_id"] = subjectID
	}
	if req.RoutingTarget.Capability != "" {
		metadata["routing_capability"] = req.RoutingTarget.Capability
	}
	if req.RoutingTarget.Principal != nil {
		if ref := req.RoutingTarget.Principal.PrincipalRef(); !ref.IsZero() {
			metadata["routing_principal_type"] = PrincipalTypeForModel(ref.Type)
			metadata["routing_principal_id"] = ref.ID
		}
	}
	metadata["requested_access"] = req.RequestedAccess
	metadata["duration_seconds"] = req.DurationSeconds
	if req.TaskID != "" {
		metadata["task_id"] = req.TaskID
	}
	if req.AudienceType != "" {
		metadata["audience_type"] = req.AudienceType
	}
	if req.AudienceID != "" {
		metadata["audience_id"] = req.AudienceID
	}
	if req.ResolutionReason != "" {
		metadata["resolution_reason"] = req.ResolutionReason
	}
	if req.GrantedGrantID != "" {
		metadata["granted_grant_id"] = req.GrantedGrantID
	}
	if !req.ExpiresAt.IsZero() {
		metadata["expires_at"] = req.ExpiresAt.UTC().Format(time.RFC3339)
	}

	event := &audit.AuditEvent{
		Timestamp:     time.Now(),
		EventType:     audit.EventTypeAuthorityRequest,
		ActorType:     actorType,
		ActorID:       actorID,
		SubjectType:   subjectType,
		SubjectID:     subjectID,
		AuthorityMode: audit.AuthorityModeDirect,
		Operation:     lifecycleOp,
		Workspace:     workspace,
		GatewayID:     a.gatewayID,
		Success:       true,
		Metadata:      metadata,
		Source:        audit.SourceGateway,
	}
	a.shared.LogEvent(ctx, event)
}

// entryToEvent translates an ACL AuditLogEntry into an audit.AuditEvent
// suitable for the shared writer. Field shape matches the INSERT that the
// old acl.aclEntryBatchWriter performed (event_type='authorization',
// success=ALLOW, error="access denied" on deny, metadata carries
// decision/access_level/fallback_applied/rule_id plus any extras).
func (a *AuditLogger) entryToEvent(entry *AuditLogEntry) *audit.AuditEvent {
	success := entry.Decision == DecisionAllow
	errorMsg := ""
	if !success {
		errorMsg = "access denied"
	}

	// Build the metadata map by reusing the JSON-side merge in
	// buildACLMetadata, then unmarshalling back into a map. This keeps the
	// merged shape identical to what the previous batched writer persisted
	// (preserves caller-supplied extras alongside the ACL-specific fields).
	var metadata map[string]interface{}
	if raw, err := buildACLMetadata(entry); err == nil {
		_ = json.Unmarshal(raw, &metadata)
	}
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	return &audit.AuditEvent{
		Timestamp:              entry.Timestamp,
		EventType:              audit.EventTypeAuthorization,
		ActorType:              entry.PrincipalType,
		ActorID:                entry.PrincipalID,
		SubjectType:            entry.SubjectType,
		SubjectID:              entry.SubjectID,
		RootSubjectType:        entry.RootSubjectType,
		RootSubjectID:          entry.RootSubjectID,
		AuthorityMode:          entry.AuthorityMode,
		RootAuthorityGrantID:   entry.RootGrantID,
		AuthorityGrantID:       entry.AuthorityGrantID,
		ParentAuthorityGrantID: entry.ParentGrantID,
		ResourceType:           entry.ResourceType,
		ResourceID:             entry.ResourceID,
		Operation:              entry.Operation,
		Workspace:              entry.Workspace,
		SessionID:              entry.SessionID,
		GatewayID:              entry.GatewayID,
		Success:                success,
		ErrorMessage:           errorMsg,
		Metadata:               metadata,
		Source:                 audit.SourceGateway,
	}
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

// Close releases adapter references. The shared writer's Close is owned
// by the gateway shutdown sequence (not the ACL layer), so this is a
// no-op that exists only to preserve the prior API.
func (a *AuditLogger) Close() error {
	a.shared = nil
	a.db = nil
	return nil
}

// QueryAuditLog retrieves audit log entries matching the given filters.
// Reads run against the same physical file the shared writer targets;
// SQLite's WAL allows concurrent readers alongside the single writer.
func (a *AuditLogger) QueryAuditLog(ctx context.Context, filter AuditLogFilter) ([]*AuditLogEntry, error) {
	if a.db == nil {
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

	rows, err := a.db.QueryContext(ctx, query, args...)
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

// CleanupOldLogs removes audit logs older than the retention period.
// Runs against the read-side DB handle; the shared writer is not involved.
func (a *AuditLogger) CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error) {
	if a.db == nil {
		return 0, fmt.Errorf("acl audit logger has no read database handle")
	}
	query := `SELECT cleanup_old_audit_logs($1)`

	var deletedCount int64
	err := a.db.QueryRowContext(ctx, query, retentionDays).Scan(&deletedCount)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup old audit logs: %w", err)
	}

	return deletedCount, nil
}
