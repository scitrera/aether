// Package sqlite provides a native-SQLite implementation of audit.Store.
// It is the Stage 2 replacement for the dbcompat-translated postgres impl
// that AetherLite used in Stage 1. This implementation:
//
//   - Uses pure SQLite SQL (no postgres-isms, no dbcompat translation)
//   - Stores timestamps as ISO-8601 TEXT, parses to time.Time inline
//   - Stores JSON metadata as TEXT, uses json_extract for queries if needed
//   - Uses INTEGER PRIMARY KEY AUTOINCREMENT (not BIGSERIAL)
//   - Implements CleanupOldLogs via parameterized DELETE (not PG stored function)
//   - Enforces single-writer discipline via sync.Mutex to prevent SQLITE_BUSY
//     contention in WAL mode (see master plan section 14.3)
//
// The store owns its own *sql.DB handle and migration runner. Callers
// provide an already-opened *sql.DB; the constructor runs migrations and
// configures connection pool limits.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/storage/audit"
	migrations "github.com/scitrera/aether/migrations/sqlite_audit_native"

	// Register the bare "sqlite" driver (modernc.org/sqlite). This is the
	// same underlying driver that pkg/dbcompat wraps as "sqlite_compat",
	// but we use it directly without the translation layer since all SQL
	// in this package is native SQLite.
	_ "modernc.org/sqlite"
)

// Compile-time conformance asserts.
var (
	_ audit.Store     = (*Store)(nil) // full interface
	_ audit.EventSink = (*Store)(nil) // narrow write-only interface used by ACL layer
)

// Store is the native-SQLite audit store. It uses the BaseLogger pattern
// from internal/audit for async batched writes (same as the postgres impl)
// but owns all SQL and timestamp handling natively.
//
// Write serialization: all database writes (both sync and batched) are
// serialized through writeMu. This prevents SQLITE_BUSY contention that
// would otherwise occur when the async batch writer and a synchronous
// LogEventSync caller race on the WAL writer lock. The mutex is lighter
// than the channel-based single-writer-goroutine alternative because the
// BaseLogger already provides the goroutine serialization for async writes;
// the mutex only needs to cover the sync path vs the async path.
type Store struct {
	db        *sql.DB
	config    *audit.Config
	gatewayID string
	writeMu   sync.Mutex // serializes all writes to prevent SQLITE_BUSY

	// Async batched writer infrastructure — mirrors internal/audit.BaseLogger
	// but with native SQL and inline timestamp formatting.
	entries      chan *audit.Event
	stopCh       chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
	droppedCount int64
	droppedMu    sync.Mutex
}

// New constructs a native-SQLite audit Store. It runs the native migration
// set against db, configures the connection pool for single-writer
// semantics, and starts the background batch writer goroutine.
//
// The db handle must be opened with the bare "sqlite" driver (not
// "sqlite_compat") since this impl owns all its own SQL. Callers retain
// ownership of db; Store.Close() does NOT close the underlying handle.
func New(db *sql.DB, gatewayID string, config *audit.Config) (*Store, error) {
	if config == nil {
		config = audit.DefaultConfig()
	}

	// Enforce single-writer pool. Even though we serialize writes via
	// writeMu, limiting the pool to 1 write connection avoids any
	// residual SQLITE_BUSY from concurrent readers that escalate to
	// writers in edge cases.
	db.SetMaxOpenConns(2) // 1 writer + 1 reader for concurrent query during batch write
	db.SetMaxIdleConns(2)

	ctx := context.Background()
	if err := runMigrations(ctx, db, migrations.MigrationFS); err != nil {
		return nil, fmt.Errorf("audit sqlite migrations: %w", err)
	}

	s := &Store{
		db:        db,
		config:    config,
		gatewayID: gatewayID,
	}

	// Start background writer if audit logging is enabled.
	if config.Enabled {
		s.entries = make(chan *audit.Event, config.ChannelBuffer)
		s.stopCh = make(chan struct{})
		s.wg.Add(1)
		go s.writeLoop()
	}

	return s, nil
}

// ---------------------------------------------------------------------------
// audit.Store interface implementation
// ---------------------------------------------------------------------------

// LogEvent enqueues an audit event for async batched write. Returns
// immediately. Drops the event if the channel buffer is full.
func (s *Store) LogEvent(ctx context.Context, event *audit.Event) {
	if !s.config.Enabled {
		return
	}
	if !s.config.IsEventTypeEnabled(event.EventType) {
		return
	}
	s.prepareEvent(event)

	select {
	case s.entries <- event:
	default:
		s.droppedMu.Lock()
		s.droppedCount++
		count := s.droppedCount
		s.droppedMu.Unlock()
		if count%10 == 1 {
			logging.Logger.Warn().Int64("total_dropped", count).Msg("audit event dropped (channel full)")
		}
	}
}

// LogEventSync writes a single audit event synchronously.
func (s *Store) LogEventSync(ctx context.Context, event *audit.Event) error {
	if !s.config.Enabled {
		return nil
	}
	if !s.config.IsEventTypeEnabled(event.EventType) {
		return audit.ErrEventNotEnabled
	}
	s.prepareEvent(event)

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeEvent(ctx, event)
}

// Close stops the async writer, flushing remaining events. Safe to call
// multiple times.
func (s *Store) Close() error {
	if !s.config.Enabled {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.stopCh)
		s.wg.Wait()
		close(s.entries)
	})
	return nil
}

// QueryAuditLog retrieves audit events matching the filter, ordered by
// timestamp DESC.
func (s *Store) QueryAuditLog(ctx context.Context, filter audit.EventFilter) ([]*audit.Event, error) {
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

	if filter.StartTime != nil {
		query += " AND timestamp >= ?"
		args = append(args, filter.StartTime.Format(time.RFC3339Nano))
	}
	if filter.EndTime != nil {
		query += " AND timestamp <= ?"
		args = append(args, filter.EndTime.Format(time.RFC3339Nano))
	}
	if filter.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, filter.EventType)
	}
	if filter.ActorType != "" {
		query += " AND actor_type = ?"
		args = append(args, filter.ActorType)
	}
	if filter.ActorID != "" {
		query += " AND actor_id = ?"
		args = append(args, filter.ActorID)
	}
	if filter.SubjectType != "" {
		query += " AND subject_type = ?"
		args = append(args, filter.SubjectType)
	}
	if filter.SubjectID != "" {
		query += " AND subject_id = ?"
		args = append(args, filter.SubjectID)
	}
	if filter.AuthorityMode != "" {
		query += " AND authority_mode = ?"
		args = append(args, filter.AuthorityMode)
	}
	if filter.AuthorityGrantID != "" {
		query += " AND authority_grant_id = ?"
		args = append(args, filter.AuthorityGrantID)
	}
	if filter.ResourceType != "" {
		query += " AND resource_type = ?"
		args = append(args, filter.ResourceType)
	}
	if filter.ResourceID != "" {
		query += " AND resource_id = ?"
		args = append(args, filter.ResourceID)
	}
	if filter.Operation != "" {
		query += " AND operation = ?"
		args = append(args, filter.Operation)
	}
	if filter.Workspace != "" {
		query += " AND workspace = ?"
		args = append(args, filter.Workspace)
	}
	if filter.SessionID != nil {
		query += " AND session_id = ?"
		args = append(args, filter.SessionID.String())
	}
	if filter.Success != nil {
		query += " AND success = ?"
		if *filter.Success {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}
	if len(filter.ExcludeActorTypes) > 0 {
		placeholders := make([]string, len(filter.ExcludeActorTypes))
		for i, v := range filter.ExcludeActorTypes {
			placeholders[i] = "?"
			args = append(args, v)
		}
		query += " AND actor_type NOT IN (" + strings.Join(placeholders, ",") + ")"
	}
	if len(filter.ExcludeWorkspaces) > 0 {
		placeholders := make([]string, len(filter.ExcludeWorkspaces))
		for i, v := range filter.ExcludeWorkspaces {
			placeholders[i] = "?"
			args = append(args, v)
		}
		query += " AND workspace NOT IN (" + strings.Join(placeholders, ",") + ")"
	}
	if filter.ExcludeServiceDirect {
		query += " AND NOT (actor_type = 'service' AND authority_mode = 'direct')"
	}

	query += " ORDER BY timestamp DESC"

	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}
	if filter.Offset > 0 {
		if filter.Limit <= 0 {
			// SQLite requires a LIMIT before OFFSET. Use -1 for unlimited.
			query += " LIMIT -1"
		}
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit log: %w", err)
	}
	defer rows.Close()

	var events []*audit.Event
	for rows.Next() {
		var (
			ev                     audit.Event
			timestampStr           string
			metadataJSON           []byte
			errorMsg               sql.NullString
			rootAuthorityGrantID   sql.NullString
			authorityGrantID       sql.NullString
			parentAuthorityGrantID sql.NullString
			sessionIDStr           sql.NullString
			successInt             int
		)

		err := rows.Scan(
			&ev.AuditID,
			&timestampStr,
			&ev.EventType,
			&ev.ActorType,
			&ev.ActorID,
			&ev.SubjectType,
			&ev.SubjectID,
			&ev.RootSubjectType,
			&ev.RootSubjectID,
			&ev.AuthorityMode,
			&rootAuthorityGrantID,
			&authorityGrantID,
			&parentAuthorityGrantID,
			&ev.ResourceType,
			&ev.ResourceID,
			&ev.Operation,
			&ev.Workspace,
			&sessionIDStr,
			&ev.GatewayID,
			&successInt,
			&errorMsg,
			&metadataJSON,
			&ev.Source,
		)
		if err != nil {
			return nil, fmt.Errorf("scan audit entry: %w", err)
		}

		// Inline timestamp parsing (no driver-level coercion).
		ev.Timestamp, err = parseTimestamp(timestampStr)
		if err != nil {
			return nil, fmt.Errorf("parse timestamp %q: %w", timestampStr, err)
		}

		ev.Success = successInt != 0

		if errorMsg.Valid {
			ev.ErrorMessage = errorMsg.String
		}
		if rootAuthorityGrantID.Valid {
			ev.RootAuthorityGrantID = &rootAuthorityGrantID.String
		}
		if authorityGrantID.Valid {
			ev.AuthorityGrantID = &authorityGrantID.String
		}
		if parentAuthorityGrantID.Valid {
			ev.ParentAuthorityGrantID = &parentAuthorityGrantID.String
		}
		if sessionIDStr.Valid {
			parsed, err := uuid.Parse(sessionIDStr.String)
			if err != nil {
				return nil, fmt.Errorf("parse session_id %q: %w", sessionIDStr.String, err)
			}
			ev.SessionID = parsed
		}

		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &ev.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}
		if ev.Metadata == nil {
			ev.Metadata = make(map[string]interface{})
		}

		events = append(events, &ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit log: %w", err)
	}

	if events == nil {
		events = []*audit.Event{}
	}
	return events, nil
}

// CleanupOldLogs deletes audit entries older than retentionDays via a
// parameterized DELETE. This replaces the postgres stored function
// cleanup_old_comprehensive_audit_logs that the postgres impl uses.
func (s *Store) CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays).Format(time.RFC3339Nano)

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	result, err := s.db.ExecContext(ctx,
		`DELETE FROM comprehensive_audit_log WHERE timestamp < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleanup old audit logs: %w", err)
	}
	return result.RowsAffected()
}

// GetConfig returns the active audit configuration.
func (s *Store) GetConfig() *audit.Config {
	return s.config
}

// ---------------------------------------------------------------------------
// Internal: event preparation and writing
// ---------------------------------------------------------------------------

// prepareEvent sets defaults on an event before writing. Mirrors the
// postgres impl's prepareEvent exactly for parity.
func (s *Store) prepareEvent(event *audit.Event) {
	if event.GatewayID == "" {
		event.GatewayID = s.gatewayID
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if event.Metadata == nil {
		event.Metadata = make(map[string]interface{})
	}
	if event.Source == "" {
		event.Source = audit.SourceGateway
	}
	if s.config.VerbosityLevel == audit.VerbosityHigh {
		event.Metadata = audit.SanitizeMetadata(event.Metadata)
	}

	// Apply direct-authority defaults — same logic as the postgres impl
	// (internal/audit.applyDirectAuthority). Inlined here to avoid
	// depending on the legacy package's unexported function.
	if event.AuthorityMode == "" {
		event.AuthorityMode = audit.AuthorityModeDirect
	}
	if event.SubjectType == "" {
		event.SubjectType = event.ActorType
	}
	if event.SubjectID == "" {
		event.SubjectID = event.ActorID
	}
	if event.RootSubjectType == "" {
		event.RootSubjectType = event.SubjectType
	}
	if event.RootSubjectID == "" {
		event.RootSubjectID = event.SubjectID
	}
	event.ActorType = audit.NormalizePrincipalTypeCase(event.ActorType)
	event.SubjectType = audit.NormalizePrincipalTypeCase(event.SubjectType)
	event.RootSubjectType = audit.NormalizePrincipalTypeCase(event.RootSubjectType)
}

// writeEvent writes a single event to the database. Caller must hold writeMu.
func (s *Store) writeEvent(ctx context.Context, event *audit.Event) error {
	metadataJSON, err := json.Marshal(event.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	var errorMsg *string
	if event.ErrorMessage != "" {
		errorMsg = &event.ErrorMessage
	}

	successInt := 0
	if event.Success {
		successInt = 1
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO comprehensive_audit_log (
			timestamp, event_type, actor_type, actor_id, subject_type, subject_id,
			root_subject_type, root_subject_id, authority_mode, root_authority_grant_id,
			authority_grant_id, parent_authority_grant_id, resource_type, resource_id,
			operation, workspace, session_id, gateway_id, success, error_message, metadata, source
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		event.Timestamp.Format(time.RFC3339Nano),
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
		event.SessionID.String(),
		event.GatewayID,
		successInt,
		errorMsg,
		metadataJSON,
		event.Source,
	)
	if err != nil {
		return fmt.Errorf("insert audit log entry: %w", err)
	}
	return nil
}

// writeBatch writes a batch of events in a single transaction. Called by
// the background writeLoop. Acquires writeMu for the entire batch.
func (s *Store) writeBatch(ctx context.Context, events []*audit.Event) error {
	if len(events) == 0 {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO comprehensive_audit_log (
			timestamp, event_type, actor_type, actor_id, subject_type, subject_id,
			root_subject_type, root_subject_id, authority_mode, root_authority_grant_id,
			authority_grant_id, parent_authority_grant_id, resource_type, resource_id,
			operation, workspace, session_id, gateway_id, success, error_message, metadata, source
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		metadataJSON, err := json.Marshal(event.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}

		var errorMsg *string
		if event.ErrorMessage != "" {
			errorMsg = &event.ErrorMessage
		}

		successInt := 0
		if event.Success {
			successInt = 1
		}

		_, err = stmt.ExecContext(ctx,
			event.Timestamp.Format(time.RFC3339Nano),
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
			event.SessionID.String(),
			event.GatewayID,
			successInt,
			errorMsg,
			metadataJSON,
			event.Source,
		)
		if err != nil {
			return fmt.Errorf("insert audit entry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// writeLoop is the background goroutine that batches and writes events.
// Mirrors internal/audit.BaseLogger.writeLoop.
func (s *Store) writeLoop() {
	defer s.wg.Done()
	batch := make([]*audit.Event, 0, s.config.BatchSize)
	ticker := time.NewTicker(s.config.FlushPeriod)
	defer ticker.Stop()

	for {
		select {
		case entry := <-s.entries:
			batch = append(batch, entry)
			if len(batch) >= s.config.BatchSize {
				if err := s.writeBatch(context.Background(), batch); err != nil {
					logging.Logger.Error().Err(err).Msg("failed to write audit log batch")
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := s.writeBatch(context.Background(), batch); err != nil {
					logging.Logger.Error().Err(err).Msg("failed to write audit log batch")
				}
				batch = batch[:0]
			}
		case <-s.stopCh:
			// Drain remaining entries.
			for {
				select {
				case entry := <-s.entries:
					batch = append(batch, entry)
				default:
					goto done
				}
			}
		done:
			if len(batch) > 0 {
				if err := s.writeBatch(context.Background(), batch); err != nil {
					logging.Logger.Error().Err(err).Msg("failed to write final audit log batch")
				}
			}
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Timestamp parsing — inline, no driver-level coercion
// ---------------------------------------------------------------------------

// timestampFormats lists the on-disk encodings we may encounter. The native
// impl writes RFC3339Nano exclusively, but we also handle the formats that
// SQLite's CURRENT_TIMESTAMP default and strftime produce.
var timestampFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000",       // strftime('%Y-%m-%dT%H:%M:%f', ...)
	"2006-01-02 15:04:05.999999999", // CURRENT_TIMESTAMP with fractional
	"2006-01-02 15:04:05",           // CURRENT_TIMESTAMP without fractional
}

// parseTimestamp parses a TEXT-encoded timestamp from SQLite into time.Time.
func parseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	for _, layout := range timestampFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

// ---------------------------------------------------------------------------
// Migration runner — self-contained, no dependency on cmd/aetherlite
// ---------------------------------------------------------------------------

// runMigrations applies the embedded SQL migration files to db in
// lexicographic order, tracking applied versions in a schema_migrations
// table. This is the same pattern used by the conformance test helper and
// cmd/aetherlite/main.go but self-contained to avoid import cycles.
func runMigrations(ctx context.Context, db *sql.DB, fs embed.FS) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now'))
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read embed fs: %w", err)
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
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}

		content, err := fs.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("exec %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	return nil
}
