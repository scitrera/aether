// Package sqlite provides a native SQLite implementation of workflow.Store.
//
// This is the Stage 2 native implementation — it writes its own SQL using
// SQLite-native idioms (TEXT timestamps stored as ISO-8601, INTEGER booleans,
// json_extract for JSON columns, no PG-isms). It does NOT go through
// pkg/dbcompat; it uses the bare "sqlite" driver (modernc.org/sqlite)
// directly, owning all timestamp parsing inline.
//
// Key design decisions:
//   - Single-writer goroutine: SetMaxOpenConns(1) is enforced at construction
//     time per §14.3 (WAL contention is per-file, not per-table).
//   - Inline time.Time parsing: all timestamp columns are stored as ISO-8601
//     TEXT and parsed back to time.Time at scan time. No driver-level coercion.
//   - No isSQLite bool: this impl is always SQLite. The postgres-specific
//     `FOR UPDATE SKIP LOCKED` suffix on GetDueSchedules is simply absent.
//   - RETURNING clauses: SQLite 3.35+ supports RETURNING. modernc.org/sqlite
//     embeds SQLite 3.46+ so this is safe.
//   - DISTINCT ON: not supported by SQLite. GetWorkflowDefinitionsForTrigger
//     uses a GROUP BY + MAX(version) subquery instead.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	workflow "github.com/scitrera/aether/internal/storage/workflow"
	migrations "github.com/scitrera/aether/migrations/sqlite_workflow"

	_ "modernc.org/sqlite" // registers bare "sqlite" driver
)

// Compile-time conformance assert — build breaks if Store drifts from
// the workflow.Store interface.
var _ workflow.Store = (*Store)(nil)

// Store is the native-SQLite workflow store. It owns its own *sql.DB
// handle (one file: workflow.db) and runs migrations at construction time.
type Store struct {
	db *sql.DB
}

// New constructs a native-SQLite workflow Store on top of the given *sql.DB.
// The caller MUST have opened the DB with the bare "sqlite" driver
// (modernc.org/sqlite) — NOT "sqlite_compat". New enforces
// SetMaxOpenConns(1) for single-writer safety (§14.3) and runs the
// native migration set from migrations/sqlite_workflow/.
//
// Callers retain ownership of db; Close is a no-op on the Store itself
// (the Store does not own background goroutines).
func New(db *sql.DB) (*Store, error) {
	// §14.3: single-writer goroutine per file. Enforce at the pool level
	// so no concurrent Exec/Query can trigger SQLITE_BUSY on the WAL lock.
	db.SetMaxOpenConns(1)

	if err := runMigrations(context.Background(), db); err != nil {
		return nil, fmt.Errorf("workflow sqlite migrations: %w", err)
	}

	return &Store{db: db}, nil
}

// =============================================================================
// Timestamp helpers — inline parsing per §15.4 and the master plan §3.
//
// All timestamps are stored as ISO-8601 TEXT via strftime('%Y-%m-%dT%H:%M:%SZ')
// in the schema defaults. Parameter-bound timestamps are formatted the same
// way. On read, we parse back to time.Time. The monotonic-clock suffix
// (m=+...) is stripped defensively in case any code path stringifies a
// time.Time via its default String() method.
// =============================================================================

// timestampFormats lists the formats we accept on read, tried in order.
// The first format matches our own writes; the rest are defensive against
// legacy data or driver quirks.
var timestampFormats = []string{
	time.RFC3339Nano, // 2006-01-02T15:04:05.999999999Z07:00
	time.RFC3339,     // 2006-01-02T15:04:05Z07:00
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05.999999999 -0700 MST",
}

// formatTime formats a time.Time for storage. Always UTC, always ISO-8601
// with no monotonic suffix.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// formatTimePtr formats a *time.Time for storage, returning nil for NULL.
func formatTimePtr(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

// parseTime parses a TEXT timestamp column into time.Time.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	// Strip Go monotonic-clock suffix defensively.
	if i := strings.Index(s, " m=+"); i >= 0 {
		s = s[:i]
	} else if i := strings.Index(s, " m=-"); i >= 0 {
		s = s[:i]
	}
	for _, layout := range timestampFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

// parseTimePtr parses a nullable TEXT timestamp column.
func parseTimePtr(v interface{}) (*time.Time, error) {
	if v == nil {
		return nil, nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected string for timestamp, got %T", v)
	}
	if s == "" {
		return nil, nil
	}
	t, err := parseTime(s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// nowUTC returns the current time in UTC, stripped of monotonic clock.
func nowUTC() string {
	return formatTime(time.Now())
}

// =============================================================================
// Rules — workflow_rules table
// =============================================================================

func (s *Store) GetMatchingRules(ctx context.Context, sourceAgent, sourceEvent, workspace string) ([]Rule, error) {
	query := `
		SELECT id, rule_name, source_agent, source_event, trigger_condition,
		       transformation_style, destination_template, workspace, priority, active,
		       created_at, updated_at
		FROM workflow_rules
		WHERE active = 1
		  AND source_event = ?
		  AND (source_agent = ? OR source_agent = '*')
		  AND (workspace = ? OR workspace = '*')
		ORDER BY priority DESC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, query, sourceEvent, sourceAgent, workspace)
	if err != nil {
		return nil, fmt.Errorf("query matching rules: %w", err)
	}
	defer rows.Close()
	return scanRules(rows)
}

func (s *Store) CreateRule(ctx context.Context, r *Rule) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_rules (rule_name, source_agent, source_event, trigger_condition,
		                            transformation_style, destination_template, workspace, priority, active,
		                            created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id, created_at, updated_at
	`
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query,
		r.RuleName, r.SourceAgent, r.SourceEvent, r.TriggerCondition,
		r.TransformationStyle, r.DestinationTemplate, r.Workspace, r.Priority, boolToInt(r.Active),
		now, now,
	).Scan(&r.ID, &createdAtStr, &updatedAtStr)
	if err != nil {
		return fmt.Errorf("create rule: %w", err)
	}
	r.CreatedAt, err = parseTime(createdAtStr)
	if err != nil {
		return fmt.Errorf("parse created_at: %w", err)
	}
	r.UpdatedAt, err = parseTime(updatedAtStr)
	if err != nil {
		return fmt.Errorf("parse updated_at: %w", err)
	}
	return nil
}

func (s *Store) UpdateRule(ctx context.Context, r *Rule) error {
	query := `
		UPDATE workflow_rules
		SET rule_name = ?, source_agent = ?, source_event = ?, trigger_condition = ?,
		    transformation_style = ?, destination_template = ?, workspace = ?,
		    priority = ?, active = ?
		WHERE id = ?
	`
	_, err := s.db.ExecContext(ctx, query,
		r.RuleName, r.SourceAgent, r.SourceEvent, r.TriggerCondition,
		r.TransformationStyle, r.DestinationTemplate, r.Workspace, r.Priority, boolToInt(r.Active),
		r.ID,
	)
	return err
}

func (s *Store) DeleteRule(ctx context.Context, id int) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM workflow_rules WHERE id = ?", id)
	return err
}

func (s *Store) ListRules(ctx context.Context, workspace string) ([]Rule, error) {
	query := `
		SELECT id, rule_name, source_agent, source_event, trigger_condition,
		       transformation_style, destination_template, workspace, priority, active,
		       created_at, updated_at
		FROM workflow_rules
		WHERE workspace = ? OR workspace = '*'
		ORDER BY priority DESC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	defer rows.Close()
	return scanRules(rows)
}

func (s *Store) GetRule(ctx context.Context, id int) (*Rule, error) {
	query := `
		SELECT id, rule_name, source_agent, source_event, trigger_condition,
		       transformation_style, destination_template, workspace, priority, active,
		       created_at, updated_at
		FROM workflow_rules
		WHERE id = ?
	`
	var r Rule
	var activeInt int
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&r.ID, &r.RuleName, &r.SourceAgent, &r.SourceEvent, &r.TriggerCondition,
		&r.TransformationStyle, &r.DestinationTemplate, &r.Workspace, &r.Priority, &activeInt,
		&createdAtStr, &updatedAtStr,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rule: %w", err)
	}
	r.Active = activeInt != 0
	r.CreatedAt, _ = parseTime(createdAtStr)
	r.UpdatedAt, _ = parseTime(updatedAtStr)
	return &r, nil
}

// scanRules scans multiple rule rows from a query result.
func scanRules(rows *sql.Rows) ([]Rule, error) {
	var rules []Rule
	for rows.Next() {
		var r Rule
		var activeInt int
		var createdAtStr, updatedAtStr string
		if err := rows.Scan(
			&r.ID, &r.RuleName, &r.SourceAgent, &r.SourceEvent, &r.TriggerCondition,
			&r.TransformationStyle, &r.DestinationTemplate, &r.Workspace, &r.Priority, &activeInt,
			&createdAtStr, &updatedAtStr,
		); err != nil {
			return nil, fmt.Errorf("scan rule: %w", err)
		}
		r.Active = activeInt != 0
		r.CreatedAt, _ = parseTime(createdAtStr)
		r.UpdatedAt, _ = parseTime(updatedAtStr)
		rules = append(rules, r)
	}
	return rules, rows.Err()
}

// =============================================================================
// Workflow definitions — workflow_definitions table
// =============================================================================

func (s *Store) GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error) {
	query := `
		SELECT id, version, workspace, definition, active, created_at, updated_at
		FROM workflow_definitions
		WHERE id = ? AND active = 1
		ORDER BY version DESC
		LIMIT 1
	`
	var d WorkflowDefinition
	var activeInt int
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&d.ID, &d.Version, &d.Workspace, &d.Definition, &activeInt, &createdAtStr, &updatedAtStr,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	d.Active = activeInt != 0
	d.CreatedAt, _ = parseTime(createdAtStr)
	d.UpdatedAt, _ = parseTime(updatedAtStr)
	return &d, nil
}

func (s *Store) CreateWorkflowDefinition(ctx context.Context, d *WorkflowDefinition) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_definitions (id, version, workspace, definition, active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING created_at, updated_at
	`
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query,
		d.ID, d.Version, d.Workspace, d.Definition, boolToInt(d.Active), now, now,
	).Scan(&createdAtStr, &updatedAtStr)
	if err != nil {
		return fmt.Errorf("create workflow definition: %w", err)
	}
	d.CreatedAt, _ = parseTime(createdAtStr)
	d.UpdatedAt, _ = parseTime(updatedAtStr)
	return nil
}

// GetWorkflowDefinitionsForTrigger returns the latest active version of every
// definition visible to the given workspace. Uses GROUP BY + MAX(version)
// instead of postgres's DISTINCT ON.
func (s *Store) GetWorkflowDefinitionsForTrigger(ctx context.Context, workspace string) ([]WorkflowDefinition, error) {
	query := `
		SELECT d.id, d.version, d.workspace, d.definition, d.active, d.created_at, d.updated_at
		FROM workflow_definitions d
		INNER JOIN (
			SELECT id, MAX(version) AS max_version
			FROM workflow_definitions
			WHERE active = 1 AND (workspace = ? OR workspace = '*')
			GROUP BY id
		) latest ON d.id = latest.id AND d.version = latest.max_version
		WHERE d.active = 1
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("get workflow definitions for trigger: %w", err)
	}
	defer rows.Close()

	var defs []WorkflowDefinition
	for rows.Next() {
		var d WorkflowDefinition
		var activeInt int
		var createdAtStr, updatedAtStr string
		if err := rows.Scan(&d.ID, &d.Version, &d.Workspace, &d.Definition, &activeInt, &createdAtStr, &updatedAtStr); err != nil {
			return nil, fmt.Errorf("scan workflow definition: %w", err)
		}
		d.Active = activeInt != 0
		d.CreatedAt, _ = parseTime(createdAtStr)
		d.UpdatedAt, _ = parseTime(updatedAtStr)
		defs = append(defs, d)
	}
	return defs, rows.Err()
}

func (s *Store) DeactivateWorkflowDefinition(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE workflow_definitions SET active = 0 WHERE id = ?", id)
	return err
}

// =============================================================================
// Executions — workflow_executions table
// =============================================================================

func (s *Store) CreateExecution(ctx context.Context, e *WorkflowExecution) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_executions (execution_id, workflow_id, workflow_version, workspace, status, trigger_data, metadata, started_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING started_at
	`
	var startedAtStr string
	err := s.db.QueryRowContext(ctx, query,
		e.ExecutionID, e.WorkflowID, e.WorkflowVersion, e.Workspace, e.Status, e.TriggerData, e.Metadata, now,
	).Scan(&startedAtStr)
	if err != nil {
		return fmt.Errorf("create execution: %w", err)
	}
	e.StartedAt, _ = parseTime(startedAtStr)
	return nil
}

func (s *Store) GetExecution(ctx context.Context, executionID string) (*WorkflowExecution, error) {
	query := `
		SELECT execution_id, workflow_id, workflow_version, workspace, status,
		       trigger_data, started_at, completed_at, error_message, metadata
		FROM workflow_executions
		WHERE execution_id = ?
	`
	var e WorkflowExecution
	var errMsg sql.NullString
	var startedAtStr string
	var completedAtRaw sql.NullString
	err := s.db.QueryRowContext(ctx, query, executionID).Scan(
		&e.ExecutionID, &e.WorkflowID, &e.WorkflowVersion, &e.Workspace, &e.Status,
		&e.TriggerData, &startedAtStr, &completedAtRaw, &errMsg, &e.Metadata,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get execution: %w", err)
	}
	e.StartedAt, _ = parseTime(startedAtStr)
	e.ErrorMessage = errMsg.String
	if completedAtRaw.Valid {
		t, parseErr := parseTime(completedAtRaw.String)
		if parseErr == nil {
			e.CompletedAt = &t
		}
	}
	return &e, nil
}

func (s *Store) UpdateExecutionStatus(ctx context.Context, executionID, status, errorMessage string) error {
	var query string
	if status == workflow.ExecStatusCompleted || status == workflow.ExecStatusFailed || status == workflow.ExecStatusCancelled {
		now := nowUTC()
		query = `UPDATE workflow_executions SET status = ?, error_message = ?, completed_at = ? WHERE execution_id = ?`
		_, err := s.db.ExecContext(ctx, query, status, errorMessage, now, executionID)
		return err
	}
	query = `UPDATE workflow_executions SET status = ?, error_message = ? WHERE execution_id = ?`
	_, err := s.db.ExecContext(ctx, query, status, errorMessage, executionID)
	return err
}

func (s *Store) GetRunningExecutions(ctx context.Context) ([]WorkflowExecution, error) {
	query := `
		SELECT execution_id, workflow_id, workflow_version, workspace, status,
		       trigger_data, started_at, completed_at, error_message, metadata
		FROM workflow_executions
		WHERE status = 'running'
		ORDER BY started_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get running executions: %w", err)
	}
	defer rows.Close()
	return scanExecutions(rows)
}

func (s *Store) CountRunningExecutions(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_executions WHERE status = 'running'").Scan(&count)
	return count, err
}

func (s *Store) GetExecutionsByStatus(ctx context.Context, status string) ([]WorkflowExecution, error) {
	query := `
		SELECT execution_id, workflow_id, workflow_version, workspace, status,
		       trigger_data, started_at, completed_at, error_message, metadata
		FROM workflow_executions
		WHERE status = ?
		ORDER BY started_at DESC
		LIMIT 100
	`
	rows, err := s.db.QueryContext(ctx, query, status)
	if err != nil {
		return nil, fmt.Errorf("get executions by status: %w", err)
	}
	defer rows.Close()
	return scanExecutions(rows)
}

// scanExecutions scans multiple execution rows.
func scanExecutions(rows *sql.Rows) ([]WorkflowExecution, error) {
	var execs []WorkflowExecution
	for rows.Next() {
		var e WorkflowExecution
		var errMsg sql.NullString
		var startedAtStr string
		var completedAtRaw sql.NullString
		if err := rows.Scan(
			&e.ExecutionID, &e.WorkflowID, &e.WorkflowVersion, &e.Workspace, &e.Status,
			&e.TriggerData, &startedAtStr, &completedAtRaw, &errMsg, &e.Metadata,
		); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		e.StartedAt, _ = parseTime(startedAtStr)
		e.ErrorMessage = errMsg.String
		if completedAtRaw.Valid {
			t, parseErr := parseTime(completedAtRaw.String)
			if parseErr == nil {
				e.CompletedAt = &t
			}
		}
		execs = append(execs, e)
	}
	return execs, rows.Err()
}

// =============================================================================
// Step states — workflow_step_states table
// =============================================================================

func (s *Store) CreateStepState(ctx context.Context, st *StepState) error {
	query := `
		INSERT INTO workflow_step_states (execution_id, step_id, status, input_data, attempt)
		VALUES (?, ?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, query,
		st.ExecutionID, st.StepID, st.Status, st.InputData, st.Attempt,
	)
	return err
}

func (s *Store) UpdateStepStatus(ctx context.Context, executionID, stepID, status string) error {
	now := nowUTC()
	var query string
	switch status {
	case workflow.StepStatusRunning:
		query = `UPDATE workflow_step_states SET status = ?, started_at = ? WHERE execution_id = ? AND step_id = ?`
		_, err := s.db.ExecContext(ctx, query, status, now, executionID, stepID)
		return err
	case workflow.StepStatusCompleted, workflow.StepStatusFailed, workflow.StepStatusSkipped:
		query = `UPDATE workflow_step_states SET status = ?, completed_at = ? WHERE execution_id = ? AND step_id = ?`
		_, err := s.db.ExecContext(ctx, query, status, now, executionID, stepID)
		return err
	default:
		query = `UPDATE workflow_step_states SET status = ? WHERE execution_id = ? AND step_id = ?`
		_, err := s.db.ExecContext(ctx, query, status, executionID, stepID)
		return err
	}
}

func (s *Store) SetStepOutput(ctx context.Context, executionID, stepID string, output json.RawMessage) error {
	now := nowUTC()
	query := `UPDATE workflow_step_states SET output_data = ?, status = 'completed', completed_at = ? WHERE execution_id = ? AND step_id = ?`
	_, err := s.db.ExecContext(ctx, query, output, now, executionID, stepID)
	return err
}

func (s *Store) SetStepError(ctx context.Context, executionID, stepID, errorMessage string) error {
	now := nowUTC()
	query := `UPDATE workflow_step_states SET error_message = ?, status = 'failed', completed_at = ? WHERE execution_id = ? AND step_id = ?`
	_, err := s.db.ExecContext(ctx, query, errorMessage, now, executionID, stepID)
	return err
}

func (s *Store) SetStepTaskID(ctx context.Context, executionID, stepID, taskID string) error {
	query := `UPDATE workflow_step_states SET task_id = ? WHERE execution_id = ? AND step_id = ?`
	_, err := s.db.ExecContext(ctx, query, taskID, executionID, stepID)
	return err
}

func (s *Store) IncrementStepAttempt(ctx context.Context, executionID, stepID string) error {
	query := `UPDATE workflow_step_states SET attempt = attempt + 1, status = 'pending', started_at = NULL, completed_at = NULL, error_message = NULL WHERE execution_id = ? AND step_id = ?`
	_, err := s.db.ExecContext(ctx, query, executionID, stepID)
	return err
}

func (s *Store) GetStepStates(ctx context.Context, executionID string) ([]StepState, error) {
	query := `
		SELECT execution_id, step_id, status, started_at, completed_at,
		       input_data, output_data, error_message, attempt, COALESCE(task_id, '')
		FROM workflow_step_states
		WHERE execution_id = ?
		ORDER BY step_id
	`
	rows, err := s.db.QueryContext(ctx, query, executionID)
	if err != nil {
		return nil, fmt.Errorf("get step states: %w", err)
	}
	defer rows.Close()

	var steps []StepState
	for rows.Next() {
		var st StepState
		var errMsg sql.NullString
		var startedAtRaw, completedAtRaw sql.NullString
		if err := rows.Scan(
			&st.ExecutionID, &st.StepID, &st.Status, &startedAtRaw, &completedAtRaw,
			&st.InputData, &st.OutputData, &errMsg, &st.Attempt, &st.TaskID,
		); err != nil {
			return nil, fmt.Errorf("scan step state: %w", err)
		}
		st.ErrorMessage = errMsg.String
		if startedAtRaw.Valid {
			t, parseErr := parseTime(startedAtRaw.String)
			if parseErr == nil {
				st.StartedAt = &t
			}
		}
		if completedAtRaw.Valid {
			t, parseErr := parseTime(completedAtRaw.String)
			if parseErr == nil {
				st.CompletedAt = &t
			}
		}
		steps = append(steps, st)
	}
	return steps, rows.Err()
}

func (s *Store) GetStepByTaskID(ctx context.Context, taskID string) (*StepState, error) {
	query := `
		SELECT execution_id, step_id, status, started_at, completed_at,
		       input_data, output_data, error_message, attempt, task_id
		FROM workflow_step_states
		WHERE task_id = ?
	`
	var st StepState
	var errMsg sql.NullString
	var startedAtRaw, completedAtRaw sql.NullString
	err := s.db.QueryRowContext(ctx, query, taskID).Scan(
		&st.ExecutionID, &st.StepID, &st.Status, &startedAtRaw, &completedAtRaw,
		&st.InputData, &st.OutputData, &errMsg, &st.Attempt, &st.TaskID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get step by task ID: %w", err)
	}
	st.ErrorMessage = errMsg.String
	if startedAtRaw.Valid {
		t, parseErr := parseTime(startedAtRaw.String)
		if parseErr == nil {
			st.StartedAt = &t
		}
	}
	if completedAtRaw.Valid {
		t, parseErr := parseTime(completedAtRaw.String)
		if parseErr == nil {
			st.CompletedAt = &t
		}
	}
	return &st, nil
}

// =============================================================================
// Schedules — workflow_schedules table
// =============================================================================

func (s *Store) GetDueSchedules(ctx context.Context, now time.Time) ([]Schedule, error) {
	// No FOR UPDATE SKIP LOCKED — SQLite is single-writer; the scheduler
	// is the only consumer of due schedules in lite mode.
	query := `
		SELECT id, name, workspace, schedule_type, schedule_expr, action,
		       COALESCE(workflow_id, ''), enabled, next_fire_at, last_fired_at, miss_policy,
		       max_concurrent, COALESCE(active_task_id, ''),
		       created_at, updated_at
		FROM workflow_schedules
		WHERE enabled = 1 AND next_fire_at IS NOT NULL AND next_fire_at <= ?
		ORDER BY next_fire_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query, formatTime(now))
	if err != nil {
		return nil, fmt.Errorf("get due schedules: %w", err)
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func (s *Store) CreateSchedule(ctx context.Context, sc *Schedule) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_schedules (id, name, workspace, schedule_type, schedule_expr, action,
		                                workflow_id, enabled, next_fire_at, miss_policy, max_concurrent,
		                                created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?)
		RETURNING created_at, updated_at
	`
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query,
		sc.ID, sc.Name, sc.Workspace, sc.ScheduleType, sc.ScheduleExpr, sc.Action,
		sc.WorkflowID, boolToInt(sc.Enabled), formatTimePtr(sc.NextFireAt), sc.MissPolicy, sc.MaxConcurrent,
		now, now,
	).Scan(&createdAtStr, &updatedAtStr)
	if err != nil {
		return fmt.Errorf("create schedule: %w", err)
	}
	sc.CreatedAt, _ = parseTime(createdAtStr)
	sc.UpdatedAt, _ = parseTime(updatedAtStr)
	return nil
}

func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM workflow_schedules WHERE id = ?", id)
	return err
}

func (s *Store) ListSchedules(ctx context.Context, workspace string) ([]Schedule, error) {
	query := `
		SELECT id, name, workspace, schedule_type, schedule_expr, action,
		       COALESCE(workflow_id, ''), enabled, next_fire_at, last_fired_at, miss_policy,
		       max_concurrent, COALESCE(active_task_id, ''),
		       created_at, updated_at
		FROM workflow_schedules
		WHERE workspace = ? OR workspace = '*'
		ORDER BY name ASC
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func (s *Store) GetSchedule(ctx context.Context, id string) (*Schedule, error) {
	query := `
		SELECT id, name, workspace, schedule_type, schedule_expr, action,
		       COALESCE(workflow_id, ''), enabled, next_fire_at, last_fired_at, miss_policy,
		       max_concurrent, COALESCE(active_task_id, ''),
		       created_at, updated_at
		FROM workflow_schedules
		WHERE id = ?
	`
	rows, err := s.db.QueryContext(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("get schedule: %w", err)
	}
	defer rows.Close()

	schedules, err := scanSchedules(rows)
	if err != nil {
		return nil, err
	}
	if len(schedules) == 0 {
		return nil, nil
	}
	return &schedules[0], nil
}

func (s *Store) UpsertSchedule(ctx context.Context, sc *Schedule) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_schedules (id, name, workspace, schedule_type, schedule_expr, action,
		                                workflow_id, enabled, next_fire_at, miss_policy, max_concurrent,
		                                created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			workspace = EXCLUDED.workspace,
			schedule_type = EXCLUDED.schedule_type,
			schedule_expr = EXCLUDED.schedule_expr,
			action = EXCLUDED.action,
			workflow_id = EXCLUDED.workflow_id,
			enabled = EXCLUDED.enabled,
			miss_policy = EXCLUDED.miss_policy,
			max_concurrent = EXCLUDED.max_concurrent
		RETURNING created_at, updated_at
	`
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query,
		sc.ID, sc.Name, sc.Workspace, sc.ScheduleType, sc.ScheduleExpr, sc.Action,
		sc.WorkflowID, boolToInt(sc.Enabled), formatTimePtr(sc.NextFireAt), sc.MissPolicy, sc.MaxConcurrent,
		now, now,
	).Scan(&createdAtStr, &updatedAtStr)
	if err != nil {
		return fmt.Errorf("upsert schedule: %w", err)
	}
	sc.CreatedAt, _ = parseTime(createdAtStr)
	sc.UpdatedAt, _ = parseTime(updatedAtStr)
	return nil
}

func (s *Store) UpdateScheduleAfterFire(ctx context.Context, id string, lastFired time.Time, nextFire *time.Time) error {
	query := `UPDATE workflow_schedules SET last_fired_at = ?, next_fire_at = ? WHERE id = ?`
	_, err := s.db.ExecContext(ctx, query, formatTime(lastFired), formatTimePtr(nextFire), id)
	return err
}

func (s *Store) SetScheduleActiveTask(ctx context.Context, scheduleID, taskID string) error {
	query := `UPDATE workflow_schedules SET active_task_id = NULLIF(?, '') WHERE id = ?`
	_, err := s.db.ExecContext(ctx, query, taskID, scheduleID)
	return err
}

// scanSchedules scans multiple schedule rows. Handles inline time.Time
// parsing for all timestamp columns including the §15.4 trap: next_fire_at
// is a *time.Time that may be NULL.
func scanSchedules(rows *sql.Rows) ([]Schedule, error) {
	var schedules []Schedule
	for rows.Next() {
		var sc Schedule
		var enabledInt int
		var nextFireAtRaw, lastFiredAtRaw sql.NullString
		var createdAtStr, updatedAtStr string
		if err := rows.Scan(
			&sc.ID, &sc.Name, &sc.Workspace, &sc.ScheduleType, &sc.ScheduleExpr, &sc.Action,
			&sc.WorkflowID, &enabledInt, &nextFireAtRaw, &lastFiredAtRaw, &sc.MissPolicy,
			&sc.MaxConcurrent, &sc.ActiveTaskID,
			&createdAtStr, &updatedAtStr,
		); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		sc.Enabled = enabledInt != 0
		sc.CreatedAt, _ = parseTime(createdAtStr)
		sc.UpdatedAt, _ = parseTime(updatedAtStr)
		// §15.4 trap: next_fire_at is *time.Time. Parse from TEXT carefully.
		if nextFireAtRaw.Valid {
			t, parseErr := parseTime(nextFireAtRaw.String)
			if parseErr == nil {
				sc.NextFireAt = &t
			}
		}
		if lastFiredAtRaw.Valid {
			t, parseErr := parseTime(lastFiredAtRaw.String)
			if parseErr == nil {
				sc.LastFiredAt = &t
			}
		}
		schedules = append(schedules, sc)
	}
	return schedules, rows.Err()
}

// =============================================================================
// State machines — workflow_state_machines + workflow_state_machine_instances
// =============================================================================

func (s *Store) CreateStateMachine(ctx context.Context, sm *StateMachineDef) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_state_machines (id, workspace, definition, active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		RETURNING created_at, updated_at
	`
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query, sm.ID, sm.Workspace, sm.Definition, boolToInt(sm.Active), now, now).
		Scan(&createdAtStr, &updatedAtStr)
	if err != nil {
		return fmt.Errorf("create state machine: %w", err)
	}
	sm.CreatedAt, _ = parseTime(createdAtStr)
	sm.UpdatedAt, _ = parseTime(updatedAtStr)
	return nil
}

func (s *Store) GetStateMachine(ctx context.Context, id string) (*StateMachineDef, error) {
	query := `
		SELECT id, workspace, definition, active, created_at, updated_at
		FROM workflow_state_machines
		WHERE id = ? AND active = 1
	`
	var sm StateMachineDef
	var activeInt int
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&sm.ID, &sm.Workspace, &sm.Definition, &activeInt, &createdAtStr, &updatedAtStr,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get state machine: %w", err)
	}
	sm.Active = activeInt != 0
	sm.CreatedAt, _ = parseTime(createdAtStr)
	sm.UpdatedAt, _ = parseTime(updatedAtStr)
	return &sm, nil
}

func (s *Store) ListStateMachines(ctx context.Context, workspace string) ([]StateMachineDef, error) {
	query := `
		SELECT id, workspace, definition, active, created_at, updated_at
		FROM workflow_state_machines
		WHERE active = 1 AND (workspace = ? OR workspace = '*')
		ORDER BY id
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("list state machines: %w", err)
	}
	defer rows.Close()

	var machines []StateMachineDef
	for rows.Next() {
		var sm StateMachineDef
		var activeInt int
		var createdAtStr, updatedAtStr string
		if err := rows.Scan(&sm.ID, &sm.Workspace, &sm.Definition, &activeInt, &createdAtStr, &updatedAtStr); err != nil {
			return nil, fmt.Errorf("scan state machine: %w", err)
		}
		sm.Active = activeInt != 0
		sm.CreatedAt, _ = parseTime(createdAtStr)
		sm.UpdatedAt, _ = parseTime(updatedAtStr)
		machines = append(machines, sm)
	}
	return machines, rows.Err()
}

func (s *Store) DeactivateStateMachine(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE workflow_state_machines SET active = 0 WHERE id = ?", id)
	return err
}

func (s *Store) CreateStateMachineInstance(ctx context.Context, inst *StateMachineInstance) error {
	now := nowUTC()
	query := `
		INSERT INTO workflow_state_machine_instances (instance_id, machine_id, workspace, current_state, data, timeout_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING created_at, updated_at
	`
	var createdAtStr, updatedAtStr string
	err := s.db.QueryRowContext(ctx, query,
		inst.InstanceID, inst.MachineID, inst.Workspace, inst.CurrentState, inst.Data, formatTimePtr(inst.TimeoutAt), now, now,
	).Scan(&createdAtStr, &updatedAtStr)
	if err != nil {
		return fmt.Errorf("create state machine instance: %w", err)
	}
	inst.CreatedAt, _ = parseTime(createdAtStr)
	inst.UpdatedAt, _ = parseTime(updatedAtStr)
	return nil
}

func (s *Store) GetStateMachineInstance(ctx context.Context, instanceID string) (*StateMachineInstance, error) {
	query := `
		SELECT instance_id, machine_id, workspace, current_state, data,
		       created_at, updated_at, completed_at, timeout_at
		FROM workflow_state_machine_instances
		WHERE instance_id = ?
	`
	var inst StateMachineInstance
	var createdAtStr, updatedAtStr string
	var completedAtRaw, timeoutAtRaw sql.NullString
	err := s.db.QueryRowContext(ctx, query, instanceID).Scan(
		&inst.InstanceID, &inst.MachineID, &inst.Workspace, &inst.CurrentState, &inst.Data,
		&createdAtStr, &updatedAtStr, &completedAtRaw, &timeoutAtRaw,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get state machine instance: %w", err)
	}
	inst.CreatedAt, _ = parseTime(createdAtStr)
	inst.UpdatedAt, _ = parseTime(updatedAtStr)
	if completedAtRaw.Valid {
		t, parseErr := parseTime(completedAtRaw.String)
		if parseErr == nil {
			inst.CompletedAt = &t
		}
	}
	if timeoutAtRaw.Valid {
		t, parseErr := parseTime(timeoutAtRaw.String)
		if parseErr == nil {
			inst.TimeoutAt = &t
		}
	}
	return &inst, nil
}

func (s *Store) ListStateMachineInstances(ctx context.Context, machineID string) ([]StateMachineInstance, error) {
	query := `
		SELECT instance_id, machine_id, workspace, current_state, data,
		       created_at, updated_at, completed_at, timeout_at
		FROM workflow_state_machine_instances
		WHERE machine_id = ?
		ORDER BY created_at DESC
		LIMIT 100
	`
	rows, err := s.db.QueryContext(ctx, query, machineID)
	if err != nil {
		return nil, fmt.Errorf("list state machine instances: %w", err)
	}
	defer rows.Close()
	return scanStateMachineInstances(rows)
}

func (s *Store) UpdateStateMachineInstance(ctx context.Context, instanceID, newState string, timeoutAt *time.Time, completed bool) error {
	if completed {
		now := nowUTC()
		query := `UPDATE workflow_state_machine_instances SET current_state = ?, timeout_at = ?, completed_at = ? WHERE instance_id = ?`
		_, err := s.db.ExecContext(ctx, query, newState, formatTimePtr(timeoutAt), now, instanceID)
		return err
	}
	query := `UPDATE workflow_state_machine_instances SET current_state = ?, timeout_at = ? WHERE instance_id = ?`
	_, err := s.db.ExecContext(ctx, query, newState, formatTimePtr(timeoutAt), instanceID)
	return err
}

func (s *Store) GetTimedOutInstances(ctx context.Context, now time.Time) ([]StateMachineInstance, error) {
	query := `
		SELECT instance_id, machine_id, workspace, current_state, data,
		       created_at, updated_at, completed_at, timeout_at
		FROM workflow_state_machine_instances
		WHERE timeout_at IS NOT NULL AND timeout_at <= ? AND completed_at IS NULL
		ORDER BY timeout_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query, formatTime(now))
	if err != nil {
		return nil, fmt.Errorf("get timed out instances: %w", err)
	}
	defer rows.Close()
	return scanStateMachineInstances(rows)
}

func (s *Store) ClearInstanceTimeout(ctx context.Context, instanceID string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE workflow_state_machine_instances SET timeout_at = NULL WHERE instance_id = ?", instanceID)
	return err
}

// scanStateMachineInstances scans multiple state-machine instance rows.
func scanStateMachineInstances(rows *sql.Rows) ([]StateMachineInstance, error) {
	var instances []StateMachineInstance
	for rows.Next() {
		var inst StateMachineInstance
		var createdAtStr, updatedAtStr string
		var completedAtRaw, timeoutAtRaw sql.NullString
		if err := rows.Scan(
			&inst.InstanceID, &inst.MachineID, &inst.Workspace, &inst.CurrentState, &inst.Data,
			&createdAtStr, &updatedAtStr, &completedAtRaw, &timeoutAtRaw,
		); err != nil {
			return nil, fmt.Errorf("scan state machine instance: %w", err)
		}
		inst.CreatedAt, _ = parseTime(createdAtStr)
		inst.UpdatedAt, _ = parseTime(updatedAtStr)
		if completedAtRaw.Valid {
			t, parseErr := parseTime(completedAtRaw.String)
			if parseErr == nil {
				inst.CompletedAt = &t
			}
		}
		if timeoutAtRaw.Valid {
			t, parseErr := parseTime(timeoutAtRaw.String)
			if parseErr == nil {
				inst.TimeoutAt = &t
			}
		}
		instances = append(instances, inst)
	}
	return instances, rows.Err()
}

// =============================================================================
// Migration runner — self-contained per §6 of the Stage 0 decisions doc.
// Mirrors the pattern in internal/workflow/migrations/sqlite/runner.go but
// is independent (separate schema_migrations table prefix, separate embed.FS).
// =============================================================================

func runMigrations(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS workflow_native_schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
		)
	`)
	if err != nil {
		return fmt.Errorf("create workflow_native_schema_migrations table: %w", err)
	}

	entries, err := migrations.MigrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read workflow sqlite_workflow migrations dir: %w", err)
	}

	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	applied := make(map[string]bool)
	rows, err := db.QueryContext(ctx, "SELECT version FROM workflow_native_schema_migrations")
	if err != nil {
		return fmt.Errorf("query applied workflow native migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("scan workflow native migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating workflow native migrations: %w", err)
	}

	appliedCount := 0
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		if applied[version] {
			continue
		}

		log.Debug().Str("version", version).Msg("applying workflow native sqlite migration")

		content, err := migrations.MigrationFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("read workflow native migration %s: %w", filename, err)
		}

		stmts := splitStatements(string(content))
		for _, stmt := range stmts {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				if isDuplicateColumnError(err) {
					log.Debug().Str("version", version).Msg("skipping duplicate column (prior partial migration)")
					continue
				}
				return fmt.Errorf("execute workflow native migration %s: %w", filename, err)
			}
		}

		if _, err := db.ExecContext(ctx, "INSERT INTO workflow_native_schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("record workflow native migration %s: %w", filename, err)
		}

		log.Debug().Str("version", version).Msg("workflow native sqlite migration applied")
		appliedCount++
	}

	if appliedCount == 0 {
		log.Debug().Msg("all workflow native sqlite migrations already applied")
	} else {
		log.Info().Int("count", appliedCount).Msg("workflow native sqlite migrations applied")
	}

	return nil
}

// splitStatements splits SQL text on semicolons into individual statements,
// skipping empty/comment-only fragments. Unlike the simpler splitter in
// internal/workflow/migrations/sqlite/runner.go, this version is aware of
// BEGIN...END blocks (used by CREATE TRIGGER) so it doesn't split on
// semicolons inside trigger bodies.
func splitStatements(sqlText string) []string {
	var out []string
	var current strings.Builder
	depth := 0 // tracks nested BEGIN...END

	lines := strings.Split(sqlText, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip pure comment lines for depth tracking but still append them.
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			current.WriteString(line)
			current.WriteString("\n")
			continue
		}

		upper := strings.ToUpper(trimmed)

		// Track BEGIN...END depth for trigger bodies.
		if upper == "BEGIN" || strings.HasPrefix(upper, "BEGIN ") || strings.HasSuffix(upper, " BEGIN") {
			depth++
		}

		current.WriteString(line)
		current.WriteString("\n")

		// Check if line ends with END; (closing a trigger) — depth decrease
		// happens when we see the END keyword on its own line or "END;"
		if (upper == "END;" || upper == "END") && depth > 0 {
			depth--
			if depth == 0 {
				// Flush the complete trigger statement.
				stmt := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(current.String()), ";"))
				if stmt != "" {
					out = append(out, stmt)
				}
				current.Reset()
			}
			continue
		}

		// Outside a BEGIN block, semicolons terminate statements.
		if depth == 0 && strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			// Remove trailing semicolon for exec.
			stmt = strings.TrimSuffix(stmt, ";")
			stmt = strings.TrimSpace(stmt)
			if stmt != "" && hasCode(stmt) {
				out = append(out, stmt)
			}
			current.Reset()
		}
	}

	// Flush any remaining content.
	remaining := strings.TrimSpace(current.String())
	remaining = strings.TrimSuffix(remaining, ";")
	remaining = strings.TrimSpace(remaining)
	if remaining != "" && hasCode(remaining) {
		out = append(out, remaining)
	}

	return out
}

// hasCode returns true if the string contains at least one non-comment,
// non-empty line.
func hasCode(s string) bool {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return true
		}
	}
	return false
}

// isDuplicateColumnError checks if a SQLite error is a "duplicate column name" error.
func isDuplicateColumnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
}

// =============================================================================
// Helpers
// =============================================================================

// boolToInt converts a Go bool to a SQLite INTEGER (0/1).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Type aliases for the workflow domain types, imported from the parent
// package so this impl file doesn't need a separate import of the legacy
// internal/workflow package.
type (
	Rule                 = workflow.Rule
	WorkflowDefinition   = workflow.WorkflowDefinition
	WorkflowExecution    = workflow.WorkflowExecution
	StepState            = workflow.StepState
	Schedule             = workflow.Schedule
	StateMachineDef      = workflow.StateMachineDef
	StateMachineInstance = workflow.StateMachineInstance
)
