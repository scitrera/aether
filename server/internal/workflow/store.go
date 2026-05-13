package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Store provides database operations for all workflow tables.
// Supports both PostgreSQL and SQLite backends.
type Store struct {
	db       *sql.DB
	isSQLite bool
}

func NewStore(db *sql.DB, isSQLite bool) *Store {
	return &Store{db: db, isSQLite: isSQLite}
}

// =============================================================================
// Rule types and operations
// =============================================================================

type Rule struct {
	ID                  int
	RuleName            string
	SourceAgent         string
	SourceEvent         string
	TriggerCondition    string
	TransformationStyle string
	DestinationTemplate string
	Workspace           string
	Priority            int
	Active              bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// GetMatchingRules returns active rules matching the given source agent and event.
// Wildcard '*' source_agent rules are also included.
func (s *Store) GetMatchingRules(ctx context.Context, sourceAgent, sourceEvent, workspace string) ([]Rule, error) {
	query := `
		SELECT id, rule_name, source_agent, source_event, trigger_condition,
		       transformation_style, destination_template, workspace, priority, active,
		       created_at, updated_at
		FROM workflow_rules
		WHERE active = true
		  AND source_event = $1
		  AND (source_agent = $2 OR source_agent = '*')
		  AND (workspace = $3 OR workspace = '*')
		ORDER BY priority DESC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, query, sourceEvent, sourceAgent, workspace)
	if err != nil {
		return nil, fmt.Errorf("query matching rules: %w", err)
	}
	defer rows.Close()

	var rules []Rule
	for rows.Next() {
		var r Rule
		if err := rows.Scan(
			&r.ID, &r.RuleName, &r.SourceAgent, &r.SourceEvent, &r.TriggerCondition,
			&r.TransformationStyle, &r.DestinationTemplate, &r.Workspace, &r.Priority, &r.Active,
			&r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan rule: %w", err)
		}
		rules = append(rules, r)
	}
	return rules, rows.Err()
}

func (s *Store) CreateRule(ctx context.Context, r *Rule) error {
	query := `
		INSERT INTO workflow_rules (rule_name, source_agent, source_event, trigger_condition,
		                            transformation_style, destination_template, workspace, priority, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, created_at, updated_at
	`
	return s.db.QueryRowContext(ctx, query,
		r.RuleName, r.SourceAgent, r.SourceEvent, r.TriggerCondition,
		r.TransformationStyle, r.DestinationTemplate, r.Workspace, r.Priority, r.Active,
	).Scan(&r.ID, &r.CreatedAt, &r.UpdatedAt)
}

func (s *Store) UpdateRule(ctx context.Context, r *Rule) error {
	query := `
		UPDATE workflow_rules
		SET rule_name = $2, source_agent = $3, source_event = $4, trigger_condition = $5,
		    transformation_style = $6, destination_template = $7, workspace = $8,
		    priority = $9, active = $10
		WHERE id = $1
	`
	_, err := s.db.ExecContext(ctx, query,
		r.ID, r.RuleName, r.SourceAgent, r.SourceEvent, r.TriggerCondition,
		r.TransformationStyle, r.DestinationTemplate, r.Workspace, r.Priority, r.Active,
	)
	return err
}

func (s *Store) DeleteRule(ctx context.Context, id int) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM workflow_rules WHERE id = $1", id)
	return err
}

func (s *Store) ListRules(ctx context.Context, workspace string) ([]Rule, error) {
	query := `
		SELECT id, rule_name, source_agent, source_event, trigger_condition,
		       transformation_style, destination_template, workspace, priority, active,
		       created_at, updated_at
		FROM workflow_rules
		WHERE workspace = $1 OR workspace = '*'
		ORDER BY priority DESC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	defer rows.Close()

	var rules []Rule
	for rows.Next() {
		var r Rule
		if err := rows.Scan(
			&r.ID, &r.RuleName, &r.SourceAgent, &r.SourceEvent, &r.TriggerCondition,
			&r.TransformationStyle, &r.DestinationTemplate, &r.Workspace, &r.Priority, &r.Active,
			&r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan rule: %w", err)
		}
		rules = append(rules, r)
	}
	return rules, rows.Err()
}

// =============================================================================
// Workflow Definition types and operations
// =============================================================================

type WorkflowDefinition struct {
	ID         string
	Version    int
	Workspace  string
	Definition json.RawMessage
	Active     bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (s *Store) GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error) {
	query := `
		SELECT id, version, workspace, definition, active, created_at, updated_at
		FROM workflow_definitions
		WHERE id = $1 AND active = true
		ORDER BY version DESC
		LIMIT 1
	`
	var d WorkflowDefinition
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&d.ID, &d.Version, &d.Workspace, &d.Definition, &d.Active, &d.CreatedAt, &d.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	return &d, nil
}

func (s *Store) CreateWorkflowDefinition(ctx context.Context, d *WorkflowDefinition) error {
	query := `
		INSERT INTO workflow_definitions (id, version, workspace, definition, active)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING created_at, updated_at
	`
	return s.db.QueryRowContext(ctx, query,
		d.ID, d.Version, d.Workspace, d.Definition, d.Active,
	).Scan(&d.CreatedAt, &d.UpdatedAt)
}

// GetWorkflowDefinitionsForTrigger finds active definitions matching an event trigger.
func (s *Store) GetWorkflowDefinitionsForTrigger(ctx context.Context, workspace string) ([]WorkflowDefinition, error) {
	query := `
		SELECT DISTINCT ON (id) id, version, workspace, definition, active, created_at, updated_at
		FROM workflow_definitions
		WHERE active = true AND (workspace = $1 OR workspace = '*')
		ORDER BY id, version DESC
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("get workflow definitions for trigger: %w", err)
	}
	defer rows.Close()

	var defs []WorkflowDefinition
	for rows.Next() {
		var d WorkflowDefinition
		if err := rows.Scan(&d.ID, &d.Version, &d.Workspace, &d.Definition, &d.Active, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan workflow definition: %w", err)
		}
		defs = append(defs, d)
	}
	return defs, rows.Err()
}

// =============================================================================
// Execution types and operations
// =============================================================================

type WorkflowExecution struct {
	ExecutionID     string
	WorkflowID      string
	WorkflowVersion int
	Workspace       string
	Status          string
	TriggerData     json.RawMessage
	StartedAt       time.Time
	CompletedAt     *time.Time
	ErrorMessage    string
	Metadata        json.RawMessage
}

const (
	ExecStatusRunning   = "running"
	ExecStatusCompleted = "completed"
	ExecStatusFailed    = "failed"
	ExecStatusCancelled = "cancelled"
)

func (s *Store) CreateExecution(ctx context.Context, e *WorkflowExecution) error {
	query := `
		INSERT INTO workflow_executions (execution_id, workflow_id, workflow_version, workspace, status, trigger_data, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING started_at
	`
	return s.db.QueryRowContext(ctx, query,
		e.ExecutionID, e.WorkflowID, e.WorkflowVersion, e.Workspace, e.Status, e.TriggerData, e.Metadata,
	).Scan(&e.StartedAt)
}

func (s *Store) GetExecution(ctx context.Context, executionID string) (*WorkflowExecution, error) {
	query := `
		SELECT execution_id, workflow_id, workflow_version, workspace, status,
		       trigger_data, started_at, completed_at, error_message, metadata
		FROM workflow_executions
		WHERE execution_id = $1
	`
	var e WorkflowExecution
	var errMsg sql.NullString
	err := s.db.QueryRowContext(ctx, query, executionID).Scan(
		&e.ExecutionID, &e.WorkflowID, &e.WorkflowVersion, &e.Workspace, &e.Status,
		&e.TriggerData, &e.StartedAt, &e.CompletedAt, &errMsg, &e.Metadata,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get execution: %w", err)
	}
	e.ErrorMessage = errMsg.String
	return &e, nil
}

func (s *Store) UpdateExecutionStatus(ctx context.Context, executionID, status, errorMessage string) error {
	var query string
	if status == ExecStatusCompleted || status == ExecStatusFailed || status == ExecStatusCancelled {
		query = `UPDATE workflow_executions SET status = $2, error_message = $3, completed_at = NOW() WHERE execution_id = $1`
	} else {
		query = `UPDATE workflow_executions SET status = $2, error_message = $3 WHERE execution_id = $1`
	}
	_, err := s.db.ExecContext(ctx, query, executionID, status, errorMessage)
	return err
}

// GetRunningExecutions returns all currently running executions.
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

	var execs []WorkflowExecution
	for rows.Next() {
		var e WorkflowExecution
		var errMsg sql.NullString
		if err := rows.Scan(
			&e.ExecutionID, &e.WorkflowID, &e.WorkflowVersion, &e.Workspace, &e.Status,
			&e.TriggerData, &e.StartedAt, &e.CompletedAt, &errMsg, &e.Metadata,
		); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		e.ErrorMessage = errMsg.String
		execs = append(execs, e)
	}
	return execs, rows.Err()
}

func (s *Store) CountRunningExecutions(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_executions WHERE status = 'running'").Scan(&count)
	return count, err
}

// =============================================================================
// Step State types and operations
// =============================================================================

type StepState struct {
	ExecutionID  string
	StepID       string
	Status       string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	InputData    json.RawMessage
	OutputData   json.RawMessage
	ErrorMessage string
	Attempt      int
	TaskID       string
}

const (
	StepStatusPending   = "pending"
	StepStatusRunning   = "running"
	StepStatusCompleted = "completed"
	StepStatusFailed    = "failed"
	StepStatusSkipped   = "skipped"
)

func (s *Store) CreateStepState(ctx context.Context, st *StepState) error {
	query := `
		INSERT INTO workflow_step_states (execution_id, step_id, status, input_data, attempt)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := s.db.ExecContext(ctx, query,
		st.ExecutionID, st.StepID, st.Status, st.InputData, st.Attempt,
	)
	return err
}

func (s *Store) UpdateStepStatus(ctx context.Context, executionID, stepID, status string) error {
	var query string
	switch status {
	case StepStatusRunning:
		query = `UPDATE workflow_step_states SET status = $3, started_at = NOW() WHERE execution_id = $1 AND step_id = $2`
	case StepStatusCompleted, StepStatusFailed, StepStatusSkipped:
		query = `UPDATE workflow_step_states SET status = $3, completed_at = NOW() WHERE execution_id = $1 AND step_id = $2`
	default:
		query = `UPDATE workflow_step_states SET status = $3 WHERE execution_id = $1 AND step_id = $2`
	}
	_, err := s.db.ExecContext(ctx, query, executionID, stepID, status)
	return err
}

func (s *Store) SetStepOutput(ctx context.Context, executionID, stepID string, output json.RawMessage) error {
	query := `UPDATE workflow_step_states SET output_data = $3, status = 'completed', completed_at = NOW() WHERE execution_id = $1 AND step_id = $2`
	_, err := s.db.ExecContext(ctx, query, executionID, stepID, output)
	return err
}

func (s *Store) SetStepError(ctx context.Context, executionID, stepID, errorMessage string) error {
	query := `UPDATE workflow_step_states SET error_message = $3, status = 'failed', completed_at = NOW() WHERE execution_id = $1 AND step_id = $2`
	_, err := s.db.ExecContext(ctx, query, executionID, stepID, errorMessage)
	return err
}

func (s *Store) SetStepTaskID(ctx context.Context, executionID, stepID, taskID string) error {
	query := `UPDATE workflow_step_states SET task_id = $3 WHERE execution_id = $1 AND step_id = $2`
	_, err := s.db.ExecContext(ctx, query, executionID, stepID, taskID)
	return err
}

func (s *Store) IncrementStepAttempt(ctx context.Context, executionID, stepID string) error {
	query := `UPDATE workflow_step_states SET attempt = attempt + 1, status = 'pending', started_at = NULL, completed_at = NULL, error_message = NULL WHERE execution_id = $1 AND step_id = $2`
	_, err := s.db.ExecContext(ctx, query, executionID, stepID)
	return err
}

func (s *Store) GetStepStates(ctx context.Context, executionID string) ([]StepState, error) {
	query := `
		SELECT execution_id, step_id, status, started_at, completed_at,
		       input_data, output_data, error_message, attempt, COALESCE(task_id, '')
		FROM workflow_step_states
		WHERE execution_id = $1
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
		if err := rows.Scan(
			&st.ExecutionID, &st.StepID, &st.Status, &st.StartedAt, &st.CompletedAt,
			&st.InputData, &st.OutputData, &errMsg, &st.Attempt, &st.TaskID,
		); err != nil {
			return nil, fmt.Errorf("scan step state: %w", err)
		}
		st.ErrorMessage = errMsg.String
		steps = append(steps, st)
	}
	return steps, rows.Err()
}

func (s *Store) GetStepByTaskID(ctx context.Context, taskID string) (*StepState, error) {
	query := `
		SELECT execution_id, step_id, status, started_at, completed_at,
		       input_data, output_data, error_message, attempt, task_id
		FROM workflow_step_states
		WHERE task_id = $1
	`
	var st StepState
	var errMsg sql.NullString
	err := s.db.QueryRowContext(ctx, query, taskID).Scan(
		&st.ExecutionID, &st.StepID, &st.Status, &st.StartedAt, &st.CompletedAt,
		&st.InputData, &st.OutputData, &errMsg, &st.Attempt, &st.TaskID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get step by task ID: %w", err)
	}
	st.ErrorMessage = errMsg.String
	return &st, nil
}

// =============================================================================
// Schedule types and operations
// =============================================================================

type Schedule struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Workspace     string          `json:"workspace"`
	ScheduleType  string          `json:"schedule_type"`
	ScheduleExpr  string          `json:"schedule_expr"`
	Action        json.RawMessage `json:"action"`
	WorkflowID    string          `json:"workflow_id"`
	Enabled       bool            `json:"enabled"`
	NextFireAt    *time.Time      `json:"next_fire_at,omitempty"`
	LastFiredAt   *time.Time      `json:"last_fired_at,omitempty"`
	MissPolicy    string          `json:"miss_policy"`
	MaxConcurrent int             `json:"max_concurrent"` // 0 = unlimited; 1 = don't fire if previous still running
	ActiveTaskID  string          `json:"active_task_id"` // Tracks currently running task (for max_concurrent=1)
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

const (
	ScheduleTypeCron         = "cron"
	ScheduleTypeInterval     = "interval"
	ScheduleTypeOnce         = "once"
	ScheduleTypeEventDelayed = "event_delayed"
)

func (s *Store) GetDueSchedules(ctx context.Context, now time.Time) ([]Schedule, error) {
	query := `
		SELECT id, name, workspace, schedule_type, schedule_expr, action,
		       COALESCE(workflow_id, ''), enabled, next_fire_at, last_fired_at, miss_policy,
		       max_concurrent, COALESCE(active_task_id, ''),
		       created_at, updated_at
		FROM workflow_schedules
		WHERE enabled = true AND next_fire_at IS NOT NULL AND next_fire_at <= $1
		ORDER BY next_fire_at ASC
	`
	if !s.isSQLite {
		query += " FOR UPDATE SKIP LOCKED"
	}
	rows, err := s.db.QueryContext(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("get due schedules: %w", err)
	}
	defer rows.Close()

	var schedules []Schedule
	for rows.Next() {
		var sc Schedule
		if err := rows.Scan(
			&sc.ID, &sc.Name, &sc.Workspace, &sc.ScheduleType, &sc.ScheduleExpr, &sc.Action,
			&sc.WorkflowID, &sc.Enabled, &sc.NextFireAt, &sc.LastFiredAt, &sc.MissPolicy,
			&sc.MaxConcurrent, &sc.ActiveTaskID,
			&sc.CreatedAt, &sc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		schedules = append(schedules, sc)
	}
	return schedules, rows.Err()
}

func (s *Store) UpdateScheduleAfterFire(ctx context.Context, id string, lastFired time.Time, nextFire *time.Time) error {
	query := `UPDATE workflow_schedules SET last_fired_at = $2, next_fire_at = $3 WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id, lastFired, nextFire)
	return err
}

func (s *Store) CreateSchedule(ctx context.Context, sc *Schedule) error {
	query := `
		INSERT INTO workflow_schedules (id, name, workspace, schedule_type, schedule_expr, action,
		                                workflow_id, enabled, next_fire_at, miss_policy, max_concurrent)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, $11)
		RETURNING created_at, updated_at
	`
	return s.db.QueryRowContext(ctx, query,
		sc.ID, sc.Name, sc.Workspace, sc.ScheduleType, sc.ScheduleExpr, sc.Action,
		sc.WorkflowID, sc.Enabled, sc.NextFireAt, sc.MissPolicy, sc.MaxConcurrent,
	).Scan(&sc.CreatedAt, &sc.UpdatedAt)
}

func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM workflow_schedules WHERE id = $1", id)
	return err
}

func (s *Store) ListSchedules(ctx context.Context, workspace string) ([]Schedule, error) {
	query := `
		SELECT id, name, workspace, schedule_type, schedule_expr, action,
		       COALESCE(workflow_id, ''), enabled, next_fire_at, last_fired_at, miss_policy,
		       max_concurrent, COALESCE(active_task_id, ''),
		       created_at, updated_at
		FROM workflow_schedules
		WHERE workspace = $1 OR workspace = '*'
		ORDER BY name ASC
	`
	rows, err := s.db.QueryContext(ctx, query, workspace)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()

	var schedules []Schedule
	for rows.Next() {
		var sc Schedule
		if err := rows.Scan(
			&sc.ID, &sc.Name, &sc.Workspace, &sc.ScheduleType, &sc.ScheduleExpr, &sc.Action,
			&sc.WorkflowID, &sc.Enabled, &sc.NextFireAt, &sc.LastFiredAt, &sc.MissPolicy,
			&sc.MaxConcurrent, &sc.ActiveTaskID,
			&sc.CreatedAt, &sc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		schedules = append(schedules, sc)
	}
	return schedules, rows.Err()
}

func (s *Store) GetSchedule(ctx context.Context, id string) (*Schedule, error) {
	query := `
		SELECT id, name, workspace, schedule_type, schedule_expr, action,
		       COALESCE(workflow_id, ''), enabled, next_fire_at, last_fired_at, miss_policy,
		       max_concurrent, COALESCE(active_task_id, ''),
		       created_at, updated_at
		FROM workflow_schedules
		WHERE id = $1
	`
	var sc Schedule
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&sc.ID, &sc.Name, &sc.Workspace, &sc.ScheduleType, &sc.ScheduleExpr, &sc.Action,
		&sc.WorkflowID, &sc.Enabled, &sc.NextFireAt, &sc.LastFiredAt, &sc.MissPolicy,
		&sc.MaxConcurrent, &sc.ActiveTaskID,
		&sc.CreatedAt, &sc.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get schedule: %w", err)
	}
	return &sc, nil
}

func (s *Store) UpsertSchedule(ctx context.Context, sc *Schedule) error {
	query := `
		INSERT INTO workflow_schedules (id, name, workspace, schedule_type, schedule_expr, action,
		                                workflow_id, enabled, next_fire_at, miss_policy, max_concurrent)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, $11)
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
	return s.db.QueryRowContext(ctx, query,
		sc.ID, sc.Name, sc.Workspace, sc.ScheduleType, sc.ScheduleExpr, sc.Action,
		sc.WorkflowID, sc.Enabled, sc.NextFireAt, sc.MissPolicy, sc.MaxConcurrent,
	).Scan(&sc.CreatedAt, &sc.UpdatedAt)
}

func (s *Store) SetScheduleActiveTask(ctx context.Context, scheduleID, taskID string) error {
	query := `UPDATE workflow_schedules SET active_task_id = NULLIF($2, '') WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, scheduleID, taskID)
	return err
}

// =============================================================================
// Additional query methods for Admin API
// =============================================================================

func (s *Store) GetRule(ctx context.Context, id int) (*Rule, error) {
	query := `
		SELECT id, rule_name, source_agent, source_event, trigger_condition,
		       transformation_style, destination_template, workspace, priority, active,
		       created_at, updated_at
		FROM workflow_rules
		WHERE id = $1
	`
	var r Rule
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&r.ID, &r.RuleName, &r.SourceAgent, &r.SourceEvent, &r.TriggerCondition,
		&r.TransformationStyle, &r.DestinationTemplate, &r.Workspace, &r.Priority, &r.Active,
		&r.CreatedAt, &r.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rule: %w", err)
	}
	return &r, nil
}

func (s *Store) DeactivateWorkflowDefinition(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE workflow_definitions SET active = false WHERE id = $1", id)
	return err
}

func (s *Store) GetExecutionsByStatus(ctx context.Context, status string) ([]WorkflowExecution, error) {
	query := `
		SELECT execution_id, workflow_id, workflow_version, workspace, status,
		       trigger_data, started_at, completed_at, error_message, metadata
		FROM workflow_executions
		WHERE status = $1
		ORDER BY started_at DESC
		LIMIT 100
	`
	rows, err := s.db.QueryContext(ctx, query, status)
	if err != nil {
		return nil, fmt.Errorf("get executions by status: %w", err)
	}
	defer rows.Close()

	var execs []WorkflowExecution
	for rows.Next() {
		var e WorkflowExecution
		var errMsg sql.NullString
		if err := rows.Scan(
			&e.ExecutionID, &e.WorkflowID, &e.WorkflowVersion, &e.Workspace, &e.Status,
			&e.TriggerData, &e.StartedAt, &e.CompletedAt, &errMsg, &e.Metadata,
		); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		e.ErrorMessage = errMsg.String
		execs = append(execs, e)
	}
	return execs, rows.Err()
}

// =============================================================================
// State Machine types and operations
// =============================================================================

type StateMachineDef struct {
	ID         string          `json:"id"`
	Workspace  string          `json:"workspace"`
	Definition json.RawMessage `json:"definition"`
	Active     bool            `json:"active"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type StateMachineInstance struct {
	InstanceID   string          `json:"instance_id"`
	MachineID    string          `json:"machine_id"`
	Workspace    string          `json:"workspace"`
	CurrentState string          `json:"current_state"`
	Data         json.RawMessage `json:"data"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	CompletedAt  *time.Time      `json:"completed_at"`
	TimeoutAt    *time.Time      `json:"timeout_at"`
}

func (s *Store) CreateStateMachine(ctx context.Context, sm *StateMachineDef) error {
	query := `
		INSERT INTO workflow_state_machines (id, workspace, definition, active)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at, updated_at
	`
	return s.db.QueryRowContext(ctx, query, sm.ID, sm.Workspace, sm.Definition, sm.Active).
		Scan(&sm.CreatedAt, &sm.UpdatedAt)
}

func (s *Store) GetStateMachine(ctx context.Context, id string) (*StateMachineDef, error) {
	query := `
		SELECT id, workspace, definition, active, created_at, updated_at
		FROM workflow_state_machines
		WHERE id = $1 AND active = true
	`
	var sm StateMachineDef
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&sm.ID, &sm.Workspace, &sm.Definition, &sm.Active, &sm.CreatedAt, &sm.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get state machine: %w", err)
	}
	return &sm, nil
}

func (s *Store) ListStateMachines(ctx context.Context, workspace string) ([]StateMachineDef, error) {
	query := `
		SELECT id, workspace, definition, active, created_at, updated_at
		FROM workflow_state_machines
		WHERE active = true AND (workspace = $1 OR workspace = '*')
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
		if err := rows.Scan(&sm.ID, &sm.Workspace, &sm.Definition, &sm.Active, &sm.CreatedAt, &sm.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan state machine: %w", err)
		}
		machines = append(machines, sm)
	}
	return machines, rows.Err()
}

func (s *Store) DeactivateStateMachine(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE workflow_state_machines SET active = false WHERE id = $1", id)
	return err
}

func (s *Store) CreateStateMachineInstance(ctx context.Context, inst *StateMachineInstance) error {
	query := `
		INSERT INTO workflow_state_machine_instances (instance_id, machine_id, workspace, current_state, data, timeout_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at, updated_at
	`
	return s.db.QueryRowContext(ctx, query,
		inst.InstanceID, inst.MachineID, inst.Workspace, inst.CurrentState, inst.Data, inst.TimeoutAt,
	).Scan(&inst.CreatedAt, &inst.UpdatedAt)
}

func (s *Store) GetStateMachineInstance(ctx context.Context, instanceID string) (*StateMachineInstance, error) {
	query := `
		SELECT instance_id, machine_id, workspace, current_state, data,
		       created_at, updated_at, completed_at, timeout_at
		FROM workflow_state_machine_instances
		WHERE instance_id = $1
	`
	var inst StateMachineInstance
	err := s.db.QueryRowContext(ctx, query, instanceID).Scan(
		&inst.InstanceID, &inst.MachineID, &inst.Workspace, &inst.CurrentState, &inst.Data,
		&inst.CreatedAt, &inst.UpdatedAt, &inst.CompletedAt, &inst.TimeoutAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get state machine instance: %w", err)
	}
	return &inst, nil
}

func (s *Store) ListStateMachineInstances(ctx context.Context, machineID string) ([]StateMachineInstance, error) {
	query := `
		SELECT instance_id, machine_id, workspace, current_state, data,
		       created_at, updated_at, completed_at, timeout_at
		FROM workflow_state_machine_instances
		WHERE machine_id = $1
		ORDER BY created_at DESC
		LIMIT 100
	`
	rows, err := s.db.QueryContext(ctx, query, machineID)
	if err != nil {
		return nil, fmt.Errorf("list state machine instances: %w", err)
	}
	defer rows.Close()

	var instances []StateMachineInstance
	for rows.Next() {
		var inst StateMachineInstance
		if err := rows.Scan(
			&inst.InstanceID, &inst.MachineID, &inst.Workspace, &inst.CurrentState, &inst.Data,
			&inst.CreatedAt, &inst.UpdatedAt, &inst.CompletedAt, &inst.TimeoutAt,
		); err != nil {
			return nil, fmt.Errorf("scan state machine instance: %w", err)
		}
		instances = append(instances, inst)
	}
	return instances, rows.Err()
}

func (s *Store) UpdateStateMachineInstance(ctx context.Context, instanceID, newState string, timeoutAt *time.Time, completed bool) error {
	if completed {
		query := `UPDATE workflow_state_machine_instances SET current_state = $2, timeout_at = $3, completed_at = NOW() WHERE instance_id = $1`
		_, err := s.db.ExecContext(ctx, query, instanceID, newState, timeoutAt)
		return err
	}
	query := `UPDATE workflow_state_machine_instances SET current_state = $2, timeout_at = $3 WHERE instance_id = $1`
	_, err := s.db.ExecContext(ctx, query, instanceID, newState, timeoutAt)
	return err
}

func (s *Store) GetTimedOutInstances(ctx context.Context, now time.Time) ([]StateMachineInstance, error) {
	query := `
		SELECT instance_id, machine_id, workspace, current_state, data,
		       created_at, updated_at, completed_at, timeout_at
		FROM workflow_state_machine_instances
		WHERE timeout_at IS NOT NULL AND timeout_at <= $1 AND completed_at IS NULL
		ORDER BY timeout_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("get timed out instances: %w", err)
	}
	defer rows.Close()

	var instances []StateMachineInstance
	for rows.Next() {
		var inst StateMachineInstance
		if err := rows.Scan(
			&inst.InstanceID, &inst.MachineID, &inst.Workspace, &inst.CurrentState, &inst.Data,
			&inst.CreatedAt, &inst.UpdatedAt, &inst.CompletedAt, &inst.TimeoutAt,
		); err != nil {
			return nil, fmt.Errorf("scan timed out instance: %w", err)
		}
		instances = append(instances, inst)
	}
	return instances, rows.Err()
}

func (s *Store) ClearInstanceTimeout(ctx context.Context, instanceID string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE workflow_state_machine_instances SET timeout_at = NULL WHERE instance_id = $1", instanceID)
	return err
}
