// Package sqlite provides a native-SQLite implementation of tasks.Store.
// It is the Stage 2 replacement for the dbcompat-translated postgres impl
// that AetherLite used in Stage 1. This implementation:
//
//   - Uses pure SQLite SQL (no postgres-isms, no dbcompat translation)
//   - Stores timestamps as ISO-8601 TEXT, parses to time.Time inline
//   - Stores JSON columns as TEXT
//   - Uses ? placeholders (not $N)
//   - Uses strftime('%Y-%m-%dT%H:%M:%f', 'now') instead of NOW()
//   - Enforces single-writer discipline via SetMaxOpenConns(1) to prevent
//     SQLITE_BUSY contention in WAL mode (see master plan section 14.3)
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
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/storage/tasks"
	migrations "github.com/scitrera/aether/migrations/sqlite_tasks"

	// Register the bare "sqlite" driver (modernc.org/sqlite). This is the
	// same underlying driver that pkg/dbcompat wraps as "sqlite_compat",
	// but we use it directly without the translation layer since all SQL
	// in this package is native SQLite.
	_ "modernc.org/sqlite"
)

// Compile-time conformance assert: *Store satisfies tasks.Store.
var _ tasks.Store = (*Store)(nil)

// Store is the native-SQLite task store. All writes are serialized
// through SetMaxOpenConns(1) to prevent SQLITE_BUSY contention.
type Store struct {
	db *sql.DB
}

// New constructs a native-SQLite tasks Store. It runs the native migration
// set against db and configures the connection pool for single-writer
// semantics.
//
// The db handle must be opened with the bare "sqlite" driver (not
// "sqlite_compat") since this impl owns all its own SQL. Callers retain
// ownership of db; Store does NOT close the underlying handle.
func New(db *sql.DB) (*Store, error) {
	// Single-writer pool to avoid SQLITE_BUSY in WAL mode (section 14.3).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	if err := runMigrations(ctx, db, migrations.MigrationFS); err != nil {
		return nil, fmt.Errorf("tasks sqlite migrations: %w", err)
	}

	return &Store{db: db}, nil
}

// now returns the current time formatted as ISO-8601 for storage.
func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// =============================================================================
// Lifecycle (CRUD)
// =============================================================================

func (s *Store) CreateTask(ctx context.Context, task *tasks.Task) error {
	if task.TaskID == "" {
		task.TaskID = uuid.New().String()
	}
	if task.Status == "" {
		task.Status = tasks.TaskStatusPending
	}
	if task.AssignmentMode == "" {
		task.AssignmentMode = tasks.AssignmentModeSelfAssign
	}
	if task.TaskCategory == "" {
		task.TaskCategory = tasks.TaskCategoryRegular
	}
	if task.MaxRetries == 0 {
		task.MaxRetries = 3
	}

	launchParamsJSON, err := json.Marshal(task.LaunchParams)
	if err != nil {
		return fmt.Errorf("failed to marshal launch_params: %w", err)
	}
	metadataJSON, err := json.Marshal(task.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	checkpointJSON, err := json.Marshal(task.CheckpointData)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint_data: %w", err)
	}
	heartbeatJSON, err := json.Marshal(task.HeartbeatDetails)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat_details: %w", err)
	}
	var waitSpecStr sql.NullString
	if task.WaitSpec != nil {
		b, merr := json.Marshal(task.WaitSpec)
		if merr != nil {
			return fmt.Errorf("failed to marshal wait_spec: %w", merr)
		}
		waitSpecStr = sql.NullString{String: string(b), Valid: true}
	}
	var dependsOnStr sql.NullString
	if len(task.DependsOn) > 0 {
		b, merr := json.Marshal(task.DependsOn)
		if merr != nil {
			return fmt.Errorf("failed to marshal depends_on: %w", merr)
		}
		dependsOnStr = sql.NullString{String: string(b), Valid: true}
	}
	var pausedAtMs sql.NullInt64
	if task.PausedAt != nil {
		pausedAtMs = sql.NullInt64{Int64: task.PausedAt.UnixMilli(), Valid: true}
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO tasks (
			task_id, task_type, workspace, implementation, specifier,
			status, priority, scheduled_for,
			assignment_mode, task_category, target_agent_id, target_implementation, target_specifier,
			launch_params, queued_for_startup, parent_agent_id, parent_task_id,
			max_retries, payload, metadata, checkpoint_data,
			schedule_to_start_ms, start_to_close_ms, heartbeat_timeout_ms, schedule_to_close_ms,
			heartbeat_details, target_topic, source_topic, message_type,
			authority_mode, subject_type, subject_id, root_subject_type, root_subject_id,
			authority_grant_id, root_authority_grant_id, parent_authority_grant_id,
			authority_audience_type, authority_audience_id, authority_delegate_type, authority_delegate_id,
			task_class, grace_window_ms,
			wait_spec, depends_on, context_id, paused_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?,
			?, ?, ?, ?
		)
	`,
		task.TaskID,
		task.TaskType,
		task.Workspace,
		nullStr(task.Implementation),
		nullStr(task.Specifier),
		string(task.Status),
		task.Priority,
		nullTimeStr(task.ScheduledFor),
		string(task.AssignmentMode),
		string(task.TaskCategory),
		nullStr(task.TargetAgentID),
		nullStr(task.TargetImplementation),
		nullStr(task.TargetSpecifier),
		string(launchParamsJSON),
		boolToInt(task.QueuedForStartup),
		nullStr(task.ParentAgentID),
		nullStr(task.ParentTaskID),
		task.MaxRetries,
		task.Payload,
		string(metadataJSON),
		string(checkpointJSON),
		nullInt64(task.ScheduleToStartMs),
		nullInt64(task.StartToCloseMs),
		nullInt64(task.HeartbeatTimeoutMs),
		nullInt64(task.ScheduleToCloseMs),
		string(heartbeatJSON),
		nullStr(task.TargetTopic),
		nullStr(task.SourceTopic),
		nullStr(task.MessageType),
		nullStr(task.Authority.Mode),
		nullStr(task.Authority.SubjectType),
		nullStr(task.Authority.SubjectID),
		nullStr(task.Authority.RootSubjectType),
		nullStr(task.Authority.RootSubjectID),
		nullStr(task.Authority.AuthorityGrantID),
		nullStr(task.Authority.RootAuthorityGrantID),
		nullStr(task.Authority.ParentAuthorityGrantID),
		nullStr(task.Authority.AudienceType),
		nullStr(task.Authority.AudienceID),
		nullStr(task.Authority.DelegateType),
		nullStr(task.Authority.DelegateID),
		task.TaskClass,
		task.GraceWindowMs,
		waitSpecStr,
		dependsOnStr,
		nullStr(task.ContextID),
		pausedAtMs,
	)
	return err
}

func (s *Store) GetTask(ctx context.Context, taskID string) (*tasks.Task, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+taskSelectColumns+` FROM tasks WHERE task_id = ?`, taskID)
	return scanTask(row)
}

func (s *Store) UpdateTaskStatus(ctx context.Context, taskID string, status tasks.TaskStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, string(status), taskID)
	return err
}

func (s *Store) UpdateTaskMetadata(ctx context.Context, taskID string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE tasks SET metadata = ? WHERE task_id = ?`, string(metadataJSON), taskID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("task %s not found", taskID)
	}
	return nil
}

func (s *Store) UpdateTaskAuthority(ctx context.Context, taskID string, authority tasks.TaskAuthorityInfo, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET metadata = ?,
			authority_mode = ?,
			subject_type = ?,
			subject_id = ?,
			root_subject_type = ?,
			root_subject_id = ?,
			authority_grant_id = ?,
			root_authority_grant_id = ?,
			parent_authority_grant_id = ?,
			authority_audience_type = ?,
			authority_audience_id = ?,
			authority_delegate_type = ?,
			authority_delegate_id = ?
		WHERE task_id = ?
	`,
		string(metadataJSON),
		nullStr(authority.Mode),
		nullStr(authority.SubjectType),
		nullStr(authority.SubjectID),
		nullStr(authority.RootSubjectType),
		nullStr(authority.RootSubjectID),
		nullStr(authority.AuthorityGrantID),
		nullStr(authority.RootAuthorityGrantID),
		nullStr(authority.ParentAuthorityGrantID),
		nullStr(authority.AudienceType),
		nullStr(authority.AudienceID),
		nullStr(authority.DelegateType),
		nullStr(authority.DelegateID),
		taskID,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("task %s not found", taskID)
	}
	return nil
}

// =============================================================================
// State transitions
// =============================================================================

func (s *Store) AssignTask(ctx context.Context, taskID, workerIdentity string) error {
	nowStr := now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	result, err := tx.ExecContext(ctx, `
		UPDATE tasks SET status = 'assigned', assigned_to = ?, assigned_at = ?
		WHERE task_id = ? AND status = 'pending'
	`, workerIdentity, nowStr, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in pending state", taskID)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO task_assignments (assignment_id, task_id, worker_identity, assigned_at)
		VALUES (?, ?, ?, ?)
	`, uuid.New().String(), taskID, workerIdentity, nowStr)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) StartingTask(ctx context.Context, taskID string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET status = 'starting' WHERE task_id = ? AND status = 'assigned'
	`, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not in assigned state", taskID)
	}
	return nil
}

func (s *Store) StartTask(ctx context.Context, taskID string) error {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'running', started_at = COALESCE(started_at, ?)
		WHERE task_id = ? AND status IN ('assigned', 'starting', 'running')
	`, nowStr, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not in assigned, starting, or running state", taskID)
	}
	return nil
}

func (s *Store) StartTaskWithAgent(ctx context.Context, taskID, agentIdentity string) error {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'running', started_at = COALESCE(started_at, ?), target_agent_id = ?
		WHERE task_id = ? AND status IN ('assigned', 'starting', 'running')
	`, nowStr, agentIdentity, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not in assigned, starting, or running state", taskID)
	}
	return nil
}

func (s *Store) CompleteTask(ctx context.Context, taskID string) error {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET status = 'completed', completed_at = ? WHERE task_id = ?
	`, nowStr, taskID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in a completable state", taskID)
	}
	return nil
}

func (s *Store) FailTask(ctx context.Context, taskID, errorMsg string) error {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'failed', failed_at = ?, error_message = ?, retry_count = retry_count + 1
		WHERE task_id = ?
	`, nowStr, errorMsg, taskID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in a failable state", taskID)
	}
	return nil
}

func (s *Store) FailTaskWithRetry(ctx context.Context, taskID, errorType, errorMsg string, nextRetry *time.Time) error {
	nowStr := now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'failed', failed_at = ?, error_type = ?, error_message = ?,
		    next_retry_at = ?, retry_count = retry_count + 1
		WHERE task_id = ?
	`, nowStr, errorType, errorMsg, nullTimeStr(nextRetry), taskID)
	return err
}

func (s *Store) CancelTask(ctx context.Context, taskID string) error {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'cancelled', completed_at = ?, error_type = 'CANCELLED', error_message = 'Task cancelled'
		WHERE task_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')
	`, nowStr, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or already completed/failed/cancelled", taskID)
	}
	return nil
}

func (s *Store) RetryTask(ctx context.Context, taskID string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'pending',
			started_at = NULL, failed_at = NULL, assigned_to = NULL, assigned_at = NULL,
			next_retry_at = NULL, error_message = NULL, error_type = NULL
		WHERE task_id = ? AND status IN ('failed', 'cancelled')
	`, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in retryable state", taskID)
	}
	return nil
}

func (s *Store) RescheduleTaskAt(ctx context.Context, taskID string, retryAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET next_retry_at = ? WHERE task_id = ? AND status = 'failed'
	`, retryAt.UTC().Format(time.RFC3339Nano), taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in failed state", taskID)
	}
	return nil
}

// =============================================================================
// Listing / queries
// =============================================================================

// buildFilterClausesSQLite returns the shared WHERE fragment used by
// ListTasks and ListTasksPage. When skipParentTaskID is true the ParentTaskID
// predicate is omitted so callers can splice the recursive CTE in.
func buildFilterClausesSQLite(filter *tasks.TaskFilter, args []interface{}, skipParentTaskID bool) (string, []interface{}) {
	var query string
	if len(filter.Statuses) > 0 {
		placeholders := make([]string, len(filter.Statuses))
		for i, st := range filter.Statuses {
			placeholders[i] = "?"
			args = append(args, string(st))
		}
		query += " AND status IN (" + strings.Join(placeholders, ", ") + ")"
	} else if filter.Status != nil {
		query += " AND status = ?"
		args = append(args, string(*filter.Status))
	}
	if filter.Workspace != "" {
		query += " AND workspace = ?"
		args = append(args, filter.Workspace)
	}
	if filter.TaskType != "" {
		query += " AND task_type = ?"
		args = append(args, filter.TaskType)
	}
	if filter.TaskCategory != nil {
		query += " AND task_category = ?"
		args = append(args, string(*filter.TaskCategory))
	}
	if filter.AssignmentMode != nil {
		query += " AND assignment_mode = ?"
		args = append(args, string(*filter.AssignmentMode))
	}
	if filter.AssignedTo != "" {
		query += " AND assigned_to = ?"
		args = append(args, filter.AssignedTo)
	}
	if filter.TargetAgentID != "" {
		query += " AND target_agent_id = ?"
		args = append(args, filter.TargetAgentID)
	}
	if filter.TargetImplementation != "" {
		query += " AND target_implementation = ?"
		args = append(args, filter.TargetImplementation)
	}
	if filter.QueuedForStartup != nil {
		query += " AND queued_for_startup = ?"
		args = append(args, boolToInt(*filter.QueuedForStartup))
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
	if filter.RootAuthorityGrantID != "" {
		query += " AND root_authority_grant_id = ?"
		args = append(args, filter.RootAuthorityGrantID)
	}
	if filter.ParentTaskID != "" && !skipParentTaskID {
		query += " AND parent_task_id = ?"
		args = append(args, filter.ParentTaskID)
	}
	if filter.TaskClass != 0 {
		query += " AND task_class = ?"
		args = append(args, filter.TaskClass)
	}
	if len(filter.ExcludeTaskClasses) > 0 {
		placeholders := make([]string, len(filter.ExcludeTaskClasses))
		for i, c := range filter.ExcludeTaskClasses {
			placeholders[i] = "?"
			args = append(args, c)
		}
		query += " AND (task_class IS NULL OR task_class NOT IN (" + strings.Join(placeholders, ", ") + "))"
	}
	if filter.ContextID != "" {
		query += " AND context_id = ?"
		args = append(args, filter.ContextID)
	}
	if len(filter.ExcludeStatuses) > 0 {
		placeholders := make([]string, len(filter.ExcludeStatuses))
		for i, st := range filter.ExcludeStatuses {
			placeholders[i] = "?"
			args = append(args, string(st))
		}
		query += " AND status NOT IN (" + strings.Join(placeholders, ", ") + ")"
	}
	// Phase 4 filters. CreatorActorID maps to the parent_agent_id column
	// (storage backing for TaskInfo.creator_actor_id). CreatorActorType is
	// informational only.
	if filter.CreatorActorID != "" {
		query += " AND parent_agent_id = ?"
		args = append(args, filter.CreatorActorID)
	}
	if filter.StatusTimestampAfterUnixMs > 0 {
		// Compare against the ISO-8601 TEXT format the native sqlite store
		// uses for timestamps (parseTimestamp accepts both .000 and .000000
		// fractional precisions; the canonical strftime format uses .000).
		t := time.UnixMilli(filter.StatusTimestampAfterUnixMs).UTC()
		query += " AND updated_at >= ?"
		args = append(args, t.Format("2006-01-02T15:04:05.000"))
	}
	return query, args
}

func (s *Store) ListTasks(ctx context.Context, filter *tasks.TaskFilter) ([]*tasks.Task, error) {
	if filter == nil {
		filter = &tasks.TaskFilter{}
	}
	if filter.Limit == 0 {
		filter.Limit = 100
	}

	query := `SELECT ` + taskSelectColumns + ` FROM tasks WHERE 1=1`
	args := []interface{}{}

	clauses, args := buildFilterClausesSQLite(filter, args, false)
	query += clauses

	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, filter.Limit, filter.Offset)

	return s.queryTasks(ctx, query, args...)
}

// ListTasksPage implements the cursor-aware Phase 4 listing. SQLite supports
// recursive CTEs so the IncludeDescendants walk uses the same WITH RECURSIVE
// shape as the postgres path; we cap row count at 10000 to bound runaway
// walks on deep trees.
func (s *Store) ListTasksPage(ctx context.Context, filter *tasks.TaskFilter) ([]*tasks.Task, string, error) {
	if filter == nil {
		filter = &tasks.TaskFilter{}
	}
	if filter.Limit == 0 {
		filter.Limit = 100
	}
	limit := filter.Limit

	// Decode cursor if present. The cursor encodes (updated_at_micros, task_id).
	// On sqlite the updated_at column is ISO-8601 TEXT, so we format the
	// cursor timestamp identically and compare lexicographically — which is
	// correct because ISO-8601 TEXT sorts the same as time order.
	var cursorTime string
	var cursorTaskID string
	useCursor := false
	if filter.PageToken != "" {
		micros, id, decErr := tasks.DecodePageToken(filter.PageToken)
		if decErr != nil {
			return nil, "", fmt.Errorf("invalid page_token: %w", decErr)
		}
		cursorTime = time.UnixMicro(micros).UTC().Format("2006-01-02T15:04:05.000")
		cursorTaskID = id
		useCursor = true
	}

	recursive := filter.IncludeDescendants && filter.ParentTaskID != ""

	args := []interface{}{}
	var query string
	if recursive {
		// SQLite's recursive CTE syntax mirrors postgres. We cap the
		// walked rows to keep traversal bounded; combined with the LIMIT
		// in the outer SELECT, deep cycles cannot DoS the server.
		query = `
			WITH RECURSIVE descendants AS (
				SELECT t.* FROM tasks t WHERE t.parent_task_id = ?
				UNION ALL
				SELECT t.* FROM tasks t
				INNER JOIN descendants d ON t.parent_task_id = d.task_id
			)
			SELECT ` + taskSelectColumns + ` FROM descendants WHERE 1=1`
		args = append(args, filter.ParentTaskID)
	} else {
		query = `SELECT ` + taskSelectColumns + ` FROM tasks WHERE 1=1`
	}

	clauses, args := buildFilterClausesSQLite(filter, args, recursive)
	query += clauses

	if useCursor {
		// SQLite supports row-value comparison since 3.15.0; pin to that
		// for stable cursor pagination ordered (updated_at DESC, task_id DESC).
		query += " AND (updated_at, task_id) < (?, ?)"
		args = append(args, cursorTime, cursorTaskID)
	}

	query += " ORDER BY updated_at DESC, task_id DESC"
	if recursive && limit > 10000 {
		limit = 10000
	}
	query += " LIMIT ?"
	args = append(args, limit)

	rows, err := s.queryTasks(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}

	var nextToken string
	if len(rows) == limit && len(rows) > 0 {
		last := rows[len(rows)-1]
		nextToken = tasks.EncodePageToken(last.UpdatedAt.UnixMicro(), last.TaskID)
	}
	return rows, nextToken, nil
}

func (s *Store) GetTasksByStatus(ctx context.Context, status tasks.TaskStatus, limit int) ([]*tasks.Task, error) {
	statusPtr := status
	return s.ListTasks(ctx, &tasks.TaskFilter{Status: &statusPtr, Limit: limit})
}

func (s *Store) GetQueuedTasksForAgent(ctx context.Context, agentID string) ([]*tasks.Task, error) {
	queuedTrue := true
	return s.ListTasks(ctx, &tasks.TaskFilter{
		TargetAgentID:    agentID,
		QueuedForStartup: &queuedTrue,
	})
}

func (s *Store) GetWorkspaceTasks(ctx context.Context, workspace string, orchestratedOnly bool) ([]*tasks.Task, error) {
	filter := &tasks.TaskFilter{Workspace: workspace}
	if orchestratedOnly {
		cat := tasks.TaskCategoryOrchestrated
		filter.TaskCategory = &cat
	}
	return s.ListTasks(ctx, filter)
}

func (s *Store) GetAgentTasks(ctx context.Context, agentID string) ([]*tasks.Task, error) {
	query := `SELECT ` + taskSelectColumns + ` FROM tasks WHERE assigned_to = ? OR target_agent_id = ? ORDER BY created_at DESC LIMIT 1000`
	return s.queryTasks(ctx, query, agentID, agentID)
}

func (s *Store) GetTasksNeedingRetry(ctx context.Context, beforeTime time.Time, limit int) ([]*tasks.Task, error) {
	query := `
		SELECT ` + taskSelectColumns + `
		FROM tasks
		WHERE status = 'failed'
			AND next_retry_at IS NOT NULL
			AND next_retry_at <= ?
			AND retry_count < max_retries
		ORDER BY next_retry_at ASC
		LIMIT ?
	`
	return s.queryTasks(ctx, query, beforeTime.UTC().Format(time.RFC3339Nano), limit)
}

func (s *Store) GetTaskCounts(ctx context.Context) (*tasks.TaskCounts, error) {
	// SQLite does not support FILTER (WHERE ...) aggregate syntax.
	// Use SUM(CASE ...) instead.
	query := `
		SELECT
			COUNT(*) as total,
			SUM(CASE WHEN status = 'pending'   THEN 1 ELSE 0 END) as pending,
			SUM(CASE WHEN status = 'assigned'  THEN 1 ELSE 0 END) as assigned,
			SUM(CASE WHEN status = 'starting'  THEN 1 ELSE 0 END) as starting,
			SUM(CASE WHEN status = 'running'   THEN 1 ELSE 0 END) as running,
			SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
			SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END) as failed
		FROM tasks
	`
	var counts tasks.TaskCounts
	err := s.db.QueryRowContext(ctx, query).Scan(
		&counts.Total,
		&counts.Pending,
		&counts.Assigned,
		&counts.Starting,
		&counts.Running,
		&counts.Completed,
		&counts.Failed,
	)
	if err != nil {
		return nil, err
	}
	return &counts, nil
}

// =============================================================================
// Pool / startup
// =============================================================================

func (s *Store) MarkTaskNotQueued(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET queued_for_startup = 0 WHERE task_id = ?`, taskID)
	return err
}

func (s *Store) ClaimPoolTask(ctx context.Context, taskID, workerIdentity string) (bool, error) {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'assigned', assigned_to = ?, assigned_at = ?, queued_for_startup = 0
		WHERE task_id = ? AND status = 'pending' AND assignment_mode = 'pool'
	`, workerIdentity, nowStr, taskID)
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

func (s *Store) UnassignPoolTask(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'pending', assigned_to = NULL, assigned_at = NULL, queued_for_startup = 1
		WHERE task_id = ? AND status = 'assigned' AND assignment_mode = 'pool'
	`, taskID)
	return err
}

func (s *Store) GetPendingPoolTasks(ctx context.Context, implementation, workspace string) ([]*tasks.Task, error) {
	pendingStatus := tasks.TaskStatusPending
	poolMode := tasks.AssignmentModePool
	queuedTrue := true
	return s.ListTasks(ctx, &tasks.TaskFilter{
		Status:               &pendingStatus,
		AssignmentMode:       &poolMode,
		TargetImplementation: implementation,
		Workspace:            workspace,
		QueuedForStartup:     &queuedTrue,
		Limit:                100,
	})
}

func (s *Store) HasActiveStartupTask(ctx context.Context, targetImplementation, workspace, targetSpecifier string) (bool, string, error) {
	var taskID string
	err := s.db.QueryRowContext(ctx, `
		SELECT task_id FROM tasks
		WHERE task_type = 'agent_startup'
			AND target_implementation = ?
			AND workspace = ?
			AND COALESCE(target_specifier, '') = ?
			AND status IN ('pending', 'assigned', 'starting', 'running')
		LIMIT 1
	`, targetImplementation, workspace, targetSpecifier).Scan(&taskID)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, taskID, nil
}

// =============================================================================
// Checkpoints / heartbeat
// =============================================================================

func (s *Store) UpdateCheckpoint(ctx context.Context, taskID string, checkpointData map[string]interface{}) error {
	checkpointJSON, err := json.Marshal(checkpointData)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint_data: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `UPDATE tasks SET checkpoint_data = ? WHERE task_id = ?`, string(checkpointJSON), taskID)
	return err
}

func (s *Store) UpdateHeartbeat(ctx context.Context, taskID string, details map[string]interface{}) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat_details: %w", err)
	}
	nowStr := now()
	_, err = s.db.ExecContext(ctx, `UPDATE tasks SET last_heartbeat = ?, heartbeat_details = ? WHERE task_id = ?`, nowStr, string(detailsJSON), taskID)
	return err
}

func (s *Store) CreateCheckpoint(ctx context.Context, checkpoint *tasks.CheckpointRecord) error {
	if checkpoint.CheckpointID == "" {
		checkpoint.CheckpointID = uuid.New().String()
	}
	checkpointJSON, err := json.Marshal(checkpoint.CheckpointData)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint_data: %w", err)
	}
	nowStr := now()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO task_checkpoints (checkpoint_id, task_id, sequence_number, checkpoint_data, created_by, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (task_id, sequence_number) DO UPDATE SET
			checkpoint_data = excluded.checkpoint_data,
			created_at = excluded.created_at,
			created_by = excluded.created_by
	`, checkpoint.CheckpointID, checkpoint.TaskID, checkpoint.SequenceNumber, string(checkpointJSON), checkpoint.CreatedBy, nowStr)
	return err
}

func (s *Store) GetLatestCheckpoint(ctx context.Context, taskID string) (*tasks.CheckpointRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT checkpoint_id, task_id, sequence_number, checkpoint_data, created_at, created_by
		FROM task_checkpoints WHERE task_id = ? ORDER BY sequence_number DESC LIMIT 1
	`, taskID)

	var cp tasks.CheckpointRecord
	var checkpointJSON []byte
	var createdAtStr string

	err := row.Scan(&cp.CheckpointID, &cp.TaskID, &cp.SequenceNumber, &checkpointJSON, &createdAtStr, &cp.CreatedBy)
	if err != nil {
		return nil, err
	}

	cp.CreatedAt, err = parseTimestamp(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint created_at: %w", err)
	}

	if len(checkpointJSON) > 0 {
		if err := json.Unmarshal(checkpointJSON, &cp.CheckpointData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal checkpoint_data: %w", err)
		}
	}

	return &cp, nil
}

// =============================================================================
// Timers
// =============================================================================

func (s *Store) CreateTimer(ctx context.Context, timer *tasks.TimerRecord) error {
	if timer.TimerID == "" {
		timer.TimerID = uuid.New().String()
	}
	metadataJSON, err := json.Marshal(timer.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO task_timers (timer_id, task_id, timer_type, fires_at, metadata)
		VALUES (?, ?, ?, ?, ?)
	`, timer.TimerID, timer.TaskID, string(timer.TimerType), timer.FiresAt.UTC().Format(time.RFC3339Nano), string(metadataJSON))
	return err
}

func (s *Store) GetTimer(ctx context.Context, timerID string) (*tasks.TimerRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT timer_id, task_id, timer_type, fires_at, created_at, fired, fired_at, metadata
		FROM task_timers WHERE timer_id = ?
	`, timerID)
	return scanTimer(row)
}

func (s *Store) GetTimersForTask(ctx context.Context, taskID string) ([]*tasks.TimerRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT timer_id, task_id, timer_type, fires_at, created_at, fired, fired_at, metadata
		FROM task_timers WHERE task_id = ? AND fired = 0 ORDER BY fires_at ASC
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimerRows(rows)
}

func (s *Store) GetPendingTimers(ctx context.Context, beforeTime time.Time, limit int) ([]*tasks.TimerRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT timer_id, task_id, timer_type, fires_at, created_at, fired, fired_at, metadata
		FROM task_timers WHERE fired = 0 AND fires_at <= ? ORDER BY fires_at ASC LIMIT ?
	`, beforeTime.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimerRows(rows)
}

func (s *Store) MarkTimerFired(ctx context.Context, timerID string) error {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE task_timers SET fired = 1, fired_at = ? WHERE timer_id = ? AND fired = 0
	`, nowStr, timerID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("timer %s not found or already fired", timerID)
	}
	return nil
}

func (s *Store) DeleteTimer(ctx context.Context, timerID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM task_timers WHERE timer_id = ?`, timerID)
	return err
}

func (s *Store) DeleteTimersForTask(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM task_timers WHERE task_id = ?`, taskID)
	return err
}

// =============================================================================
// Assignment history
// =============================================================================

func (s *Store) RecordAssignment(ctx context.Context, assignment *tasks.AssignmentRecord) error {
	if assignment.AssignmentID == "" {
		assignment.AssignmentID = uuid.New().String()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO task_assignments (assignment_id, task_id, worker_identity, assigned_at)
		VALUES (?, ?, ?, ?)
	`, assignment.AssignmentID, assignment.TaskID, assignment.WorkerIdentity,
		assignment.AssignedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) GetAssignmentHistory(ctx context.Context, taskID string) ([]*tasks.AssignmentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT assignment_id, task_id, worker_identity, assigned_at, started_at, completed_at, failed, failure_reason
		FROM task_assignments WHERE task_id = ? ORDER BY assigned_at ASC
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assignments []*tasks.AssignmentRecord
	for rows.Next() {
		var a tasks.AssignmentRecord
		var assignedAtStr string
		var startedAtStr, completedAtStr sql.NullString
		var failureReason sql.NullString

		err := rows.Scan(&a.AssignmentID, &a.TaskID, &a.WorkerIdentity, &assignedAtStr, &startedAtStr, &completedAtStr, &a.Failed, &failureReason)
		if err != nil {
			return nil, err
		}

		a.AssignedAt, err = parseTimestamp(assignedAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse assigned_at: %w", err)
		}
		if startedAtStr.Valid {
			t, err := parseTimestamp(startedAtStr.String)
			if err != nil {
				return nil, fmt.Errorf("parse started_at: %w", err)
			}
			a.StartedAt = &t
		}
		if completedAtStr.Valid {
			t, err := parseTimestamp(completedAtStr.String)
			if err != nil {
				return nil, fmt.Errorf("parse completed_at: %w", err)
			}
			a.CompletedAt = &t
		}
		if failureReason.Valid {
			a.FailureReason = failureReason.String
		}
		assignments = append(assignments, &a)
	}
	return assignments, rows.Err()
}

// =============================================================================
// Audit events
// =============================================================================

func (s *Store) RecordAuditEvent(ctx context.Context, event *tasks.TaskAuditEvent) error {
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}
	eventJSON, err := json.Marshal(event.EventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event_data: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO task_audit_events (event_id, task_id, event_type, event_data, created_by)
		VALUES (?, ?, ?, ?, ?)
	`, event.EventID, event.TaskID, event.EventType, string(eventJSON), event.CreatedBy)
	return err
}

func (s *Store) RecordAuditEventTx(ctx context.Context, tx tasks.StoreTx, event *tasks.TaskAuditEvent) error {
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}
	rawTx := unwrapTx(tx)
	eventJSON, err := json.Marshal(event.EventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event_data: %w", err)
	}
	_, err = rawTx.ExecContext(ctx, `
		INSERT INTO task_audit_events (event_id, task_id, event_type, event_data, created_by)
		VALUES (?, ?, ?, ?, ?)
	`, event.EventID, event.TaskID, event.EventType, string(eventJSON), event.CreatedBy)
	return err
}

func (s *Store) GetTaskAuditEvents(ctx context.Context, taskID string) ([]*tasks.TaskAuditEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, task_id, event_type, event_data, created_at, created_by
		FROM task_audit_events WHERE task_id = ? ORDER BY created_at ASC
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*tasks.TaskAuditEvent
	for rows.Next() {
		var e tasks.TaskAuditEvent
		var eventJSON []byte
		var createdAtStr string

		err := rows.Scan(&e.EventID, &e.TaskID, &e.EventType, &eventJSON, &createdAtStr, &e.CreatedBy)
		if err != nil {
			return nil, err
		}

		e.CreatedAt, err = parseTimestamp(createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse audit event created_at: %w", err)
		}

		if len(eventJSON) > 0 {
			if err := json.Unmarshal(eventJSON, &e.EventData); err != nil {
				return nil, fmt.Errorf("failed to unmarshal event_data: %w", err)
			}
		}
		events = append(events, &e)
	}
	return events, rows.Err()
}

// =============================================================================
// StoreTx lifecycle + transactional queue operations
// =============================================================================

// sqliteStoreTx wraps a *sql.Tx and satisfies tasks.StoreTx.
type sqliteStoreTx struct {
	tx *sql.Tx
}

func (t *sqliteStoreTx) Commit() error   { return t.tx.Commit() }
func (t *sqliteStoreTx) Rollback() error { return t.tx.Rollback() }

// unwrapTx recovers the *sql.Tx from a tasks.StoreTx. Panics if the
// concrete type is not *sqliteStoreTx.
func unwrapTx(tx tasks.StoreTx) *sql.Tx {
	stx, ok := tx.(*sqliteStoreTx)
	if !ok {
		panic(fmt.Sprintf("sqlite.Store: expected *sqliteStoreTx, got %T", tx))
	}
	return stx.tx
}

func (s *Store) BeginTx(ctx context.Context) (tasks.StoreTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqliteStoreTx{tx: tx}, nil
}

func (s *Store) QueryQueueEntryForUnclaimTx(ctx context.Context, tx tasks.StoreTx, queueID string) (taskID, workspace string, retryCount, maxRetries int, err error) {
	rawTx := unwrapTx(tx)
	err = rawTx.QueryRowContext(ctx, `
		SELECT task_id, workspace, retry_count, max_retries
		FROM orchestrated_task_queue
		WHERE queue_id = ? AND status = 'claimed'
	`, queueID).Scan(&taskID, &workspace, &retryCount, &maxRetries)
	return
}

func (s *Store) UpdateQueueEntryForRetryTx(ctx context.Context, tx tasks.StoreTx, queueID string, newRetryCount, backoffSeconds int) error {
	rawTx := unwrapTx(tx)
	// SQLite doesn't support interval arithmetic. Use datetime() function
	// to add seconds to the current time.
	_, err := rawTx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'pending',
		    claimed_by = NULL,
		    claimed_at = NULL,
		    retry_count = ?,
		    next_retry_at = strftime('%Y-%m-%dT%H:%M:%f', 'now', '+' || ? || ' seconds')
		WHERE queue_id = ?
	`, newRetryCount, backoffSeconds, queueID)
	return err
}

func (s *Store) MarkQueueEntryFailedTx(ctx context.Context, tx tasks.StoreTx, queueID, errorMsg string) error {
	rawTx := unwrapTx(tx)
	nowStr := now()
	_, err := rawTx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'failed', error_message = ?, completed_at = ?
		WHERE queue_id = ?
	`, errorMsg, nowStr, queueID)
	return err
}

func (s *Store) InsertDLQEntryTx(ctx context.Context, tx tasks.StoreTx, taskID, workspace, reason string, attemptCount int) error {
	rawTx := unwrapTx(tx)
	nowStr := now()
	_, err := rawTx.ExecContext(ctx, `
		INSERT INTO dlq (original_task_id, category, workspace, failure_reason, attempt_count, last_attempt_at)
		VALUES (?, 'delivery_failure', ?, ?, ?, ?)
	`, taskID, workspace, reason, attemptCount, nowStr)
	return err
}

// =============================================================================
// Non-transactional queue operations (orchestrated_task_queue)
// =============================================================================

func (s *Store) InsertQueueEntry(ctx context.Context, queueID, taskID, targetImplementation, workspace, profile string, launchParamsJSON []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO orchestrated_task_queue
		(queue_id, task_id, target_implementation, workspace, profile, launch_params, status)
		VALUES (?, ?, ?, ?, ?, ?, 'pending')
	`, queueID, taskID, targetImplementation, workspace, profile, launchParamsJSON)
	return err
}

func (s *Store) PollPendingQueueEntries(ctx context.Context, limit int) ([]*tasks.QueueEntryNotification, error) {
	if limit <= 0 {
		limit = 10
	}
	nowStr := now()
	rows, err := s.db.QueryContext(ctx, `
		SELECT queue_id, task_id, profile, workspace, target_implementation
		FROM orchestrated_task_queue
		WHERE status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
		ORDER BY created_at ASC
		LIMIT ?
	`, nowStr, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*tasks.QueueEntryNotification
	for rows.Next() {
		var e tasks.QueueEntryNotification
		if err := rows.Scan(&e.QueueID, &e.TaskID, &e.Profile, &e.Workspace, &e.TargetImplementation); err != nil {
			return nil, err
		}
		entries = append(entries, &e)
	}
	return entries, rows.Err()
}

func (s *Store) CountPendingQueueEntries(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orchestrated_task_queue WHERE status = 'pending'
	`).Scan(&count)
	return count, err
}

func (s *Store) ClaimQueueEntry(ctx context.Context, queueID, claimedBy string) (bool, error) {
	nowStr := now()
	result, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'claimed', claimed_by = ?, claimed_at = ?
		WHERE queue_id = ? AND status = 'pending'
	`, claimedBy, nowStr, queueID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) CompleteQueueEntry(ctx context.Context, queueID string) error {
	nowStr := now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'completed', completed_at = ?
		WHERE queue_id = ?
	`, nowStr, queueID)
	return err
}

func (s *Store) FailQueueEntry(ctx context.Context, queueID, errorMsg string) error {
	nowStr := now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'failed', error_message = ?, completed_at = ?
		WHERE queue_id = ?
	`, errorMsg, nowStr, queueID)
	return err
}

func (s *Store) GetQueueEntryDetails(ctx context.Context, queueID string) (*tasks.QueueEntryDetails, error) {
	var d tasks.QueueEntryDetails
	var launchParamsJSON []byte

	err := s.db.QueryRowContext(ctx, `
		SELECT task_id, target_implementation, workspace, profile, launch_params
		FROM orchestrated_task_queue
		WHERE queue_id = ?
	`, queueID).Scan(&d.TaskID, &d.TargetImplementation, &d.Workspace, &d.Profile, &launchParamsJSON)
	if err != nil {
		return nil, err
	}

	if launchParamsJSON != nil {
		if err := json.Unmarshal(launchParamsJSON, &d.LaunchParams); err != nil {
			return nil, fmt.Errorf("unmarshal launch_params: %w", err)
		}
	}
	return &d, nil
}

func (s *Store) ListStaleClaimedQueueEntries(ctx context.Context, threshold time.Duration, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}
	// SQLite doesn't support interval arithmetic. Compute the cutoff
	// timestamp in Go and pass it as a parameter.
	cutoff := time.Now().UTC().Add(-threshold).Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
		SELECT queue_id
		FROM orchestrated_task_queue
		WHERE status = 'claimed' AND claimed_at < ?
		ORDER BY claimed_at ASC
		LIMIT ?
	`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) CompleteQueueEntryByTaskID(ctx context.Context, taskID string) error {
	nowStr := now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'completed', completed_at = ?
		WHERE task_id = ? AND status IN ('pending', 'claimed')
	`, nowStr, taskID)
	return err
}

func (s *Store) FailQueueEntryByTaskID(ctx context.Context, taskID, errorMsg string) error {
	nowStr := now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = 'failed', error_message = ?, completed_at = ?
		WHERE task_id = ? AND status IN ('pending', 'claimed')
	`, errorMsg, nowStr, taskID)
	return err
}

// =============================================================================
// Disconnect tracking
// =============================================================================

func (s *Store) MarkTaskDisconnected(ctx context.Context, taskID string, when time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET disconnected_at = ?
		WHERE task_id = ? AND status = 'running' AND disconnected_at IS NULL
	`, when.UTC().Format(time.RFC3339Nano), taskID)
	return err
}

func (s *Store) ClearTaskDisconnected(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET disconnected_at = NULL WHERE task_id = ?`, taskID)
	return err
}

func (s *Store) ListDisconnectedTasks(ctx context.Context, limit int) ([]*tasks.Task, error) {
	if limit <= 0 {
		limit = 500
	}
	query := `SELECT ` + taskSelectColumns + ` FROM tasks WHERE status = 'running' AND disconnected_at IS NOT NULL ORDER BY disconnected_at LIMIT ?`
	return s.queryTasks(ctx, query, limit)
}

// =============================================================================
// Dead letter queue (DLQ)
// =============================================================================

func (s *Store) WriteToDLQ(ctx context.Context, dlqRecord *tasks.DLQRecord) error {
	if dlqRecord.DLQMessageID == "" {
		dlqRecord.DLQMessageID = uuid.New().String()
	}
	if dlqRecord.EnqueuedAt.IsZero() {
		dlqRecord.EnqueuedAt = time.Now()
	}

	originalMetaJSON, err := json.Marshal(dlqRecord.OriginalMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal original metadata: %w", err)
	}
	failureDetailsJSON, err := json.Marshal(dlqRecord.FailureDetails)
	if err != nil {
		return fmt.Errorf("failed to marshal failure details: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO dlq (
			dlq_message_id, original_task_id, category, workspace,
			original_payload, original_metadata, failure_reason, failure_details,
			enqueued_at, attempt_count, last_attempt_at,
			reprocessed_at, resolved, resolved_by, resolution_notes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		dlqRecord.DLQMessageID,
		dlqRecord.OriginalTaskID,
		dlqRecord.Category,
		dlqRecord.Workspace,
		dlqRecord.OriginalPayload,
		string(originalMetaJSON),
		dlqRecord.FailureReason,
		string(failureDetailsJSON),
		dlqRecord.EnqueuedAt.UTC().Format(time.RFC3339Nano),
		dlqRecord.AttemptCount,
		dlqRecord.LastAttemptAt.UTC().Format(time.RFC3339Nano),
		nullTimeStr(dlqRecord.ReprocessedAt),
		boolToInt(dlqRecord.Resolved),
		nullStr(dlqRecord.ResolvedBy),
		nullStr(dlqRecord.ResolutionNotes),
	)
	return err
}

func (s *Store) GetDLQTasks(ctx context.Context, workspace string, category string, limit int, offset int) ([]*tasks.DLQRecord, error) {
	if limit == 0 {
		limit = 100
	}

	query := `
		SELECT dlq_message_id, original_task_id, category, workspace,
			original_payload, original_metadata, failure_reason, failure_details,
			enqueued_at, attempt_count, last_attempt_at,
			reprocessed_at, resolved, resolved_by, resolution_notes
		FROM dlq WHERE 1=1
	`
	args := []interface{}{}

	if workspace != "" {
		query += " AND workspace = ?"
		args = append(args, workspace)
	}
	if category != "" {
		query += " AND category = ?"
		args = append(args, category)
	}

	query += " ORDER BY enqueued_at DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*tasks.DLQRecord
	for rows.Next() {
		record, err := scanDLQRow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// =============================================================================
// Purge / retention
// =============================================================================

func (s *Store) PurgeOldTasks(ctx context.Context, completedRetention, failedRetention, cancelledRetention time.Duration) (*tasks.PurgeResult, error) {
	result := &tasks.PurgeResult{}
	nowTime := time.Now()

	completedCutoff := nowTime.Add(-completedRetention).UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `DELETE FROM tasks WHERE status = 'completed' AND completed_at < ?`, completedCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to purge completed tasks: %w", err)
	}
	result.Completed, _ = res.RowsAffected()

	failedCutoff := nowTime.Add(-failedRetention).UTC().Format(time.RFC3339Nano)
	res, err = s.db.ExecContext(ctx, `DELETE FROM tasks WHERE status = 'failed' AND failed_at < ?`, failedCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to purge failed tasks: %w", err)
	}
	result.Failed, _ = res.RowsAffected()

	cancelledCutoff := nowTime.Add(-cancelledRetention).UTC().Format(time.RFC3339Nano)
	res, err = s.db.ExecContext(ctx, `DELETE FROM tasks WHERE status = 'cancelled' AND completed_at < ?`, cancelledCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to purge cancelled tasks: %w", err)
	}
	result.Cancelled, _ = res.RowsAffected()

	return result, nil
}

// =============================================================================
// Internal: scan helpers
// =============================================================================

const taskSelectColumns = `
	task_id, task_type, workspace, implementation, specifier,
	status, priority, created_at, updated_at, scheduled_for,
	started_at, completed_at, failed_at,
	assigned_to, assigned_at,
	assignment_mode, task_category, target_agent_id, target_implementation, target_specifier,
	launch_params, queued_for_startup, parent_agent_id, parent_task_id,
	retry_count, max_retries, next_retry_at,
	error_message, error_type,
	payload, metadata, checkpoint_data,
	schedule_to_start_ms, start_to_close_ms, heartbeat_timeout_ms, schedule_to_close_ms,
	last_heartbeat, heartbeat_details,
	target_topic, source_topic, message_type,
	authority_mode, subject_type, subject_id, root_subject_type, root_subject_id,
	authority_grant_id, root_authority_grant_id, parent_authority_grant_id,
	authority_audience_type, authority_audience_id, authority_delegate_type, authority_delegate_id,
	task_class,
	disconnected_at, grace_window_ms,
	wait_spec, depends_on, context_id, paused_at
`

// taskScanner is satisfied by both *sql.Row and *sql.Rows.
type taskScanner interface {
	Scan(dest ...interface{}) error
}

func scanTaskInto(s taskScanner) (*tasks.Task, error) {
	var task tasks.Task
	var implementation, specifier, targetAgentID, targetImpl, targetSpec, parentAgentID, parentTaskID sql.NullString
	var assignedTo, errorMsg, errorType, targetTopic, sourceTopic, messageType sql.NullString
	var authorityMode, subjectType, subjectID, rootSubjectType, rootSubjectID sql.NullString
	var authorityGrantID, rootAuthorityGrantID, parentAuthorityGrantID sql.NullString
	var authorityAudienceType, authorityAudienceID, authorityDelegateType, authorityDelegateID sql.NullString
	var contextID sql.NullString
	var createdAtStr, updatedAtStr string
	var scheduledForStr, startedAtStr, completedAtStr, failedAtStr, assignedAtStr, nextRetryAtStr, lastHeartbeatStr, disconnectedAtStr sql.NullString
	var pausedAtMs sql.NullInt64
	var scheduleToStart, startToClose, heartbeatTimeout, scheduleToClose sql.NullInt64
	var launchParamsJSON, metadataJSON, checkpointJSON, heartbeatJSON []byte
	var waitSpecJSON, dependsOnJSON sql.NullString
	var queuedForStartup int

	err := s.Scan(
		&task.TaskID, &task.TaskType, &task.Workspace, &implementation, &specifier,
		&task.Status, &task.Priority, &createdAtStr, &updatedAtStr, &scheduledForStr,
		&startedAtStr, &completedAtStr, &failedAtStr,
		&assignedTo, &assignedAtStr,
		&task.AssignmentMode, &task.TaskCategory, &targetAgentID, &targetImpl, &targetSpec,
		&launchParamsJSON, &queuedForStartup, &parentAgentID, &parentTaskID,
		&task.RetryCount, &task.MaxRetries, &nextRetryAtStr,
		&errorMsg, &errorType,
		&task.Payload, &metadataJSON, &checkpointJSON,
		&scheduleToStart, &startToClose, &heartbeatTimeout, &scheduleToClose,
		&lastHeartbeatStr, &heartbeatJSON,
		&targetTopic, &sourceTopic, &messageType,
		&authorityMode, &subjectType, &subjectID, &rootSubjectType, &rootSubjectID,
		&authorityGrantID, &rootAuthorityGrantID, &parentAuthorityGrantID,
		&authorityAudienceType, &authorityAudienceID, &authorityDelegateType, &authorityDelegateID,
		&task.TaskClass,
		&disconnectedAtStr, &task.GraceWindowMs,
		&waitSpecJSON, &dependsOnJSON, &contextID, &pausedAtMs,
	)
	if err != nil {
		return nil, err
	}

	// Boolean from integer
	task.QueuedForStartup = queuedForStartup != 0

	// Inline timestamp parsing
	task.CreatedAt, err = parseTimestamp(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	task.UpdatedAt, err = parseTimestamp(updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}

	if scheduledForStr.Valid {
		t, err := parseTimestamp(scheduledForStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse scheduled_for: %w", err)
		}
		task.ScheduledFor = &t
	}
	if startedAtStr.Valid {
		t, err := parseTimestamp(startedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse started_at: %w", err)
		}
		task.StartedAt = &t
	}
	if completedAtStr.Valid {
		t, err := parseTimestamp(completedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse completed_at: %w", err)
		}
		task.CompletedAt = &t
	}
	if failedAtStr.Valid {
		t, err := parseTimestamp(failedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse failed_at: %w", err)
		}
		task.FailedAt = &t
	}
	if assignedAtStr.Valid {
		t, err := parseTimestamp(assignedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse assigned_at: %w", err)
		}
		task.AssignedAt = &t
	}
	if nextRetryAtStr.Valid {
		t, err := parseTimestamp(nextRetryAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse next_retry_at: %w", err)
		}
		task.NextRetryAt = &t
	}
	if lastHeartbeatStr.Valid {
		t, err := parseTimestamp(lastHeartbeatStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse last_heartbeat: %w", err)
		}
		task.LastHeartbeat = &t
	}
	if disconnectedAtStr.Valid {
		t, err := parseTimestamp(disconnectedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse disconnected_at: %w", err)
		}
		task.DisconnectedAt = &t
	}

	// Nullable strings
	if implementation.Valid {
		task.Implementation = implementation.String
	}
	if specifier.Valid {
		task.Specifier = specifier.String
	}
	if targetAgentID.Valid {
		task.TargetAgentID = targetAgentID.String
	}
	if targetImpl.Valid {
		task.TargetImplementation = targetImpl.String
	}
	if targetSpec.Valid {
		task.TargetSpecifier = targetSpec.String
	}
	if parentAgentID.Valid {
		task.ParentAgentID = parentAgentID.String
	}
	if parentTaskID.Valid {
		task.ParentTaskID = parentTaskID.String
	}
	if assignedTo.Valid {
		task.AssignedTo = assignedTo.String
	}
	if errorMsg.Valid {
		task.ErrorMessage = errorMsg.String
	}
	if errorType.Valid {
		task.ErrorType = errorType.String
	}
	if targetTopic.Valid {
		task.TargetTopic = targetTopic.String
	}
	if sourceTopic.Valid {
		task.SourceTopic = sourceTopic.String
	}
	if messageType.Valid {
		task.MessageType = messageType.String
	}
	if authorityMode.Valid {
		task.Authority.Mode = authorityMode.String
	}
	if subjectType.Valid {
		task.Authority.SubjectType = subjectType.String
	}
	if subjectID.Valid {
		task.Authority.SubjectID = subjectID.String
	}
	if rootSubjectType.Valid {
		task.Authority.RootSubjectType = rootSubjectType.String
	}
	if rootSubjectID.Valid {
		task.Authority.RootSubjectID = rootSubjectID.String
	}
	if authorityGrantID.Valid {
		task.Authority.AuthorityGrantID = authorityGrantID.String
	}
	if rootAuthorityGrantID.Valid {
		task.Authority.RootAuthorityGrantID = rootAuthorityGrantID.String
	}
	if parentAuthorityGrantID.Valid {
		task.Authority.ParentAuthorityGrantID = parentAuthorityGrantID.String
	}
	if authorityAudienceType.Valid {
		task.Authority.AudienceType = authorityAudienceType.String
	}
	if authorityAudienceID.Valid {
		task.Authority.AudienceID = authorityAudienceID.String
	}
	if authorityDelegateType.Valid {
		task.Authority.DelegateType = authorityDelegateType.String
	}
	if authorityDelegateID.Valid {
		task.Authority.DelegateID = authorityDelegateID.String
	}

	// Nullable ints
	if scheduleToStart.Valid {
		task.ScheduleToStartMs = scheduleToStart.Int64
	}
	if startToClose.Valid {
		task.StartToCloseMs = startToClose.Int64
	}
	if heartbeatTimeout.Valid {
		task.HeartbeatTimeoutMs = heartbeatTimeout.Int64
	}
	if scheduleToClose.Valid {
		task.ScheduleToCloseMs = scheduleToClose.Int64
	}

	// JSON fields
	if len(launchParamsJSON) > 0 {
		if err := json.Unmarshal(launchParamsJSON, &task.LaunchParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
		}
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &task.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}
	if len(checkpointJSON) > 0 {
		if err := json.Unmarshal(checkpointJSON, &task.CheckpointData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal checkpoint_data: %w", err)
		}
	}
	if len(heartbeatJSON) > 0 {
		if err := json.Unmarshal(heartbeatJSON, &task.HeartbeatDetails); err != nil {
			return nil, fmt.Errorf("failed to unmarshal heartbeat_details: %w", err)
		}
	}

	// Phase 1: paused-state fields
	if waitSpecJSON.Valid && len(waitSpecJSON.String) > 0 {
		var ws tasks.WaitSpec
		if err := json.Unmarshal([]byte(waitSpecJSON.String), &ws); err != nil {
			return nil, fmt.Errorf("failed to unmarshal wait_spec: %w", err)
		}
		task.WaitSpec = &ws
	}
	if dependsOnJSON.Valid && len(dependsOnJSON.String) > 0 {
		if err := json.Unmarshal([]byte(dependsOnJSON.String), &task.DependsOn); err != nil {
			return nil, fmt.Errorf("failed to unmarshal depends_on: %w", err)
		}
	}
	if contextID.Valid {
		task.ContextID = contextID.String
	}
	if pausedAtMs.Valid {
		t := time.UnixMilli(pausedAtMs.Int64).UTC()
		task.PausedAt = &t
	}

	return &task, nil
}

func scanTask(row *sql.Row) (*tasks.Task, error) {
	return scanTaskInto(row)
}

func (s *Store) queryTasks(ctx context.Context, query string, args ...interface{}) ([]*tasks.Task, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*tasks.Task
	for rows.Next() {
		task, err := scanTaskInto(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

func scanTimer(row *sql.Row) (*tasks.TimerRecord, error) {
	var timer tasks.TimerRecord
	var firesAtStr, createdAtStr string
	var firedAtStr sql.NullString
	var metadataJSON []byte
	var firedInt int

	err := row.Scan(&timer.TimerID, &timer.TaskID, &timer.TimerType, &firesAtStr, &createdAtStr, &firedInt, &firedAtStr, &metadataJSON)
	if err != nil {
		return nil, err
	}

	timer.Fired = firedInt != 0
	timer.FiresAt, err = parseTimestamp(firesAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse fires_at: %w", err)
	}
	timer.CreatedAt, err = parseTimestamp(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse timer created_at: %w", err)
	}
	if firedAtStr.Valid {
		t, err := parseTimestamp(firedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse fired_at: %w", err)
		}
		timer.FiredAt = &t
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &timer.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal timer metadata: %w", err)
		}
	}
	return &timer, nil
}

func scanTimerRows(rows *sql.Rows) ([]*tasks.TimerRecord, error) {
	var timers []*tasks.TimerRecord
	for rows.Next() {
		var timer tasks.TimerRecord
		var firesAtStr, createdAtStr string
		var firedAtStr sql.NullString
		var metadataJSON []byte
		var firedInt int

		err := rows.Scan(&timer.TimerID, &timer.TaskID, &timer.TimerType, &firesAtStr, &createdAtStr, &firedInt, &firedAtStr, &metadataJSON)
		if err != nil {
			return nil, err
		}

		timer.Fired = firedInt != 0
		timer.FiresAt, err = parseTimestamp(firesAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse fires_at: %w", err)
		}
		timer.CreatedAt, err = parseTimestamp(createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse timer created_at: %w", err)
		}
		if firedAtStr.Valid {
			t, err := parseTimestamp(firedAtStr.String)
			if err != nil {
				return nil, fmt.Errorf("parse fired_at: %w", err)
			}
			timer.FiredAt = &t
		}
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &timer.Metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal timer metadata: %w", err)
			}
		}
		timers = append(timers, &timer)
	}
	return timers, rows.Err()
}

func scanDLQRow(rows *sql.Rows) (*tasks.DLQRecord, error) {
	var record tasks.DLQRecord
	var resolvedBy, resolutionNotes sql.NullString
	var reprocessedAtStr sql.NullString
	var originalMetaJSON, failureDetailsJSON []byte
	var enqueuedAtStr, lastAttemptAtStr string
	var resolvedInt int

	err := rows.Scan(
		&record.DLQMessageID,
		&record.OriginalTaskID,
		&record.Category,
		&record.Workspace,
		&record.OriginalPayload,
		&originalMetaJSON,
		&record.FailureReason,
		&failureDetailsJSON,
		&enqueuedAtStr,
		&record.AttemptCount,
		&lastAttemptAtStr,
		&reprocessedAtStr,
		&resolvedInt,
		&resolvedBy,
		&resolutionNotes,
	)
	if err != nil {
		return nil, err
	}

	record.Resolved = resolvedInt != 0

	record.EnqueuedAt, err = parseTimestamp(enqueuedAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse enqueued_at: %w", err)
	}
	record.LastAttemptAt, err = parseTimestamp(lastAttemptAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse last_attempt_at: %w", err)
	}

	if resolvedBy.Valid {
		record.ResolvedBy = resolvedBy.String
	}
	if resolutionNotes.Valid {
		record.ResolutionNotes = resolutionNotes.String
	}
	if reprocessedAtStr.Valid {
		t, err := parseTimestamp(reprocessedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse reprocessed_at: %w", err)
		}
		record.ReprocessedAt = &t
	}

	if originalMetaJSON != nil {
		if err := json.Unmarshal(originalMetaJSON, &record.OriginalMeta); err != nil {
			return nil, fmt.Errorf("failed to unmarshal original metadata: %w", err)
		}
	}
	if failureDetailsJSON != nil {
		if err := json.Unmarshal(failureDetailsJSON, &record.FailureDetails); err != nil {
			return nil, fmt.Errorf("failed to unmarshal failure details: %w", err)
		}
	}

	return &record, nil
}

// =============================================================================
// Admin workspace queries
// =============================================================================

func (s *Store) ListDistinctTaskWorkspaces(ctx context.Context) ([]*tasks.WorkspaceTaskSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT workspace, MIN(created_at) AS created_at, COUNT(*) AS task_count
		FROM tasks
		WHERE workspace IS NOT NULL AND workspace != ''
		GROUP BY workspace
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*tasks.WorkspaceTaskSummary
	for rows.Next() {
		var ws tasks.WorkspaceTaskSummary
		var createdAtStr string
		if err := rows.Scan(&ws.Workspace, &createdAtStr, &ws.TaskCount); err != nil {
			return nil, err
		}
		ws.CreatedAt, err = parseTimestamp(createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse created_at for workspace %q: %w", ws.Workspace, err)
		}
		results = append(results, &ws)
	}
	return results, rows.Err()
}

func (s *Store) GetWorkspaceTaskStats(ctx context.Context, workspaceID string) (*tasks.WorkspaceTaskStats, error) {
	var stats tasks.WorkspaceTaskStats
	var count int64
	var createdAtStr sql.NullString

	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(created_at)
		FROM tasks
		WHERE workspace = ?
	`, workspaceID).Scan(&count, &createdAtStr)
	if err != nil {
		return nil, err
	}

	stats.TaskCount = count
	if createdAtStr.Valid {
		stats.CreatedAt, err = parseTimestamp(createdAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
	}
	return &stats, nil
}

// =============================================================================
// Phase 1: Paused-state lifecycle operations
// =============================================================================

func (s *Store) PauseTask(ctx context.Context, taskID string, to tasks.TaskStatus, spec *tasks.WaitSpec) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("PauseTask: get task: %w", err)
	}
	if err := tasks.ValidateTransition(task.Status, to); err != nil {
		return fmt.Errorf("PauseTask: %w", err)
	}
	var waitSpecStr, dependsOnStr sql.NullString
	if spec != nil {
		b, merr := json.Marshal(spec)
		if merr != nil {
			return fmt.Errorf("PauseTask: marshal wait_spec: %w", merr)
		}
		waitSpecStr = sql.NullString{String: string(b), Valid: true}
		// Mirror wait_spec.depends_on into the top-level depends_on column so
		// ListTasksWaitingOnDependency can do its reverse-lookup without
		// scanning JSON.
		if len(spec.DependsOn) > 0 {
			db, derr := json.Marshal(spec.DependsOn)
			if derr != nil {
				return fmt.Errorf("PauseTask: marshal depends_on: %w", derr)
			}
			dependsOnStr = sql.NullString{String: string(db), Valid: true}
		}
	}
	nowMs := time.Now().UTC().UnixMilli()
	_, err = s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, wait_spec = ?, depends_on = ?, paused_at = ?, updated_at = ? WHERE task_id = ?`,
		string(to), waitSpecStr, dependsOnStr, nowMs, time.Now().UTC().Format(time.RFC3339Nano), taskID,
	)
	return err
}

func (s *Store) ResumeTask(ctx context.Context, taskID string, to tasks.TaskStatus) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("ResumeTask: get task: %w", err)
	}
	if err := tasks.ValidateTransition(task.Status, to); err != nil {
		return fmt.Errorf("ResumeTask: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, wait_spec = NULL, depends_on = NULL, paused_at = NULL, updated_at = ? WHERE task_id = ?`,
		string(to), time.Now().UTC().Format(time.RFC3339Nano), taskID,
	)
	return err
}

func (s *Store) RejectTask(ctx context.Context, taskID string, reason string) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("RejectTask: get task: %w", err)
	}
	if err := tasks.ValidateTransition(task.Status, tasks.TaskStatusRejected); err != nil {
		return fmt.Errorf("RejectTask: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, error_message = ?, wait_spec = NULL, depends_on = NULL, paused_at = NULL, updated_at = ? WHERE task_id = ?`,
		string(tasks.TaskStatusRejected), reason, time.Now().UTC().Format(time.RFC3339Nano), taskID,
	)
	return err
}

func (s *Store) ListWaitingTasks(ctx context.Context, limit int) ([]*tasks.Task, error) {
	if limit <= 0 {
		limit = 500
	}
	query := `SELECT ` + taskSelectColumns + ` FROM tasks
		WHERE status IN (?, ?, ?, ?)
		ORDER BY paused_at ASC
		LIMIT ?`
	return s.queryTasks(ctx, query,
		string(tasks.TaskStatusWaitingInput),
		string(tasks.TaskStatusWaitingAuthority),
		string(tasks.TaskStatusWaitingDependency),
		string(tasks.TaskStatusHibernated),
		limit,
	)
}

// ListTasksWaitingOnDependency returns waiting_dependency tasks whose depends_on
// JSON array contains dependencyTaskID. SQLite has no native JSONB containment
// operator, so we use a LIKE search on the TEXT column as a fast pre-filter;
// false positives are filtered out in Go.
func (s *Store) ListTasksWaitingOnDependency(ctx context.Context, dependencyTaskID string) ([]*tasks.Task, error) {
	query := `SELECT ` + taskSelectColumns + ` FROM tasks
		WHERE status = ?
		  AND depends_on LIKE ?`
	// Use %"<id>"% so we only match the quoted string, not a substring of another ID.
	pattern := `%"` + dependencyTaskID + `"%`
	candidates, err := s.queryTasks(ctx, query, string(tasks.TaskStatusWaitingDependency), pattern)
	if err != nil {
		return nil, err
	}
	// Post-filter: verify the ID is actually in DependsOn (guards against LIKE false-positives).
	var result []*tasks.Task
	for _, t := range candidates {
		for _, dep := range t.DependsOn {
			if dep == dependencyTaskID {
				result = append(result, t)
				break
			}
		}
	}
	return result, nil
}

func (s *Store) ListTasksByContext(ctx context.Context, contextID string, limit int) ([]*tasks.Task, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT ` + taskSelectColumns + ` FROM tasks WHERE context_id = ? ORDER BY created_at DESC LIMIT ?`
	return s.queryTasks(ctx, query, contextID, limit)
}

// =============================================================================
// Internal: timestamp parsing and helpers
// =============================================================================

// timestampFormats lists the on-disk encodings we may encounter. The native
// impl writes RFC3339Nano exclusively, but we also handle the formats that
// SQLite's strftime and CURRENT_TIMESTAMP defaults produce.
var timestampFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000",       // strftime('%Y-%m-%dT%H:%M:%f', ...)
	"2006-01-02 15:04:05.999999999", // CURRENT_TIMESTAMP with fractional
	"2006-01-02 15:04:05",           // CURRENT_TIMESTAMP without fractional
}

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

func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullTimeStr(t *time.Time) sql.NullString {
	if t == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: t.UTC().Format(time.RFC3339Nano), Valid: true}
}

func nullInt64(i int64) sql.NullInt64 {
	if i == 0 {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: i, Valid: true}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// =============================================================================
// Migration runner
// =============================================================================

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
