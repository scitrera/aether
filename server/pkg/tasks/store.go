package tasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// TaskStore provides database operations for the unified task schema
type TaskStore struct {
	db *sql.DB
}

// NewTaskStore creates a new task store
func NewTaskStore(db *sql.DB) *TaskStore {
	return &TaskStore{db: db}
}

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
	disconnected_at, grace_window_ms
`

// =============================================================================
// Task CRUD Operations
// =============================================================================

// CreateTask creates a new task in the database
func (s *TaskStore) CreateTask(ctx context.Context, task *Task) error {
	// Set defaults
	if task.TaskID == "" {
		task.TaskID = uuid.New().String()
	}
	if task.Status == "" {
		task.Status = TaskStatusPending
	}
	if task.AssignmentMode == "" {
		task.AssignmentMode = AssignmentModeSelfAssign
	}
	if task.TaskCategory == "" {
		task.TaskCategory = TaskCategoryRegular
	}
	if task.MaxRetries == 0 {
		task.MaxRetries = 3
	}

	// Marshal JSONB fields
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

	query := `
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
			task_class, grace_window_ms
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
			$29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41,
			$42, $43
		)
	`

	_, err = s.db.ExecContext(ctx, query,
		task.TaskID,
		task.TaskType,
		task.Workspace,
		nullString(task.Implementation),
		nullString(task.Specifier),
		task.Status,
		task.Priority,
		nullTime(task.ScheduledFor),
		task.AssignmentMode,
		task.TaskCategory,
		nullString(task.TargetAgentID),
		nullString(task.TargetImplementation),
		nullString(task.TargetSpecifier),
		launchParamsJSON,
		task.QueuedForStartup,
		nullString(task.ParentAgentID),
		nullString(task.ParentTaskID),
		task.MaxRetries,
		task.Payload,
		metadataJSON,
		checkpointJSON,
		nullInt64(task.ScheduleToStartMs),
		nullInt64(task.StartToCloseMs),
		nullInt64(task.HeartbeatTimeoutMs),
		nullInt64(task.ScheduleToCloseMs),
		heartbeatJSON,
		nullString(task.TargetTopic),
		nullString(task.SourceTopic),
		nullString(task.MessageType),
		nullString(task.Authority.Mode),
		nullString(task.Authority.SubjectType),
		nullString(task.Authority.SubjectID),
		nullString(task.Authority.RootSubjectType),
		nullString(task.Authority.RootSubjectID),
		nullString(task.Authority.AuthorityGrantID),
		nullString(task.Authority.RootAuthorityGrantID),
		nullString(task.Authority.ParentAuthorityGrantID),
		nullString(task.Authority.AudienceType),
		nullString(task.Authority.AudienceID),
		nullString(task.Authority.DelegateType),
		nullString(task.Authority.DelegateID),
		task.TaskClass,
		task.GraceWindowMs,
	)

	return err
}

// GetTask retrieves a task by ID
func (s *TaskStore) GetTask(ctx context.Context, taskID string) (*Task, error) {
	query := `
		SELECT ` + taskSelectColumns + `
		FROM tasks
		WHERE task_id = $1
	`

	return s.scanTask(s.db.QueryRowContext(ctx, query, taskID))
}

// UpdateTaskStatus updates the status of a task
func (s *TaskStore) UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus) error {
	query := `UPDATE tasks SET status = $1 WHERE task_id = $2`
	_, err := s.db.ExecContext(ctx, query, status, taskID)
	return err
}

// AssignTask assigns a task to a worker
func (s *TaskStore) AssignTask(ctx context.Context, taskID, workerIdentity string) error {
	now := time.Now()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `
		UPDATE tasks
		SET status = 'assigned', assigned_to = $1, assigned_at = $2
		WHERE task_id = $3 AND status = 'pending'
	`

	result, err := tx.ExecContext(ctx, query, workerIdentity, now, taskID)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in pending state", taskID)
	}

	// Record in assignment history
	_, err = tx.ExecContext(ctx, `
		INSERT INTO task_assignments (task_id, worker_identity, assigned_at)
		VALUES ($1, $2, $3)
	`, taskID, workerIdentity, now)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// UpdateTaskMetadata replaces the metadata JSON for a task.
func (s *TaskStore) UpdateTaskMetadata(ctx context.Context, taskID string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		UPDATE tasks
		SET metadata = $1, updated_at = NOW()
		WHERE task_id = $2
	`
	result, err := s.db.ExecContext(ctx, query, metadataJSON, taskID)
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

// UpdateTaskAuthority updates the first-class authority lineage fields and
// metadata for a task in one write so grant persistence does not depend on
// metadata-only storage.
func (s *TaskStore) UpdateTaskAuthority(ctx context.Context, taskID string, authority TaskAuthorityInfo, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		UPDATE tasks
		SET metadata = $1,
			authority_mode = $2,
			subject_type = $3,
			subject_id = $4,
			root_subject_type = $5,
			root_subject_id = $6,
			authority_grant_id = $7,
			root_authority_grant_id = $8,
			parent_authority_grant_id = $9,
			authority_audience_type = $10,
			authority_audience_id = $11,
			authority_delegate_type = $12,
			authority_delegate_id = $13,
			updated_at = NOW()
		WHERE task_id = $14
	`
	result, err := s.db.ExecContext(ctx, query,
		metadataJSON,
		nullString(authority.Mode),
		nullString(authority.SubjectType),
		nullString(authority.SubjectID),
		nullString(authority.RootSubjectType),
		nullString(authority.RootSubjectID),
		nullString(authority.AuthorityGrantID),
		nullString(authority.RootAuthorityGrantID),
		nullString(authority.ParentAuthorityGrantID),
		nullString(authority.AudienceType),
		nullString(authority.AudienceID),
		nullString(authority.DelegateType),
		nullString(authority.DelegateID),
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

// StartingTask marks a task as starting (orchestrator is launching the job)
// This transitions a task from 'assigned' to 'starting' state.
func (s *TaskStore) StartingTask(ctx context.Context, taskID string) error {
	query := `
		UPDATE tasks
		SET status = 'starting'
		WHERE task_id = $1 AND status = 'assigned'
	`
	result, err := s.db.ExecContext(ctx, query, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not in assigned state", taskID)
	}
	return nil
}

// StartTask marks a task as running (idempotent for reconnection scenarios)
// This transitions a task from 'assigned', 'starting', or 'running' (reconnect) to 'running'.
func (s *TaskStore) StartTask(ctx context.Context, taskID string) error {
	now := time.Now()
	// Only update started_at if transitioning from assigned/starting; running->running is a no-op
	query := `
		UPDATE tasks
		SET status = 'running', started_at = COALESCE(started_at, $1)
		WHERE task_id = $2 AND status IN ('assigned', 'starting', 'running')
	`
	result, err := s.db.ExecContext(ctx, query, now, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not in assigned, starting, or running state", taskID)
	}
	return nil
}

// StartTaskWithAgent marks a task as running and records the agent identity.
// This is used when an orchestrated agent connects - we store the agent identity
// so the task can be reconciled if the agent disconnects unexpectedly.
func (s *TaskStore) StartTaskWithAgent(ctx context.Context, taskID, agentIdentity string) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'running',
			started_at = COALESCE(started_at, $1),
			target_agent_id = $2
		WHERE task_id = $3 AND status IN ('assigned', 'starting', 'running')
	`
	result, err := s.db.ExecContext(ctx, query, now, agentIdentity, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not in assigned, starting, or running state", taskID)
	}
	return nil
}

// CompleteTask marks a task as completed
func (s *TaskStore) CompleteTask(ctx context.Context, taskID string) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'completed', completed_at = $1
		WHERE task_id = $2
	`
	result, err := s.db.ExecContext(ctx, query, now, taskID)
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

// FailTask marks a task as failed
func (s *TaskStore) FailTask(ctx context.Context, taskID, errorMsg string) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'failed', failed_at = $1, error_message = $2, retry_count = retry_count + 1
		WHERE task_id = $3
	`
	result, err := s.db.ExecContext(ctx, query, now, errorMsg, taskID)
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

// FailTaskWithRetry marks a task as failed and schedules a retry
func (s *TaskStore) FailTaskWithRetry(ctx context.Context, taskID, errorType, errorMsg string, nextRetry *time.Time) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'failed',
			failed_at = $1,
			error_type = $2,
			error_message = $3,
			next_retry_at = $4,
			retry_count = retry_count + 1
		WHERE task_id = $5
	`
	_, err := s.db.ExecContext(ctx, query, now, errorType, errorMsg, nullTime(nextRetry), taskID)
	return err
}

// CancelTask marks a task as cancelled
func (s *TaskStore) CancelTask(ctx context.Context, taskID string) error {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'cancelled',
			completed_at = $1,
			error_type = 'CANCELLED',
			error_message = 'Task cancelled'
		WHERE task_id = $2 AND status NOT IN ('completed', 'failed', 'cancelled')
	`
	result, err := s.db.ExecContext(ctx, query, now, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or already completed/failed/cancelled", taskID)
	}
	return nil
}

// WriteToDLQ writes a failed task to the dead letter queue
func (s *TaskStore) WriteToDLQ(ctx context.Context, dlqRecord *DLQRecord) error {
	// Set defaults
	if dlqRecord.DLQMessageID == "" {
		dlqRecord.DLQMessageID = uuid.New().String()
	}
	if dlqRecord.EnqueuedAt.IsZero() {
		dlqRecord.EnqueuedAt = time.Now()
	}

	// Marshal JSONB fields
	originalMetaJSON, err := json.Marshal(dlqRecord.OriginalMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal original metadata: %w", err)
	}
	failureDetailsJSON, err := json.Marshal(dlqRecord.FailureDetails)
	if err != nil {
		return fmt.Errorf("failed to marshal failure details: %w", err)
	}

	query := `
		INSERT INTO dlq (
			dlq_message_id, original_task_id, category, workspace,
			original_payload, original_metadata, failure_reason, failure_details,
			enqueued_at, attempt_count, last_attempt_at,
			reprocessed_at, resolved, resolved_by, resolution_notes
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
	`

	_, err = s.db.ExecContext(ctx, query,
		dlqRecord.DLQMessageID,
		dlqRecord.OriginalTaskID,
		dlqRecord.Category,
		dlqRecord.Workspace,
		dlqRecord.OriginalPayload,
		originalMetaJSON,
		dlqRecord.FailureReason,
		failureDetailsJSON,
		dlqRecord.EnqueuedAt,
		dlqRecord.AttemptCount,
		dlqRecord.LastAttemptAt,
		nullTime(dlqRecord.ReprocessedAt),
		dlqRecord.Resolved,
		nullString(dlqRecord.ResolvedBy),
		nullString(dlqRecord.ResolutionNotes),
	)

	return err
}

// GetDLQTasks retrieves DLQ records with optional filtering
func (s *TaskStore) GetDLQTasks(ctx context.Context, workspace string, category string, limit int, offset int) ([]*DLQRecord, error) {
	if limit == 0 {
		limit = 100
	}

	query := `
		SELECT
			dlq_message_id, original_task_id, category, workspace,
			original_payload, original_metadata, failure_reason, failure_details,
			enqueued_at, attempt_count, last_attempt_at,
			reprocessed_at, resolved, resolved_by, resolution_notes
		FROM dlq
		WHERE 1=1
	`

	args := []interface{}{}
	argNum := 1

	if workspace != "" {
		query += fmt.Sprintf(" AND workspace = $%d", argNum)
		args = append(args, workspace)
		argNum++
	}
	if category != "" {
		query += fmt.Sprintf(" AND category = $%d", argNum)
		args = append(args, category)
		argNum++
	}

	query += " ORDER BY enqueued_at DESC"
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argNum, argNum+1)
	args = append(args, limit, offset)

	return s.queryDLQRecords(ctx, query, args...)
}

// queryDLQRecords is a helper to query DLQ records
func (s *TaskStore) queryDLQRecords(ctx context.Context, query string, args ...interface{}) ([]*DLQRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*DLQRecord
	for rows.Next() {
		record, err := s.scanDLQRecordRow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, rows.Err()
}

// scanDLQRecordRow scans a DLQ record from sql.Rows
func (s *TaskStore) scanDLQRecordRow(rows *sql.Rows) (*DLQRecord, error) {
	var record DLQRecord
	var resolvedBy, resolutionNotes sql.NullString
	var reprocessedAt sql.NullTime
	var originalMetaJSON, failureDetailsJSON []byte

	err := rows.Scan(
		&record.DLQMessageID,
		&record.OriginalTaskID,
		&record.Category,
		&record.Workspace,
		&record.OriginalPayload,
		&originalMetaJSON,
		&record.FailureReason,
		&failureDetailsJSON,
		&record.EnqueuedAt,
		&record.AttemptCount,
		&record.LastAttemptAt,
		&reprocessedAt,
		&record.Resolved,
		&resolvedBy,
		&resolutionNotes,
	)
	if err != nil {
		return nil, err
	}

	// Handle nullable strings
	if resolvedBy.Valid {
		record.ResolvedBy = resolvedBy.String
	}
	if resolutionNotes.Valid {
		record.ResolutionNotes = resolutionNotes.String
	}

	// Handle nullable time
	if reprocessedAt.Valid {
		record.ReprocessedAt = &reprocessedAt.Time
	}

	// Unmarshal JSONB fields
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

// RetryTask requeues a failed task for retry
func (s *TaskStore) RetryTask(ctx context.Context, taskID string) error {
	query := `
		UPDATE tasks
		SET status = 'pending',
			started_at = NULL,
			failed_at = NULL,
			assigned_to = NULL,
			assigned_at = NULL,
			next_retry_at = NULL,
			error_message = NULL,
			error_type = NULL
		WHERE task_id = $1 AND status IN ('failed', 'cancelled')
	`
	result, err := s.db.ExecContext(ctx, query, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in retryable state", taskID)
	}
	return nil
}

// RescheduleTaskAt schedules a failed task for retry at a specific time
func (s *TaskStore) RescheduleTaskAt(ctx context.Context, taskID string, retryAt time.Time) error {
	query := `
		UPDATE tasks
		SET next_retry_at = $1
		WHERE task_id = $2 AND status = 'failed'
	`
	result, err := s.db.ExecContext(ctx, query, retryAt, taskID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("task %s not found or not in failed state", taskID)
	}
	return nil
}

// PurgeResult contains the counts of tasks purged by status
type PurgeResult struct {
	Completed int64
	Failed    int64
	Cancelled int64
}

// Total returns the total number of tasks purged
func (p PurgeResult) Total() int64 {
	return p.Completed + p.Failed + p.Cancelled
}

// PurgeOldTasks deletes tasks older than the specified retention periods.
// Each retention duration specifies how long tasks of that status should be kept.
// Tasks are deleted based on their completion/failure/cancellation time.
// Returns the count of tasks deleted by status.
func (s *TaskStore) PurgeOldTasks(ctx context.Context, completedRetention, failedRetention, cancelledRetention time.Duration) (*PurgeResult, error) {
	result := &PurgeResult{}
	now := time.Now()

	// Delete old completed tasks
	completedCutoff := now.Add(-completedRetention)
	query := `DELETE FROM tasks WHERE status = 'completed' AND completed_at < $1`
	res, err := s.db.ExecContext(ctx, query, completedCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to purge completed tasks: %w", err)
	}
	result.Completed, _ = res.RowsAffected()

	// Delete old failed tasks
	failedCutoff := now.Add(-failedRetention)
	query = `DELETE FROM tasks WHERE status = 'failed' AND failed_at < $1`
	res, err = s.db.ExecContext(ctx, query, failedCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to purge failed tasks: %w", err)
	}
	result.Failed, _ = res.RowsAffected()

	// Delete old cancelled tasks (use completed_at as cancellation time)
	cancelledCutoff := now.Add(-cancelledRetention)
	query = `DELETE FROM tasks WHERE status = 'cancelled' AND completed_at < $1`
	res, err = s.db.ExecContext(ctx, query, cancelledCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to purge cancelled tasks: %w", err)
	}
	result.Cancelled, _ = res.RowsAffected()

	return result, nil
}

// =============================================================================
// Task Query Operations
// =============================================================================

// ListTasks returns tasks with optional filtering
func (s *TaskStore) ListTasks(ctx context.Context, filter *TaskFilter) ([]*Task, error) {
	if filter == nil {
		filter = &TaskFilter{}
	}
	if filter.Limit == 0 {
		filter.Limit = 100
	}

	query := `
		SELECT ` + taskSelectColumns + `
		FROM tasks
		WHERE 1=1
	`

	args := []interface{}{}
	argNum := 1

	if len(filter.Statuses) > 0 {
		// Multiple statuses: IN clause
		placeholders := make([]string, len(filter.Statuses))
		for i, s := range filter.Statuses {
			placeholders[i] = fmt.Sprintf("$%d", argNum)
			args = append(args, s)
			argNum++
		}
		query += " AND status IN (" + strings.Join(placeholders, ", ") + ")"
	} else if filter.Status != nil {
		query += fmt.Sprintf(" AND status = $%d", argNum)
		args = append(args, *filter.Status)
		argNum++
	}
	if filter.Workspace != "" {
		query += fmt.Sprintf(" AND workspace = $%d", argNum)
		args = append(args, filter.Workspace)
		argNum++
	}
	if filter.TaskType != "" {
		query += fmt.Sprintf(" AND task_type = $%d", argNum)
		args = append(args, filter.TaskType)
		argNum++
	}
	if filter.TaskCategory != nil {
		query += fmt.Sprintf(" AND task_category = $%d", argNum)
		args = append(args, *filter.TaskCategory)
		argNum++
	}
	if filter.AssignmentMode != nil {
		query += fmt.Sprintf(" AND assignment_mode = $%d", argNum)
		args = append(args, *filter.AssignmentMode)
		argNum++
	}
	if filter.AssignedTo != "" {
		query += fmt.Sprintf(" AND assigned_to = $%d", argNum)
		args = append(args, filter.AssignedTo)
		argNum++
	}
	if filter.TargetAgentID != "" {
		query += fmt.Sprintf(" AND target_agent_id = $%d", argNum)
		args = append(args, filter.TargetAgentID)
		argNum++
	}
	if filter.TargetImplementation != "" {
		query += fmt.Sprintf(" AND target_implementation = $%d", argNum)
		args = append(args, filter.TargetImplementation)
		argNum++
	}
	if filter.QueuedForStartup != nil {
		query += fmt.Sprintf(" AND queued_for_startup = $%d", argNum)
		args = append(args, *filter.QueuedForStartup)
		argNum++
	}
	if filter.SubjectType != "" {
		query += fmt.Sprintf(" AND subject_type = $%d", argNum)
		args = append(args, filter.SubjectType)
		argNum++
	}
	if filter.SubjectID != "" {
		query += fmt.Sprintf(" AND subject_id = $%d", argNum)
		args = append(args, filter.SubjectID)
		argNum++
	}
	if filter.AuthorityMode != "" {
		query += fmt.Sprintf(" AND authority_mode = $%d", argNum)
		args = append(args, filter.AuthorityMode)
		argNum++
	}
	if filter.AuthorityGrantID != "" {
		query += fmt.Sprintf(" AND authority_grant_id = $%d", argNum)
		args = append(args, filter.AuthorityGrantID)
		argNum++
	}
	if filter.RootAuthorityGrantID != "" {
		query += fmt.Sprintf(" AND root_authority_grant_id = $%d", argNum)
		args = append(args, filter.RootAuthorityGrantID)
		argNum++
	}
	if filter.ParentTaskID != "" {
		query += fmt.Sprintf(" AND parent_task_id = $%d", argNum)
		args = append(args, filter.ParentTaskID)
		argNum++
	}
	if filter.TaskClass != 0 {
		query += fmt.Sprintf(" AND task_class = $%d", argNum)
		args = append(args, filter.TaskClass)
		argNum++
	}
	if len(filter.ExcludeTaskClasses) > 0 {
		placeholders := make([]string, len(filter.ExcludeTaskClasses))
		for i, c := range filter.ExcludeTaskClasses {
			placeholders[i] = fmt.Sprintf("$%d", argNum)
			args = append(args, c)
			argNum++
		}
		query += " AND (task_class IS NULL OR task_class NOT IN (" + strings.Join(placeholders, ", ") + "))"
	}

	query += " ORDER BY created_at DESC"
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argNum, argNum+1)
	args = append(args, filter.Limit, filter.Offset)

	return s.queryTasks(ctx, query, args...)
}

// GetTasksByStatus returns tasks with a specific status
func (s *TaskStore) GetTasksByStatus(ctx context.Context, status TaskStatus, limit int) ([]*Task, error) {
	statusPtr := status
	return s.ListTasks(ctx, &TaskFilter{Status: &statusPtr, Limit: limit})
}

// GetQueuedTasksForAgent retrieves tasks queued for a specific agent
func (s *TaskStore) GetQueuedTasksForAgent(ctx context.Context, agentID string) ([]*Task, error) {
	queuedTrue := true
	return s.ListTasks(ctx, &TaskFilter{
		TargetAgentID:    agentID,
		QueuedForStartup: &queuedTrue,
	})
}

// GetWorkspaceTasks retrieves tasks for a workspace
func (s *TaskStore) GetWorkspaceTasks(ctx context.Context, workspace string, orchestratedOnly bool) ([]*Task, error) {
	filter := &TaskFilter{Workspace: workspace}
	if orchestratedOnly {
		cat := TaskCategoryOrchestrated
		filter.TaskCategory = &cat
	}
	return s.ListTasks(ctx, filter)
}

// GetAgentTasks retrieves all tasks assigned to or targeted at an agent
func (s *TaskStore) GetAgentTasks(ctx context.Context, agentID string) ([]*Task, error) {
	query := `
		SELECT ` + taskSelectColumns + `
		FROM tasks
		WHERE assigned_to = $1 OR target_agent_id = $1
		ORDER BY created_at DESC
		LIMIT 1000
	`
	return s.queryTasks(ctx, query, agentID)
}

// GetTasksNeedingRetry returns tasks that are ready for retry
func (s *TaskStore) GetTasksNeedingRetry(ctx context.Context, beforeTime time.Time, limit int) ([]*Task, error) {
	query := `
		SELECT ` + taskSelectColumns + `
		FROM tasks
		WHERE status = 'failed'
			AND next_retry_at IS NOT NULL
			AND next_retry_at <= $1
			AND retry_count < max_retries
		ORDER BY next_retry_at ASC
		LIMIT $2
	`
	return s.queryTasks(ctx, query, beforeTime, limit)
}

// GetTaskCounts returns counts of tasks by status
func (s *TaskStore) GetTaskCounts(ctx context.Context) (*TaskCounts, error) {
	query := `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'pending') as pending,
			COUNT(*) FILTER (WHERE status = 'assigned') as assigned,
			COUNT(*) FILTER (WHERE status = 'starting') as starting,
			COUNT(*) FILTER (WHERE status = 'running') as running,
			COUNT(*) FILTER (WHERE status = 'completed') as completed,
			COUNT(*) FILTER (WHERE status = 'failed') as failed
		FROM tasks
	`
	var counts TaskCounts
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
// Orchestration-specific Operations
// =============================================================================

// MarkTaskNotQueued clears the queued_for_startup flag
func (s *TaskStore) MarkTaskNotQueued(ctx context.Context, taskID string) error {
	query := `UPDATE tasks SET queued_for_startup = false WHERE task_id = $1`
	_, err := s.db.ExecContext(ctx, query, taskID)
	return err
}

// ClaimPoolTask atomically claims a pending pool task for a worker.
// Returns (true, nil) if the claim succeeded, (false, nil) if another worker already claimed it.
func (s *TaskStore) ClaimPoolTask(ctx context.Context, taskID, workerIdentity string) (bool, error) {
	now := time.Now()
	query := `
		UPDATE tasks
		SET status = 'assigned', assigned_to = $1, assigned_at = $2, queued_for_startup = false
		WHERE task_id = $3 AND status = 'pending' AND assignment_mode = 'pool'
	`
	result, err := s.db.ExecContext(ctx, query, workerIdentity, now, taskID)
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

// UnassignPoolTask rolls back a pool task assignment, returning it to pending state.
// Used when delivery to the worker fails after claiming.
func (s *TaskStore) UnassignPoolTask(ctx context.Context, taskID string) error {
	query := `
		UPDATE tasks
		SET status = 'pending', assigned_to = NULL, assigned_at = NULL, queued_for_startup = true
		WHERE task_id = $1 AND status = 'assigned' AND assignment_mode = 'pool'
	`
	_, err := s.db.ExecContext(ctx, query, taskID)
	return err
}

// GetPendingPoolTasks returns pending pool tasks for a given implementation and workspace.
func (s *TaskStore) GetPendingPoolTasks(ctx context.Context, implementation, workspace string) ([]*Task, error) {
	pendingStatus := TaskStatusPending
	poolMode := AssignmentModePool
	queuedTrue := true
	return s.ListTasks(ctx, &TaskFilter{
		Status:               &pendingStatus,
		AssignmentMode:       &poolMode,
		TargetImplementation: implementation,
		Workspace:            workspace,
		QueuedForStartup:     &queuedTrue,
		Limit:                100,
	})
}

// HasActiveStartupTask checks if there's an active startup task for the given
// target implementation, workspace, and specifier. An active task is one in
// pending, assigned, starting, or running state.
//
// The targetSpecifier dimension lets per-user singleton agents (e.g. CoworkAgent
// at workspace="_apps", specifier=<user_id>) coexist without colliding on the
// (implementation, workspace) key. Pass "" for implementations that do not use
// the specifier dimension — rows will match only when the stored specifier is
// also empty/NULL, preserving the legacy single-agent-per-workspace behavior.
func (s *TaskStore) HasActiveStartupTask(ctx context.Context, targetImplementation, workspace, targetSpecifier string) (bool, string, error) {
	query := `
		SELECT task_id FROM tasks
		WHERE task_type = 'agent_startup'
			AND target_implementation = $1
			AND workspace = $2
			AND COALESCE(target_specifier, '') = $3
			AND status IN ('pending', 'assigned', 'starting', 'running')
		LIMIT 1
	`
	var taskID string
	err := s.db.QueryRowContext(ctx, query, targetImplementation, workspace, targetSpecifier).Scan(&taskID)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, taskID, nil
}

// UpdateCheckpoint updates the checkpoint data for a task
func (s *TaskStore) UpdateCheckpoint(ctx context.Context, taskID string, checkpointData map[string]interface{}) error {
	checkpointJSON, err := json.Marshal(checkpointData)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint_data: %w", err)
	}
	query := `UPDATE tasks SET checkpoint_data = $1 WHERE task_id = $2`
	_, err = s.db.ExecContext(ctx, query, checkpointJSON, taskID)
	return err
}

// UpdateHeartbeat updates the last heartbeat time and details for a task
func (s *TaskStore) UpdateHeartbeat(ctx context.Context, taskID string, details map[string]interface{}) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat_details: %w", err)
	}
	query := `UPDATE tasks SET last_heartbeat = NOW(), heartbeat_details = $1 WHERE task_id = $2`
	_, err = s.db.ExecContext(ctx, query, detailsJSON, taskID)
	return err
}

// =============================================================================
// Timer Operations
// =============================================================================

// CreateTimer inserts a new timer record for a task
func (s *TaskStore) CreateTimer(ctx context.Context, timer *TimerRecord) error {
	if timer.TimerID == "" {
		timer.TimerID = uuid.New().String()
	}
	metadataJSON, err := json.Marshal(timer.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO task_timers (timer_id, task_id, timer_type, fires_at, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err = s.db.ExecContext(ctx, query,
		timer.TimerID,
		timer.TaskID,
		timer.TimerType,
		timer.FiresAt,
		metadataJSON,
	)
	return err
}

// GetTimer retrieves a timer by ID
func (s *TaskStore) GetTimer(ctx context.Context, timerID string) (*TimerRecord, error) {
	query := `
		SELECT timer_id, task_id, timer_type, fires_at, created_at, fired, fired_at, metadata
		FROM task_timers
		WHERE timer_id = $1
	`
	row := s.db.QueryRowContext(ctx, query, timerID)
	return s.scanTimer(row)
}

// GetTimersForTask returns all active timers for a specific task
func (s *TaskStore) GetTimersForTask(ctx context.Context, taskID string) ([]*TimerRecord, error) {
	query := `
		SELECT timer_id, task_id, timer_type, fires_at, created_at, fired, fired_at, metadata
		FROM task_timers
		WHERE task_id = $1 AND NOT fired
		ORDER BY fires_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var timers []*TimerRecord
	for rows.Next() {
		timer, err := s.scanTimerRow(rows)
		if err != nil {
			return nil, err
		}
		timers = append(timers, timer)
	}
	return timers, rows.Err()
}

// GetPendingTimers returns timers that are due to fire
func (s *TaskStore) GetPendingTimers(ctx context.Context, beforeTime time.Time, limit int) ([]*TimerRecord, error) {
	query := `
		SELECT timer_id, task_id, timer_type, fires_at, created_at, fired, fired_at, metadata
		FROM task_timers
		WHERE NOT fired AND fires_at <= $1
		ORDER BY fires_at ASC
		LIMIT $2
	`
	rows, err := s.db.QueryContext(ctx, query, beforeTime, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var timers []*TimerRecord
	for rows.Next() {
		timer, err := s.scanTimerRow(rows)
		if err != nil {
			return nil, err
		}
		timers = append(timers, timer)
	}
	return timers, rows.Err()
}

// MarkTimerFired marks a timer as fired
func (s *TaskStore) MarkTimerFired(ctx context.Context, timerID string) error {
	query := `
		UPDATE task_timers
		SET fired = true, fired_at = NOW()
		WHERE timer_id = $1 AND NOT fired
	`
	result, err := s.db.ExecContext(ctx, query, timerID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("timer %s not found or already fired", timerID)
	}
	return nil
}

// DeleteTimer removes a timer
func (s *TaskStore) DeleteTimer(ctx context.Context, timerID string) error {
	query := `DELETE FROM task_timers WHERE timer_id = $1`
	_, err := s.db.ExecContext(ctx, query, timerID)
	return err
}

// DeleteTimersForTask removes all timers for a specific task
func (s *TaskStore) DeleteTimersForTask(ctx context.Context, taskID string) error {
	query := `DELETE FROM task_timers WHERE task_id = $1`
	_, err := s.db.ExecContext(ctx, query, taskID)
	return err
}

// =============================================================================
// Checkpoint Operations
// =============================================================================

// CreateCheckpoint creates a new checkpoint for a task
func (s *TaskStore) CreateCheckpoint(ctx context.Context, checkpoint *CheckpointRecord) error {
	if checkpoint.CheckpointID == "" {
		checkpoint.CheckpointID = uuid.New().String()
	}
	checkpointJSON, err := json.Marshal(checkpoint.CheckpointData)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint_data: %w", err)
	}

	query := `
		INSERT INTO task_checkpoints (checkpoint_id, task_id, sequence_number, checkpoint_data, created_by)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (task_id, sequence_number) DO UPDATE SET
			checkpoint_data = EXCLUDED.checkpoint_data,
			created_at = NOW(),
			created_by = EXCLUDED.created_by
	`
	_, err = s.db.ExecContext(ctx, query,
		checkpoint.CheckpointID,
		checkpoint.TaskID,
		checkpoint.SequenceNumber,
		checkpointJSON,
		checkpoint.CreatedBy,
	)
	return err
}

// GetLatestCheckpoint retrieves the most recent checkpoint for a task
func (s *TaskStore) GetLatestCheckpoint(ctx context.Context, taskID string) (*CheckpointRecord, error) {
	query := `
		SELECT checkpoint_id, task_id, sequence_number, checkpoint_data, created_at, created_by
		FROM task_checkpoints
		WHERE task_id = $1
		ORDER BY sequence_number DESC
		LIMIT 1
	`
	row := s.db.QueryRowContext(ctx, query, taskID)

	var cp CheckpointRecord
	var checkpointJSON []byte

	err := row.Scan(
		&cp.CheckpointID,
		&cp.TaskID,
		&cp.SequenceNumber,
		&checkpointJSON,
		&cp.CreatedAt,
		&cp.CreatedBy,
	)
	if err != nil {
		return nil, err
	}

	if len(checkpointJSON) > 0 {
		if err := json.Unmarshal(checkpointJSON, &cp.CheckpointData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal checkpoint_data: %w", err)
		}
	}

	return &cp, nil
}

// =============================================================================
// Assignment History Operations
// =============================================================================

// RecordAssignment records a task assignment in history
func (s *TaskStore) RecordAssignment(ctx context.Context, assignment *AssignmentRecord) error {
	if assignment.AssignmentID == "" {
		assignment.AssignmentID = uuid.New().String()
	}

	query := `
		INSERT INTO task_assignments (assignment_id, task_id, worker_identity, assigned_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := s.db.ExecContext(ctx, query,
		assignment.AssignmentID,
		assignment.TaskID,
		assignment.WorkerIdentity,
		assignment.AssignedAt,
	)
	return err
}

// GetAssignmentHistory returns all assignments for a task
func (s *TaskStore) GetAssignmentHistory(ctx context.Context, taskID string) ([]*AssignmentRecord, error) {
	query := `
		SELECT assignment_id, task_id, worker_identity, assigned_at, started_at, completed_at, failed, failure_reason
		FROM task_assignments
		WHERE task_id = $1
		ORDER BY assigned_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assignments []*AssignmentRecord
	for rows.Next() {
		var a AssignmentRecord
		var startedAt, completedAt sql.NullTime
		var failureReason sql.NullString

		err := rows.Scan(
			&a.AssignmentID,
			&a.TaskID,
			&a.WorkerIdentity,
			&a.AssignedAt,
			&startedAt,
			&completedAt,
			&a.Failed,
			&failureReason,
		)
		if err != nil {
			return nil, err
		}

		if startedAt.Valid {
			a.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			a.CompletedAt = &completedAt.Time
		}
		if failureReason.Valid {
			a.FailureReason = failureReason.String
		}
		assignments = append(assignments, &a)
	}

	return assignments, nil
}

// =============================================================================
// Audit Event Operations
// =============================================================================

// RecordAuditEvent logs a task event for audit trail
func (s *TaskStore) RecordAuditEvent(ctx context.Context, event *TaskAuditEvent) error {
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}
	eventJSON, err := json.Marshal(event.EventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event_data: %w", err)
	}

	query := `
		INSERT INTO task_audit_events (event_id, task_id, event_type, event_data, created_by)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err = s.db.ExecContext(ctx, query,
		event.EventID,
		event.TaskID,
		event.EventType,
		eventJSON,
		event.CreatedBy,
	)
	return err
}

// RecordAuditEventTx logs a task event for audit trail within an existing transaction
func (s *TaskStore) RecordAuditEventTx(ctx context.Context, tx *sql.Tx, event *TaskAuditEvent) error {
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}
	eventJSON, err := json.Marshal(event.EventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event_data: %w", err)
	}

	query := `
		INSERT INTO task_audit_events (event_id, task_id, event_type, event_data, created_by)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err = tx.ExecContext(ctx, query,
		event.EventID,
		event.TaskID,
		event.EventType,
		eventJSON,
		event.CreatedBy,
	)
	return err
}

// GetTaskAuditEvents returns all audit events for a task
func (s *TaskStore) GetTaskAuditEvents(ctx context.Context, taskID string) ([]*TaskAuditEvent, error) {
	query := `
		SELECT event_id, task_id, event_type, event_data, created_at, created_by
		FROM task_audit_events
		WHERE task_id = $1
		ORDER BY created_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*TaskAuditEvent
	for rows.Next() {
		var e TaskAuditEvent
		var eventJSON []byte

		err := rows.Scan(
			&e.EventID,
			&e.TaskID,
			&e.EventType,
			&eventJSON,
			&e.CreatedAt,
			&e.CreatedBy,
		)
		if err != nil {
			return nil, err
		}

		if len(eventJSON) > 0 {
			if err := json.Unmarshal(eventJSON, &e.EventData); err != nil {
				return nil, fmt.Errorf("failed to unmarshal event_data: %w", err)
			}
		}
		events = append(events, &e)
	}

	return events, nil
}

// =============================================================================
// Disconnect grace window
// =============================================================================

// MarkTaskDisconnected stamps disconnected_at on a running task. Idempotent —
// only takes effect on running tasks where disconnected_at is currently NULL,
// so concurrent gateway cleanups don't clobber each other.
func (s *TaskStore) MarkTaskDisconnected(ctx context.Context, taskID string, when time.Time) error {
	query := `
		UPDATE tasks
		SET disconnected_at = $1
		WHERE task_id = $2
		  AND status = 'running'
		  AND disconnected_at IS NULL
	`
	_, err := s.db.ExecContext(ctx, query, when, taskID)
	return err
}

// ClearTaskDisconnected removes the disconnect marker. Called when a worker
// reconnects and re-establishes its task association. Idempotent.
func (s *TaskStore) ClearTaskDisconnected(ctx context.Context, taskID string) error {
	query := `UPDATE tasks SET disconnected_at = NULL WHERE task_id = $1`
	_, err := s.db.ExecContext(ctx, query, taskID)
	return err
}

// ListDisconnectedTasks returns running tasks whose worker is currently
// disconnected. Ordered oldest-first so the reaper handles long-stuck tasks
// before recently-disconnected ones.
func (s *TaskStore) ListDisconnectedTasks(ctx context.Context, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 500
	}
	query := `
		SELECT ` + taskSelectColumns + `
		FROM tasks
		WHERE status = 'running'
		  AND disconnected_at IS NOT NULL
		ORDER BY disconnected_at
		LIMIT $1
	`
	return s.queryTasks(ctx, query, limit)
}

// =============================================================================
// Helper functions
// =============================================================================

func (s *TaskStore) queryTasks(ctx context.Context, query string, args ...interface{}) ([]*Task, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		task, err := s.scanTaskRow(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, rows.Err()
}

// taskScanner is satisfied by both *sql.Row and *sql.Rows.
type taskScanner interface {
	Scan(dest ...interface{}) error
}

// scanTaskInto scans a task row from any taskScanner into a Task struct.
func scanTaskInto(s taskScanner) (*Task, error) {
	var task Task
	var implementation, specifier, targetAgentID, targetImpl, targetSpec, parentAgentID, parentTaskID sql.NullString
	var assignedTo, errorMsg, errorType, targetTopic, sourceTopic, messageType sql.NullString
	var authorityMode, subjectType, subjectID, rootSubjectType, rootSubjectID sql.NullString
	var authorityGrantID, rootAuthorityGrantID, parentAuthorityGrantID sql.NullString
	var authorityAudienceType, authorityAudienceID, authorityDelegateType, authorityDelegateID sql.NullString
	var scheduledFor, startedAt, completedAt, failedAt, assignedAt, nextRetryAt, lastHeartbeat, disconnectedAt sql.NullTime
	var scheduleToStart, startToClose, heartbeatTimeout, scheduleToClose sql.NullInt64
	var launchParamsJSON, metadataJSON, checkpointJSON, heartbeatJSON []byte

	err := s.Scan(
		&task.TaskID, &task.TaskType, &task.Workspace, &implementation, &specifier,
		&task.Status, &task.Priority, &task.CreatedAt, &task.UpdatedAt, &scheduledFor,
		&startedAt, &completedAt, &failedAt,
		&assignedTo, &assignedAt,
		&task.AssignmentMode, &task.TaskCategory, &targetAgentID, &targetImpl, &targetSpec,
		&launchParamsJSON, &task.QueuedForStartup, &parentAgentID, &parentTaskID,
		&task.RetryCount, &task.MaxRetries, &nextRetryAt,
		&errorMsg, &errorType,
		&task.Payload, &metadataJSON, &checkpointJSON,
		&scheduleToStart, &startToClose, &heartbeatTimeout, &scheduleToClose,
		&lastHeartbeat, &heartbeatJSON,
		&targetTopic, &sourceTopic, &messageType,
		&authorityMode, &subjectType, &subjectID, &rootSubjectType, &rootSubjectID,
		&authorityGrantID, &rootAuthorityGrantID, &parentAuthorityGrantID,
		&authorityAudienceType, &authorityAudienceID, &authorityDelegateType, &authorityDelegateID,
		&task.TaskClass,
		&disconnectedAt, &task.GraceWindowMs,
	)
	if err != nil {
		return nil, err
	}

	// Handle nullable strings
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

	// Handle nullable times
	if scheduledFor.Valid {
		task.ScheduledFor = &scheduledFor.Time
	}
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}
	if failedAt.Valid {
		task.FailedAt = &failedAt.Time
	}
	if assignedAt.Valid {
		task.AssignedAt = &assignedAt.Time
	}
	if nextRetryAt.Valid {
		task.NextRetryAt = &nextRetryAt.Time
	}
	if lastHeartbeat.Valid {
		task.LastHeartbeat = &lastHeartbeat.Time
	}
	if disconnectedAt.Valid {
		task.DisconnectedAt = &disconnectedAt.Time
	}

	// Handle nullable ints
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

	// Unmarshal JSON fields
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

	return &task, nil
}

func (s *TaskStore) scanTask(row *sql.Row) (*Task, error) {
	return scanTaskInto(row)
}

func (s *TaskStore) scanTaskRow(rows *sql.Rows) (*Task, error) {
	return scanTaskInto(rows)
}

func (s *TaskStore) scanTimer(row *sql.Row) (*TimerRecord, error) {
	var timer TimerRecord
	var firedAt sql.NullTime
	var metadataJSON []byte

	err := row.Scan(
		&timer.TimerID,
		&timer.TaskID,
		&timer.TimerType,
		&timer.FiresAt,
		&timer.CreatedAt,
		&timer.Fired,
		&firedAt,
		&metadataJSON,
	)
	if err != nil {
		return nil, err
	}

	if firedAt.Valid {
		timer.FiredAt = &firedAt.Time
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &timer.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &timer, nil
}

func (s *TaskStore) scanTimerRow(rows *sql.Rows) (*TimerRecord, error) {
	var timer TimerRecord
	var firedAt sql.NullTime
	var metadataJSON []byte

	err := rows.Scan(
		&timer.TimerID,
		&timer.TaskID,
		&timer.TimerType,
		&timer.FiresAt,
		&timer.CreatedAt,
		&timer.Fired,
		&firedAt,
		&metadataJSON,
	)
	if err != nil {
		return nil, err
	}

	if firedAt.Valid {
		timer.FiredAt = &firedAt.Time
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &timer.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &timer, nil
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

func nullInt64(i int64) sql.NullInt64 {
	if i == 0 {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: i, Valid: true}
}
