// Package tasks defines the storage interface for the unified task subsystem
// (tasks + task_timers + task_checkpoints + task_assignments + task_audit_events
// + dlq tables and the orchestrated_task_queue cross-domain transactional
// surface).
//
// Stage 1 consumers (callers that depend on this interface today):
//   - cmd/gateway/main.go              — constructs the postgres-backed impl
//   - cmd/aetherlite/main.go           — constructs the postgres-backed impl
//     behind the sqlite_compat translation
//     layer (until Stage 2 introduces a
//     native sqlite sibling)
//   - internal/gateway/server.go       — holds the *tasks.TaskStore handle and
//     threads it through to admin handlers,
//     orchestration plumbing, and the task
//     lifecycle reconcilers
//   - internal/orchestration/dispatcher.go,
//     internal/orchestration/memory_dispatcher.go
//     — open a *sql.Tx against the shared db,
//     mutate orchestrated_task_queue, and
//     call RecordAuditEventTx with the same
//     tx so the audit row commits atomically
//
// The interface intentionally mirrors the legacy *pkg/tasks.TaskStore method
// set one-for-one. This is the mechanical-extraction phase of the storage
// refactor described in `.slop/20260513_native-storage-interfaces.md` §2/§4/§5:
// the postgres impl is byte-for-byte the same logic, just re-homed behind an
// interface so a future sqlite-native sibling (Stage 2) can drop in.
//
// Per `.slop/20260514_storage_interfaces_stage0.md`, the StoreTx abstraction
// (§6 of the master plan) is **deferred to Stage 2**. For Stage 1, the
// transactional interface method RecordAuditEventTx keeps *sql.Tx directly so
// the existing orchestration dispatcher call sites continue to compile without
// a coordinated cross-domain rewrite. Stage 2 replaces the *sql.Tx parameter
// with a backend-agnostic StoreTx handle obtained from Store.BeginTx.
package tasks

import (
	"context"
	"database/sql"
	"time"
)

// Store is the task-lifecycle surface consumed by the gateway, aetherlite, and
// the orchestration dispatchers. It covers task CRUD, lifecycle state
// transitions, listing/queries, pool claim/unassign semantics, checkpoint and
// heartbeat persistence, timer scheduling, assignment history, the audit-event
// trail (both standalone and transactional flavors), disconnect-grace
// tracking, the dead-letter queue, and the retention-driven purge sweep.
//
// Nil-tolerance policy (§14.1 of the storage-interfaces plan): callers MUST
// pass a non-nil implementation. Task persistence is load-bearing for
// orchestration correctness — silent nil-deref hazards (the chat-message
// SIGSEGV pattern that inspired this refactor) and silent typed-nil-via-
// failed-assertion hazards (the cleanup-leader-election degradation pattern)
// are both unacceptable here. There is no defensible "opt-out" mode for the
// task store: a deployment that wants in-memory task accounting would do so
// behind a sibling impl that still satisfies the same contract.
type Store interface {
	// =========================================================================
	// Lifecycle (CRUD)
	// =========================================================================

	// CreateTask inserts a new task row. Fills sensible defaults for TaskID
	// (uuid), Status (pending), AssignmentMode (self_assign), TaskCategory
	// (regular), and MaxRetries (3) when the caller leaves them zero. The
	// caller's *Task pointer is mutated in place to reflect those defaults.
	CreateTask(ctx context.Context, task *Task) error

	// GetTask retrieves a single task by ID. Returns sql.ErrNoRows-equivalent
	// behavior through the database driver when the task is absent.
	GetTask(ctx context.Context, taskID string) (*Task, error)

	// UpdateTaskStatus performs an unconditional status overwrite. Prefer the
	// state-transition methods below when the intent is a guarded transition
	// (e.g. pending→assigned); use this only for repair/admin paths.
	UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus) error

	// UpdateTaskMetadata replaces the metadata JSON for an existing task and
	// bumps updated_at. Returns an error if the task does not exist.
	UpdateTaskMetadata(ctx context.Context, taskID string, metadata map[string]interface{}) error

	// UpdateTaskAuthority writes the first-class authority lineage columns
	// (authority_mode/subject_*/grant_id/audience_*/delegate_*) and the
	// metadata JSON in a single write, so grant persistence does not depend
	// on metadata-only storage. Returns an error if the task does not exist.
	UpdateTaskAuthority(ctx context.Context, taskID string, authority TaskAuthorityInfo, metadata map[string]interface{}) error

	// =========================================================================
	// State transitions
	// =========================================================================

	// AssignTask transitions a task from pending → assigned and records a row
	// in task_assignments in the same transaction. Returns an error if the
	// task is not currently pending.
	AssignTask(ctx context.Context, taskID, workerIdentity string) error

	// StartingTask transitions a task from assigned → starting. Used by the
	// orchestrator after it has picked up the work but before the worker
	// process has connected.
	StartingTask(ctx context.Context, taskID string) error

	// StartTask transitions a task into running (idempotent for reconnect
	// scenarios). Only stamps started_at on the first transition from
	// assigned/starting; running → running is a no-op for that timestamp.
	StartTask(ctx context.Context, taskID string) error

	// StartTaskWithAgent is StartTask plus a write to target_agent_id so the
	// task can be reconciled if the orchestrated agent disconnects later.
	StartTaskWithAgent(ctx context.Context, taskID, agentIdentity string) error

	// CompleteTask marks a task as completed and stamps completed_at.
	CompleteTask(ctx context.Context, taskID string) error

	// FailTask marks a task as failed, stamps failed_at, records the error
	// message, and increments retry_count. Does not schedule a retry.
	FailTask(ctx context.Context, taskID, errorMsg string) error

	// FailTaskWithRetry is FailTask plus an error_type column write and a
	// next_retry_at timestamp for the retry sweeper to pick up.
	FailTaskWithRetry(ctx context.Context, taskID, errorType, errorMsg string, nextRetry *time.Time) error

	// CancelTask marks a task as cancelled. Refuses to cancel a task that is
	// already in a terminal state (completed/failed/cancelled).
	CancelTask(ctx context.Context, taskID string) error

	// RetryTask requeues a failed or cancelled task by resetting it back to
	// pending and clearing the assignment/retry/error columns. Refuses to
	// retry a task that is not in failed/cancelled state.
	RetryTask(ctx context.Context, taskID string) error

	// RescheduleTaskAt updates next_retry_at on a failed task without
	// transitioning state. Refuses to reschedule a task that is not failed.
	RescheduleTaskAt(ctx context.Context, taskID string, retryAt time.Time) error

	// =========================================================================
	// Listing / queries
	// =========================================================================

	// ListTasks returns tasks matching the filter, ordered by created_at DESC,
	// with default limit 100 when filter.Limit is zero. Nil filter is treated
	// as an empty filter.
	ListTasks(ctx context.Context, filter *TaskFilter) ([]*Task, error)

	// GetTasksByStatus is a convenience over ListTasks for a single status.
	GetTasksByStatus(ctx context.Context, status TaskStatus, limit int) ([]*Task, error)

	// GetQueuedTasksForAgent returns tasks targeted at a specific agent that
	// are still flagged queued_for_startup.
	GetQueuedTasksForAgent(ctx context.Context, agentID string) ([]*Task, error)

	// GetWorkspaceTasks returns tasks for a workspace, optionally filtered to
	// the orchestrated category only.
	GetWorkspaceTasks(ctx context.Context, workspace string, orchestratedOnly bool) ([]*Task, error)

	// GetAgentTasks returns tasks where the agent is either the assignee
	// (assigned_to) or the target (target_agent_id), capped at 1000 rows
	// ordered created_at DESC.
	GetAgentTasks(ctx context.Context, agentID string) ([]*Task, error)

	// GetTasksNeedingRetry returns failed tasks whose next_retry_at is at or
	// before beforeTime and whose retry_count is still under max_retries.
	GetTasksNeedingRetry(ctx context.Context, beforeTime time.Time, limit int) ([]*Task, error)

	// GetTaskCounts returns aggregate task counts grouped by status.
	GetTaskCounts(ctx context.Context) (*TaskCounts, error)

	// =========================================================================
	// Pool / startup
	// =========================================================================

	// MarkTaskNotQueued clears the queued_for_startup flag for a task.
	MarkTaskNotQueued(ctx context.Context, taskID string) error

	// ClaimPoolTask atomically claims a pending pool task for a worker.
	// Returns (true, nil) on successful claim, (false, nil) if another worker
	// already claimed the task.
	ClaimPoolTask(ctx context.Context, taskID, workerIdentity string) (bool, error)

	// UnassignPoolTask rolls back a pool task assignment, returning it to
	// pending state. Used when delivery to the worker fails after claiming.
	UnassignPoolTask(ctx context.Context, taskID string) error

	// GetPendingPoolTasks returns pending pool tasks for the given
	// implementation/workspace pair that are still queued_for_startup.
	GetPendingPoolTasks(ctx context.Context, implementation, workspace string) ([]*Task, error)

	// HasActiveStartupTask reports whether there is an active agent_startup
	// task for the (implementation, workspace, specifier) tuple. An active
	// task is one in pending/assigned/starting/running. The targetSpecifier
	// dimension lets per-user singleton agents coexist without colliding on
	// (implementation, workspace). Pass "" when the implementation does not
	// use the specifier dimension.
	HasActiveStartupTask(ctx context.Context, targetImplementation, workspace, targetSpecifier string) (bool, string, error)

	// =========================================================================
	// Checkpoints / heartbeat
	// =========================================================================

	// UpdateCheckpoint replaces the inline checkpoint_data JSON on the task
	// row. Distinct from CreateCheckpoint, which writes a separate
	// task_checkpoints history row.
	UpdateCheckpoint(ctx context.Context, taskID string, checkpointData map[string]interface{}) error

	// UpdateHeartbeat stamps last_heartbeat = NOW() and replaces
	// heartbeat_details.
	UpdateHeartbeat(ctx context.Context, taskID string, details map[string]interface{}) error

	// CreateCheckpoint inserts a new task_checkpoints row, or upserts on
	// (task_id, sequence_number) collision. Fills CheckpointID with a uuid
	// when blank.
	CreateCheckpoint(ctx context.Context, checkpoint *CheckpointRecord) error

	// GetLatestCheckpoint returns the highest-sequence_number checkpoint for
	// a task, or sql.ErrNoRows-equivalent when no checkpoints exist.
	GetLatestCheckpoint(ctx context.Context, taskID string) (*CheckpointRecord, error)

	// =========================================================================
	// Timers
	// =========================================================================

	// CreateTimer inserts a new task_timers row. Fills TimerID with a uuid
	// when blank.
	CreateTimer(ctx context.Context, timer *TimerRecord) error

	// GetTimer retrieves a timer by ID.
	GetTimer(ctx context.Context, timerID string) (*TimerRecord, error)

	// GetTimersForTask returns all unfired timers for a task, ordered by
	// fires_at ASC.
	GetTimersForTask(ctx context.Context, taskID string) ([]*TimerRecord, error)

	// GetPendingTimers returns unfired timers whose fires_at is at or before
	// beforeTime, ordered by fires_at ASC and capped at limit rows.
	GetPendingTimers(ctx context.Context, beforeTime time.Time, limit int) ([]*TimerRecord, error)

	// MarkTimerFired stamps fired=true and fired_at=NOW() on a timer.
	// Returns an error if the timer does not exist or has already fired.
	MarkTimerFired(ctx context.Context, timerID string) error

	// DeleteTimer removes a single timer row.
	DeleteTimer(ctx context.Context, timerID string) error

	// DeleteTimersForTask removes every timer associated with a task.
	DeleteTimersForTask(ctx context.Context, taskID string) error

	// =========================================================================
	// Assignment history
	// =========================================================================

	// RecordAssignment inserts a task_assignments row. Fills AssignmentID
	// with a uuid when blank. AssignTask already writes one in its
	// transaction; this is the standalone path for retroactive/manual entries.
	RecordAssignment(ctx context.Context, assignment *AssignmentRecord) error

	// GetAssignmentHistory returns every assignment row for a task, ordered
	// by assigned_at ASC.
	GetAssignmentHistory(ctx context.Context, taskID string) ([]*AssignmentRecord, error)

	// =========================================================================
	// Audit events
	// =========================================================================

	// RecordAuditEvent inserts a task_audit_events row. Fills EventID with a
	// uuid when blank.
	RecordAuditEvent(ctx context.Context, event *TaskAuditEvent) error

	// RecordAuditEventTx inserts a task_audit_events row inside an existing
	// transaction. Used by the orchestration dispatchers when they mutate
	// orchestrated_task_queue and need the audit row to commit atomically
	// with the queue mutation.
	//
	// TODO(stage2): replace the *sql.Tx parameter with a backend-agnostic
	// StoreTx handle obtained from Store.BeginTx, per
	// `.slop/20260513_native-storage-interfaces.md` §6 and
	// `.slop/20260514_storage_interfaces_stage0.md` §6. The current
	// signature is preserved for Stage 1 so the existing dispatcher call
	// sites compile without a coordinated cross-domain rewrite.
	RecordAuditEventTx(ctx context.Context, tx *sql.Tx, event *TaskAuditEvent) error

	// GetTaskAuditEvents returns every audit-event row for a task, ordered
	// by created_at ASC.
	GetTaskAuditEvents(ctx context.Context, taskID string) ([]*TaskAuditEvent, error)

	// =========================================================================
	// Disconnect tracking
	// =========================================================================

	// MarkTaskDisconnected stamps disconnected_at on a running task whose
	// disconnected_at is currently NULL. Idempotent — concurrent gateway
	// cleanups will not clobber each other.
	MarkTaskDisconnected(ctx context.Context, taskID string, when time.Time) error

	// ClearTaskDisconnected removes the disconnect marker. Called when a
	// worker reconnects and re-establishes its task association. Idempotent.
	ClearTaskDisconnected(ctx context.Context, taskID string) error

	// ListDisconnectedTasks returns running tasks whose worker is currently
	// disconnected, ordered oldest-first so the disconnect reaper handles
	// long-stuck tasks before recently-disconnected ones.
	ListDisconnectedTasks(ctx context.Context, limit int) ([]*Task, error)

	// =========================================================================
	// Dead letter queue (DLQ)
	// =========================================================================

	// WriteToDLQ inserts a dead-letter-queue row. Fills DLQMessageID and
	// EnqueuedAt with sensible defaults when the caller leaves them blank.
	WriteToDLQ(ctx context.Context, dlqRecord *DLQRecord) error

	// GetDLQTasks returns DLQ rows with optional workspace/category filters,
	// ordered by enqueued_at DESC, default limit 100.
	GetDLQTasks(ctx context.Context, workspace string, category string, limit int, offset int) ([]*DLQRecord, error)

	// =========================================================================
	// Purge / retention
	// =========================================================================

	// PurgeOldTasks deletes tasks older than the supplied retention windows,
	// scoped per terminal status (completed/failed/cancelled). Returns the
	// per-status delete counts.
	PurgeOldTasks(ctx context.Context, completedRetention, failedRetention, cancelledRetention time.Duration) (*PurgeResult, error)
}
