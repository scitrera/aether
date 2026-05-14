package tasks

// This file re-exports the shared task types and constants from the legacy
// pkg/tasks package under the new internal/storage/tasks interface namespace.
// The legacy package remains the source of truth during Stage 1 of the
// storage-interfaces refactor; Stage 2 will introduce a native sqlite sibling
// and may eventually let us collapse the legacy package into this one. For
// now, downstream callers can
//
//	import "github.com/scitrera/aether/internal/storage/tasks"
//
// and find every type, constant, and helper they need to construct, mutate,
// and query tasks — no double-import of the legacy package required.

import (
	legacy "github.com/scitrera/aether/pkg/tasks"
)

// Core types — aliased so a single import gets callers everything they need.
type (
	// Task is the unified task record (supports both messaging delivery and
	// orchestration patterns). See legacy.Task for full field docs.
	Task = legacy.Task

	// TaskStatus is the lifecycle state of a task (pending/assigned/...).
	TaskStatus = legacy.TaskStatus

	// TaskCategory categorizes the purpose of a task
	// (regular/orchestrated/system).
	TaskCategory = legacy.TaskCategory

	// AssignmentMode determines how a task is assigned to workers
	// (self_assign/targeted/pool/broadcast).
	AssignmentMode = legacy.AssignmentMode

	// TimerType discriminates persistent timers attached to a task
	// (schedule_to_start / start_to_close / heartbeat / schedule_to_close /
	// retry).
	TimerType = legacy.TimerType

	// TimerRecord is a persistent timer row for task lifecycle management.
	TimerRecord = legacy.TimerRecord

	// CheckpointRecord is a task checkpoint row for resumability.
	CheckpointRecord = legacy.CheckpointRecord

	// DLQRecord is a dead-letter-queue row.
	DLQRecord = legacy.DLQRecord

	// AssignmentRecord is a task assignment history row.
	AssignmentRecord = legacy.AssignmentRecord

	// TaskAuditEvent is a logged event row for the per-task audit trail.
	TaskAuditEvent = legacy.TaskAuditEvent

	// TaskFilter defines filtering options for ListTasks.
	TaskFilter = legacy.TaskFilter

	// TaskCounts holds task counts grouped by status.
	TaskCounts = legacy.TaskCounts

	// PurgeResult holds the per-status counts returned by PurgeOldTasks.
	PurgeResult = legacy.PurgeResult

	// TaskAuthorityInfo is the on-behalf-of authority lineage bound to a task.
	TaskAuthorityInfo = legacy.TaskAuthorityInfo
)

// Task lifecycle statuses — values stored in tasks.status.
const (
	TaskStatusPending   = legacy.TaskStatusPending
	TaskStatusAssigned  = legacy.TaskStatusAssigned
	TaskStatusStarting  = legacy.TaskStatusStarting
	TaskStatusRunning   = legacy.TaskStatusRunning
	TaskStatusCompleted = legacy.TaskStatusCompleted
	TaskStatusFailed    = legacy.TaskStatusFailed
	TaskStatusCancelled = legacy.TaskStatusCancelled
	TaskStatusDLQ       = legacy.TaskStatusDLQ
)

// Task category values — stored in tasks.task_category.
const (
	TaskCategoryRegular      = legacy.TaskCategoryRegular
	TaskCategoryOrchestrated = legacy.TaskCategoryOrchestrated
	TaskCategorySystem       = legacy.TaskCategorySystem
)

// Assignment mode values — stored in tasks.assignment_mode.
const (
	AssignmentModeSelfAssign = legacy.AssignmentModeSelfAssign
	AssignmentModeTargeted   = legacy.AssignmentModeTargeted
	AssignmentModePool       = legacy.AssignmentModePool
	AssignmentModeBroadcast  = legacy.AssignmentModeBroadcast
)

// Timer type values — stored in task_timers.timer_type.
const (
	TimerTypeScheduleToStart = legacy.TimerTypeScheduleToStart
	TimerTypeStartToClose    = legacy.TimerTypeStartToClose
	TimerTypeHeartbeat       = legacy.TimerTypeHeartbeat
	TimerTypeScheduleToClose = legacy.TimerTypeScheduleToClose
	TimerTypeRetry           = legacy.TimerTypeRetry
)

// Task audit event types — values stored in task_audit_events.event_type.
const (
	EventTypeCreated        = legacy.EventTypeCreated
	EventTypeAssigned       = legacy.EventTypeAssigned
	EventTypeStarting       = legacy.EventTypeStarting
	EventTypeStarted        = legacy.EventTypeStarted
	EventTypeCompleted      = legacy.EventTypeCompleted
	EventTypeFailed         = legacy.EventTypeFailed
	EventTypeRetryScheduled = legacy.EventTypeRetryScheduled
	EventTypeCheckpointed   = legacy.EventTypeCheckpointed
	EventTypeTimedOut       = legacy.EventTypeTimedOut
	EventTypeMovedToDLQ     = legacy.EventTypeMovedToDLQ
	EventTypeCancelled      = legacy.EventTypeCancelled
)
