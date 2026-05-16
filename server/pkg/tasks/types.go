package tasks

import (
	"fmt"
	"time"
)

// TaskStatus represents the lifecycle state of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"   // Created, waiting to be assigned
	TaskStatusAssigned  TaskStatus = "assigned"  // Assigned to a worker (orchestrator picked it up)
	TaskStatusStarting  TaskStatus = "starting"  // Orchestrator is launching the job, not yet connected
	TaskStatusRunning   TaskStatus = "running"   // Worker is actively processing (connected)
	TaskStatusCompleted TaskStatus = "completed" // Finished successfully
	TaskStatusFailed    TaskStatus = "failed"    // Failed (may be retried)
	TaskStatusCancelled TaskStatus = "cancelled" // Cancelled by user/system
	TaskStatusDLQ       TaskStatus = "dlq"       // Moved to dead letter queue

	// Phase 1: A2A-aligned paused states
	TaskStatusWaitingInput      TaskStatus = "waiting_input"      // Paused awaiting user/caller input (A2A INPUT_REQUIRED)
	TaskStatusWaitingAuthority  TaskStatus = "waiting_authority"  // Paused awaiting authority grant (A2A AUTH_REQUIRED)
	TaskStatusWaitingDependency TaskStatus = "waiting_dependency" // Paused awaiting upstream task(s)
	TaskStatusHibernated        TaskStatus = "hibernated"         // Voluntarily sleeping until a scheduled wake time
	TaskStatusRejected          TaskStatus = "rejected"           // Permanently rejected (A2A REJECTED); terminal
)

// WaitReason discriminates why a task entered a waiting/hibernated state.
type WaitReason string

const (
	WaitReasonInput       WaitReason = "input"       // Waiting for user/caller input
	WaitReasonAuthority   WaitReason = "authority"   // Waiting for authority grant
	WaitReasonDependency  WaitReason = "dependency"  // Waiting for upstream task(s)
	WaitReasonHibernation WaitReason = "hibernation" // Scheduled hibernation / wake
)

// HibernationDescriptor captures the parameters needed to release a worker on
// hibernation and re-spawn it with full state on wake. Mirrors the proto
// HibernationDescriptor message; serialized inline inside WaitSpec.
type HibernationDescriptor struct {
	CheckpointKey    string   `json:"checkpoint_key,omitempty"`    // Required: checkpoint to LOAD on wake
	ResumeSessionID  string   `json:"resume_session_id,omitempty"` // Optional: session id to resume; empty = fresh session
	WakeEventTypes   []string `json:"wake_event_types,omitempty"`  // Optional: future wake-event triggers
	EscalationPolicy string   `json:"escalation_policy,omitempty"` // Optional: "fail" (default), "retry", "alert"
}

// WaitSpec describes why a task was paused and what conditions will wake it.
// Stored as JSONB (postgres) or TEXT JSON (sqlite) in the wait_spec column.
//
// Adding a new optional nested struct (Hibernation) is round-trip safe: rows
// persisted before this field existed deserialize with Hibernation = nil, and
// the JSONB column tolerates the new key without a migration.
//
// Reserved task metadata keys for hibernation handoff: when the waker triggers
// a hibernation wake (Stage B), it copies the descriptor's CheckpointKey and
// ResumeSessionID into Task.Metadata under the keys defined below BEFORE
// clearing the WaitSpec. The orchestration delivery path then reads these keys
// to populate TaskAssignment.checkpoint_key / resume_session_id on the fresh
// worker's assignment. The "_" prefix denotes server-managed metadata; entries
// are left in place after delivery as audit history.
type WaitSpec struct {
	Reason              WaitReason             `json:"reason,omitempty"`
	ExpectedPrincipal   string                 `json:"expected_principal,omitempty"`     // For INPUT: principal expected to send the input message; empty = any
	InputMatch          map[string]string      `json:"input_match,omitempty"`            // For INPUT: metadata key/value the inbound message must match
	AuthorityRequestID  string                 `json:"authority_request_id,omitempty"`   // For AUTHORITY: correlation id for the authority request being awaited
	DependsOn           []string               `json:"depends_on,omitempty"`             // For DEPENDENCY: task IDs this task depends on
	WakeOnAny           bool                   `json:"wake_on_any,omitempty"`            // True = wake when any dependency completes (default: all)
	TimeoutMs           int64                  `json:"timeout_ms,omitempty"`             // Max wait duration in ms; 0 = no timeout
	ScheduledWakeUnixMs int64                  `json:"scheduled_wake_unix_ms,omitempty"` // Unix-ms absolute wake time (independent of TimeoutMs)
	Hibernation         *HibernationDescriptor `json:"hibernation,omitempty"`            // For HIBERNATION: checkpoint key + wake/escalation params
}

// Reserved task metadata keys for hibernation handoff. The Stage B waker /
// orchestration delivery path uses these keys to propagate hibernation
// rehydration context from the parked WaitSpec onto the fresh worker's
// TaskAssignment.
const (
	MetadataKeyHibernationCheckpointKey   = "_hibernation_checkpoint_key"
	MetadataKeyHibernationResumeSessionID = "_hibernation_resume_session_id"
)

// TaskCategory categorizes the purpose of a task
type TaskCategory string

const (
	TaskCategoryRegular      TaskCategory = "regular"      // Normal user/agent tasks
	TaskCategoryOrchestrated TaskCategory = "orchestrated" // Orchestration system tasks
	TaskCategorySystem       TaskCategory = "system"       // Internal system tasks
)

// AssignmentMode determines how a task is assigned to workers
type AssignmentMode string

const (
	AssignmentModeSelfAssign AssignmentMode = "self_assign" // Creator assigns to self
	AssignmentModeTargeted   AssignmentMode = "targeted"    // Assigned to specific agent
	AssignmentModePool       AssignmentMode = "pool"        // Assigned to any matching agent
	AssignmentModeBroadcast  AssignmentMode = "broadcast"   // Sent to all matching agents
)

// TaskAuthorityInfo captures the on-behalf-of authority lineage currently bound
// to a task. These fields mirror the persisted grant lineage needed for
// renewal, reassignment, audit correlation, and lifecycle cleanup.
type TaskAuthorityInfo struct {
	Mode                   string `json:"mode,omitempty"`
	SubjectType            string `json:"subject_type,omitempty"`
	SubjectID              string `json:"subject_id,omitempty"`
	RootSubjectType        string `json:"root_subject_type,omitempty"`
	RootSubjectID          string `json:"root_subject_id,omitempty"`
	AuthorityGrantID       string `json:"authority_grant_id,omitempty"`
	RootAuthorityGrantID   string `json:"root_authority_grant_id,omitempty"`
	ParentAuthorityGrantID string `json:"parent_authority_grant_id,omitempty"`
	AudienceType           string `json:"audience_type,omitempty"`
	AudienceID             string `json:"audience_id,omitempty"`
	DelegateType           string `json:"delegate_type,omitempty"`
	DelegateID             string `json:"delegate_id,omitempty"`
}

// Timer types for task lifecycle management
type TimerType string

const (
	TimerTypeScheduleToStart TimerType = "schedule_to_start"
	TimerTypeStartToClose    TimerType = "start_to_close"
	TimerTypeHeartbeat       TimerType = "heartbeat"
	TimerTypeScheduleToClose TimerType = "schedule_to_close"
	TimerTypeRetry           TimerType = "retry"
)

// Task represents a unified task record in the database
// This type supports both messaging delivery and orchestration patterns
type Task struct {
	// Primary identification
	TaskID   string `json:"task_id"`
	TaskType string `json:"task_type"`

	// Identity-based addressing (Aether model)
	Workspace      string `json:"workspace"`
	Implementation string `json:"implementation,omitempty"`
	Specifier      string `json:"specifier,omitempty"`

	// Lifecycle status
	Status   TaskStatus `json:"status"`
	Priority int        `json:"priority"`

	// Timestamps
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	ScheduledFor *time.Time `json:"scheduled_for,omitempty"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	FailedAt     *time.Time `json:"failed_at,omitempty"`

	// Assignment tracking
	AssignedTo string     `json:"assigned_to,omitempty"`
	AssignedAt *time.Time `json:"assigned_at,omitempty"`

	// Orchestration fields
	AssignmentMode       AssignmentMode         `json:"assignment_mode"`
	TaskCategory         TaskCategory           `json:"task_category"`
	TargetAgentID        string                 `json:"target_agent_id,omitempty"`
	TargetImplementation string                 `json:"target_implementation,omitempty"`
	TargetSpecifier      string                 `json:"target_specifier,omitempty"`
	LaunchParams         map[string]interface{} `json:"launch_params,omitempty"`
	QueuedForStartup     bool                   `json:"queued_for_startup"`
	ParentAgentID        string                 `json:"parent_agent_id,omitempty"`
	ParentTaskID         string                 `json:"parent_task_id,omitempty"`

	// Retry handling
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`

	// Error tracking
	ErrorMessage string `json:"error_message,omitempty"`
	ErrorType    string `json:"error_type,omitempty"`

	// Payload and metadata
	Payload        []byte                 `json:"payload,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	CheckpointData map[string]interface{} `json:"checkpoint_data,omitempty"`
	Authority      TaskAuthorityInfo      `json:"authority,omitempty"`

	// Timeout configuration (milliseconds)
	ScheduleToStartMs  int64 `json:"schedule_to_start_ms,omitempty"`
	StartToCloseMs     int64 `json:"start_to_close_ms,omitempty"`
	HeartbeatTimeoutMs int64 `json:"heartbeat_timeout_ms,omitempty"`
	ScheduleToCloseMs  int64 `json:"schedule_to_close_ms,omitempty"`

	// Heartbeat tracking
	LastHeartbeat    *time.Time             `json:"last_heartbeat,omitempty"`
	HeartbeatDetails map[string]interface{} `json:"heartbeat_details,omitempty"`

	// Disconnect grace window fields.
	// DisconnectedAt records when the worker's gRPC stream closed (any cause).
	// Cleared when the worker reconnects. The disconnect reaper fails the task
	// when (now - DisconnectedAt) > GraceWindowMs.
	DisconnectedAt *time.Time `json:"disconnected_at,omitempty"`
	// GraceWindowMs is the per-task reconnect grace window. 0 = use class default.
	// Set at task creation; not modified after.
	GraceWindowMs int64 `json:"grace_window_ms,omitempty"`

	// Phase 1: A2A-aligned paused-state fields.
	// WaitSpec describes why the task is paused and what will wake it.
	WaitSpec *WaitSpec `json:"wait_spec,omitempty"`
	// DependsOn lists task IDs that must complete before this task can resume.
	DependsOn []string `json:"depends_on,omitempty"`
	// ContextID is the client-minted A2A contextId that groups related tasks.
	ContextID string `json:"context_id,omitempty"`
	// PausedAt records when the task entered a waiting/hibernated state.
	PausedAt *time.Time `json:"paused_at,omitempty"`

	// Messaging support (for delivery tasks)
	TargetTopic string `json:"target_topic,omitempty"`
	SourceTopic string `json:"source_topic,omitempty"`
	MessageType string `json:"message_type,omitempty"`

	// UI classification hint (mirrors proto TaskClass enum; 0 = UNSPECIFIED)
	TaskClass int32 `json:"task_class,omitempty"`
}

// TimerRecord represents a persistent timer for task lifecycle management
type TimerRecord struct {
	TimerID   string
	TaskID    string
	TimerType TimerType
	FiresAt   time.Time
	CreatedAt time.Time
	Fired     bool
	FiredAt   *time.Time
	Metadata  map[string]interface{}
}

// CheckpointRecord represents a task checkpoint for resumability
type CheckpointRecord struct {
	CheckpointID   string
	TaskID         string
	SequenceNumber int
	CheckpointData map[string]interface{}
	CreatedAt      time.Time
	CreatedBy      string // Worker identity that created the checkpoint
}

// AssignmentRecord represents a task assignment in history
type AssignmentRecord struct {
	AssignmentID   string
	TaskID         string
	WorkerIdentity string
	AssignedAt     time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	Failed         bool
	FailureReason  string
}

// TaskAuditEvent represents a logged event for task audit trail
type TaskAuditEvent struct {
	EventID   string
	TaskID    string
	EventType string // 'created', 'assigned', 'started', 'completed', 'failed', etc.
	EventData map[string]interface{}
	CreatedAt time.Time
	CreatedBy string
}

// Task event types for logging
const (
	EventTypeCreated        = "created"
	EventTypeAssigned       = "assigned"
	EventTypeStarting       = "starting" // Orchestrator began launching the task
	EventTypeStarted        = "started"  // Task connected and is running
	EventTypeCompleted      = "completed"
	EventTypeFailed         = "failed"
	EventTypeRetryScheduled = "retry_scheduled"
	EventTypeCheckpointed   = "checkpointed"
	EventTypeTimedOut       = "timed_out"
	EventTypeMovedToDLQ     = "moved_to_dlq"
	EventTypeCancelled      = "cancelled"
)

// DLQRecord represents a dead letter queue entry
type DLQRecord struct {
	DLQMessageID    string
	OriginalTaskID  string
	Category        string // 'exhausted_retries', 'timeout', 'poison_message'
	Workspace       string
	OriginalPayload []byte
	OriginalMeta    map[string]interface{}
	FailureReason   string
	FailureDetails  map[string]interface{}
	EnqueuedAt      time.Time
	AttemptCount    int
	LastAttemptAt   time.Time
	ReprocessedAt   *time.Time
	Resolved        bool
	ResolvedBy      string
	ResolutionNotes string
}

// TaskCounts holds task counts by status
type TaskCounts struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Assigned  int `json:"assigned"`
	Starting  int `json:"starting"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// TaskFilter defines filtering options for task queries
type TaskFilter struct {
	Status               *TaskStatus
	Statuses             []TaskStatus // Filter by multiple statuses (takes priority over Status)
	Workspace            string
	TaskType             string
	TaskCategory         *TaskCategory
	AssignmentMode       *AssignmentMode
	AssignedTo           string
	TargetAgentID        string
	TargetImplementation string
	QueuedForStartup     *bool
	SubjectType          string
	SubjectID            string
	AuthorityMode        string
	AuthorityGrantID     string
	RootAuthorityGrantID string
	ParentTaskID         string
	TaskClass            int32   // 0 = no positive filter
	ExcludeTaskClasses   []int32 // any task whose TaskClass is in this list is omitted
	// Phase 1: A2A-aligned filter fields.
	ContextID       string       // Filter by client-minted session identifier; empty = no filter
	ExcludeStatuses []TaskStatus // Omit tasks whose status is in this list
	Limit           int
	Offset          int

	// Phase 4: management-surface filter extensions.
	//
	// CreatorActorType is informational only — the storage column backing
	// the creator identity (parent_agent_id in the tasks table) is a single
	// canonical identity string. CreatorActorID is the filtered value.
	CreatorActorType string
	CreatorActorID   string

	// StatusTimestampAfterUnixMs filters to tasks whose most recent status
	// transition (updated_at) is at or after this unix-ms timestamp.
	// 0 = no filter.
	StatusTimestampAfterUnixMs int64

	// PageToken is the opaque cursor returned by the previous ListTasks
	// call. When set, supersedes Offset for stable pagination. The cursor
	// format is base64url("<unix_micros>|<task_id>") and orders by
	// (updated_at DESC, task_id DESC).
	PageToken string

	// IncludeDescendants, combined with ParentTaskID, recursively walks the
	// task tree below the named parent. False (default) preserves the
	// existing direct-children-only behavior.
	IncludeDescendants bool
}

// =============================================================================
// Phase 1: Task lifecycle helpers
// =============================================================================

// terminalStatuses is the set of states from which a task cannot transition.
var terminalStatuses = map[TaskStatus]bool{
	TaskStatusCompleted: true,
	TaskStatusFailed:    true,
	TaskStatusCancelled: true,
	TaskStatusDLQ:       true,
	TaskStatusRejected:  true,
}

// waitingStatuses is the set of paused states.
var waitingStatuses = map[TaskStatus]bool{
	TaskStatusWaitingInput:      true,
	TaskStatusWaitingAuthority:  true,
	TaskStatusWaitingDependency: true,
	TaskStatusHibernated:        true,
}

// IsTerminal reports whether status is a terminal (no further transitions) state.
func IsTerminal(status TaskStatus) bool {
	return terminalStatuses[status]
}

// IsWaiting reports whether status is a paused/waiting state.
func IsWaiting(status TaskStatus) bool {
	return waitingStatuses[status]
}

// validTransitions defines the allowed from→to status transitions.
// Any transition not listed is rejected by ValidateTransition.
var validTransitions = map[TaskStatus]map[TaskStatus]bool{
	TaskStatusPending: {
		TaskStatusAssigned:          true,
		TaskStatusStarting:          true, // direct-to-starting paths (e.g. self-assigned tasks)
		TaskStatusCancelled:         true,
		TaskStatusRejected:          true, // REJECT op: declined before processing
		TaskStatusWaitingDependency: true,
	},
	TaskStatusAssigned: {
		TaskStatusStarting:  true,
		TaskStatusRunning:   true,
		TaskStatusFailed:    true,
		TaskStatusCancelled: true,
		TaskStatusRejected:  true, // REJECT op: declined after assignment but before run
		TaskStatusPending:   true, // unassign / retry
	},
	TaskStatusStarting: {
		TaskStatusRunning:   true,
		TaskStatusFailed:    true,
		TaskStatusCancelled: true,
	},
	TaskStatusRunning: {
		TaskStatusCompleted:         true,
		TaskStatusFailed:            true,
		TaskStatusCancelled:         true,
		TaskStatusWaitingInput:      true,
		TaskStatusWaitingAuthority:  true,
		TaskStatusWaitingDependency: true,
		TaskStatusHibernated:        true,
	},
	TaskStatusWaitingInput: {
		TaskStatusRunning:   true,
		TaskStatusFailed:    true,
		TaskStatusCancelled: true,
		TaskStatusRejected:  true,
	},
	TaskStatusWaitingAuthority: {
		TaskStatusRunning:   true,
		TaskStatusFailed:    true,
		TaskStatusCancelled: true,
		TaskStatusRejected:  true,
	},
	TaskStatusWaitingDependency: {
		TaskStatusRunning:   true,
		TaskStatusPending:   true, // dependency cancelled / failed
		TaskStatusFailed:    true,
		TaskStatusCancelled: true,
	},
	TaskStatusHibernated: {
		TaskStatusRunning:   true,
		TaskStatusPending:   true,
		TaskStatusFailed:    true,
		TaskStatusCancelled: true,
	},
	TaskStatusFailed: {
		TaskStatusPending: true, // retry
	},
	TaskStatusCancelled: {
		TaskStatusPending: true, // retry
	},
	// TaskStatusCompleted, TaskStatusDLQ, TaskStatusRejected: no outgoing transitions
}

// ValidateTransition returns nil if transitioning from → to is permitted,
// or a descriptive error if it is not.
func ValidateTransition(from, to TaskStatus) error {
	if from == to {
		return nil // idempotent; callers may allow this
	}
	allowed, ok := validTransitions[from]
	if !ok || !allowed[to] {
		return fmt.Errorf("invalid task state transition: %s → %s", from, to)
	}
	return nil
}

// =============================================================================
// Backwards compatibility aliases (to be removed after migration)
// =============================================================================

// ExtendedTask is an alias for Task (backwards compatibility)
type ExtendedTask = Task

// TaskRecord is an alias for Task (backwards compatibility)
type TaskRecord = Task

// TaskState is deprecated - use TaskStatus instead
type TaskState = TaskStatus

// Legacy state constants for backwards compatibility
const (
	TaskStateQueued     TaskStatus = "pending"   // Maps to pending
	TaskStateDispatched TaskStatus = "assigned"  // Maps to assigned
	TaskStateStarting   TaskStatus = "starting"  // Same
	TaskStateRunning    TaskStatus = "running"   // Same
	TaskStateCompleted  TaskStatus = "completed" // Same
	TaskStateFailed     TaskStatus = "failed"    // Same
	TaskStateDLQ        TaskStatus = "dlq"       // Same
)
