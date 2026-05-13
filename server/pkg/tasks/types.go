package tasks

import "time"

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
	Limit                int
	Offset               int
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
