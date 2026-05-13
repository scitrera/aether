package admin

import (
	"context"
	"time"
)

// StateProvider defines the interface for accessing gateway state.
// This interface is designed for future extraction to a separate monitoring API.
// Any component implementing this interface can power the admin UI,
// whether integrated into the gateway or running as a separate service.
type StateProvider interface {
	// Gateway info
	GetGatewayInfo(ctx context.Context) (*GatewayInfo, error)
	GetHealthStatus(ctx context.Context) (*HealthStatus, error)

	// Connections
	GetConnections(ctx context.Context, filter *ConnectionFilter) ([]*ConnectionInfo, error)
	GetConnectionByID(ctx context.Context, sessionID string) (*ConnectionInfo, error)
	DisconnectSession(ctx context.Context, sessionID string) error

	// Tasks
	GetTasks(ctx context.Context, filter *TaskFilter) ([]*TaskInfo, error)
	GetTaskByID(ctx context.Context, taskID string) (*TaskInfo, error)
	RetryTask(ctx context.Context, taskID string) error
	CancelTask(ctx context.Context, taskID string) error

	// Agents & Orchestration
	GetAgentRegistrations(ctx context.Context) ([]*AgentRegistrationInfo, error)
	GetAgentByImplementation(ctx context.Context, implementation string) (*AgentRegistrationInfo, error)
	RegisterAgent(ctx context.Context, agent *AgentRegistrationInfo) error
	UpdateAgent(ctx context.Context, implementation string, agent *AgentRegistrationInfo) error
	DeleteAgent(ctx context.Context, implementation string) error
	GetOrchestratorProfiles(ctx context.Context) ([]*OrchestratorProfileInfo, error)
	LaunchAgent(ctx context.Context, req *LaunchAgentRequest) (*LaunchAgentResponse, error)

	// Workspaces
	GetWorkspaces(ctx context.Context) ([]*WorkspaceInfo, error)
	GetWorkspaceByID(ctx context.Context, workspaceID string) (*WorkspaceInfo, error)
	CreateWorkspace(ctx context.Context, workspace *WorkspaceInfo) error
	UpdateWorkspace(ctx context.Context, workspaceID string, workspace *WorkspaceInfo) error
	DeleteWorkspace(ctx context.Context, workspaceID string) error

	// KV Store
	GetKVKeys(ctx context.Context, scope, prefix string) ([]string, error)
	GetKVValue(ctx context.Context, scope, key string) (*KVEntry, error)
	SetKVValue(ctx context.Context, scope, key, value string, ttl int64) error
	DeleteKVKey(ctx context.Context, scope, key string) error

	// Real-time events (for WebSocket)
	SubscribeEvents(ctx context.Context) (<-chan *Event, error)

	// Messaging - send messages to any topic
	SendMessage(ctx context.Context, req *SendMessageRequest) error

	// Topic Monitoring - subscribe to message stream on a topic
	SubscribeToTopic(ctx context.Context, topic string, handler func(*MonitoredMessage)) (cancelFunc func(), err error)

	// ACL Management
	ListACLRules(ctx context.Context, filter *ACLRuleFilter) ([]*ACLRuleInfo, error)
	GetACLRule(ctx context.Context, principalType, principalID, resourceType, resourceID string) (*ACLRuleInfo, error)
	GrantACLAccess(ctx context.Context, req *GrantACLAccessRequest) (*ACLRuleInfo, error)
	RevokeACLAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error
	ListACLAuthorityGrants(ctx context.Context, filter *ACLAuthorityGrantFilter) ([]*ACLAuthorityGrantInfo, error)
	GetACLAuthorityGrant(ctx context.Context, grantID string) (*ACLAuthorityGrantInfo, error)
	CreateACLAuthorityGrant(ctx context.Context, req *CreateACLAuthorityGrantRequest) (*ACLAuthorityGrantInfo, error)
	RenewACLAuthorityGrant(ctx context.Context, req *RenewACLAuthorityGrantRequest) (*ACLAuthorityGrantInfo, error)
	RevokeACLAuthorityGrant(ctx context.Context, grantID string) error
	SetACLFallbackPolicy(ctx context.Context, req *SetFallbackPolicyRequest) error
	GetACLFallbackPolicy(ctx context.Context, ruleCategory string) (*ACLFallbackPolicyInfo, error)
	QueryACLAuditLog(ctx context.Context, filter *ACLAuditLogFilter) ([]*ACLAuditLogEntryInfo, error)
	CleanupExpiredACLRules(ctx context.Context) (int64, error)
	CleanupOldACLAuditLogs(ctx context.Context, retentionDays int) (int64, error)

	// Message Flow Visualization
	GetMessageFlow(ctx context.Context, workspaceID string) (*MessageFlowInfo, error)

	// API Token Management
	ListTokens(ctx context.Context, limit, offset int, includeRevoked bool) ([]*TokenInfo, error)
	GetToken(ctx context.Context, tokenID string) (*TokenInfo, error)
	CreateToken(ctx context.Context, req *CreateTokenRequest) (*CreateTokenResult, error)
	DeleteToken(ctx context.Context, tokenID string) error
	RevokeToken(ctx context.Context, tokenID string) error

	// Workspace Rate Limits
	SetWorkspaceRateLimit(workspace string, rate float64) error
	GetWorkspaceRateLimit(workspace string) (float64, error)
	RemoveWorkspaceRateLimit(workspace string) error
	ListWorkspaceRateLimits() (map[string]float64, error)
}

// =============================================================================
// Data Types for Admin API
// These types are specifically for the admin API and are decoupled from
// internal gateway types to allow future extraction.
// =============================================================================

// GatewayInfo provides basic gateway information
type GatewayInfo struct {
	GatewayID      string    `json:"gateway_id"`
	Version        string    `json:"version"`
	StartedAt      time.Time `json:"started_at"`
	Uptime         string    `json:"uptime"`
	GoVersion      string    `json:"go_version"`
	NumGoroutines  int       `json:"num_goroutines"`
	MemoryAllocMB  float64   `json:"memory_alloc_mb"`
	NumConnections int       `json:"num_connections"`
}

// HealthStatus provides health check information
type HealthStatus struct {
	Status    string                  `json:"status"` // "healthy", "degraded", "unhealthy"
	Timestamp time.Time               `json:"timestamp"`
	Checks    map[string]*HealthCheck `json:"checks"`
	Stats     *GatewayStats           `json:"stats,omitempty"`
}

// HealthCheck represents a single health check
type HealthCheck struct {
	Status  string `json:"status"` // "ok", "error"
	Latency string `json:"latency,omitempty"`
	Error   string `json:"error,omitempty"`
}

// GatewayStats provides gateway-wide statistics
type GatewayStats struct {
	// Connection counts by type
	AgentConnections        int  `json:"agent_connections"`
	TaskConnections         int  `json:"task_connections"`
	UserConnections         int  `json:"user_connections"`
	OrchestratorConnections int  `json:"orchestrator_connections"`
	WorkflowEngineConnected bool `json:"workflow_engine_connected"`
	MetricsBridgeConnected  bool `json:"metrics_bridge_connected"`
	BridgeConnected         bool `json:"bridge_connected"`

	// Task statistics
	TotalTasks     int `json:"total_tasks"`
	PendingTasks   int `json:"pending_tasks"`
	RunningTasks   int `json:"running_tasks"`
	CompletedTasks int `json:"completed_tasks"`
	FailedTasks    int `json:"failed_tasks"`

	// Message statistics
	MessagesPerSecond float64 `json:"messages_per_second"`
	TotalMessages     int64   `json:"total_messages"`

	// Timer statistics
	ActiveTimers  int `json:"active_timers"`
	PendingTimers int `json:"pending_timers"`
}

// ConnectionFilter for filtering connections list
type ConnectionFilter struct {
	Type      string `json:"type,omitempty"` // "agent", "task", "user", etc.
	Workspace string `json:"workspace,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// ConnectionInfo represents an active connection
type ConnectionInfo struct {
	SessionID      string    `json:"session_id"`
	Type           string    `json:"type"`     // "agent", "task", "user", etc.
	Identity       string    `json:"identity"` // Full identity string
	Workspace      string    `json:"workspace"`
	Implementation string    `json:"implementation,omitempty"`
	Specifier      string    `json:"specifier,omitempty"`
	ConnectedAt    time.Time `json:"connected_at"`
	Duration       string    `json:"duration"`
	RemoteAddr     string    `json:"remote_addr,omitempty"`
	LastActivity   time.Time `json:"last_activity,omitempty"`
}

// TaskFilter for filtering tasks list
type TaskFilter struct {
	Status               string  `json:"status,omitempty"` // "queued", "running", "completed", "failed"
	Workspace            string  `json:"workspace,omitempty"`
	TaskType             string  `json:"task_type,omitempty"`
	TaskClass            int32   `json:"task_class,omitempty"`           // UI hint: 0=no positive filter
	ExcludeTaskClasses   []int32 `json:"exclude_task_classes,omitempty"` // omit tasks whose class is in this list
	SubjectType          string  `json:"subject_type,omitempty"`
	SubjectID            string  `json:"subject_id,omitempty"`
	AuthorityMode        string  `json:"authority_mode,omitempty"`
	AuthorityGrantID     string  `json:"authority_grant_id,omitempty"`
	RootAuthorityGrantID string  `json:"root_authority_grant_id,omitempty"`
	ParentTaskID         string  `json:"parent_task_id,omitempty"`
	Limit                int     `json:"limit,omitempty"`
	Offset               int     `json:"offset,omitempty"`
}

// TaskInfo represents a task
type TaskInfo struct {
	TaskID         string                 `json:"task_id"`
	TaskType       string                 `json:"task_type"`
	TaskClass      int32                  `json:"task_class,omitempty"`
	DisconnectedAt *time.Time             `json:"disconnected_at,omitempty"`
	GraceWindowMs  int64                  `json:"grace_window_ms,omitempty"`
	Status         string                 `json:"status"`
	Workspace      string                 `json:"workspace"`
	TargetTopic    string                 `json:"target_topic,omitempty"`
	AssignedTo     string                 `json:"assigned_to,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	StartedAt      *time.Time             `json:"started_at,omitempty"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty"`
	Attempt        int                    `json:"attempt"`
	MaxAttempts    int                    `json:"max_attempts"`
	Error          string                 `json:"error,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`

	// First-class authority lineage fields (on-behalf-of design, phase 2).
	AuthorityMode          string `json:"authority_mode,omitempty"`
	SubjectType            string `json:"subject_type,omitempty"`
	SubjectID              string `json:"subject_id,omitempty"`
	RootSubjectType        string `json:"root_subject_type,omitempty"`
	RootSubjectID          string `json:"root_subject_id,omitempty"`
	AuthorityGrantID       string `json:"authority_grant_id,omitempty"`
	RootAuthorityGrantID   string `json:"root_authority_grant_id,omitempty"`
	ParentAuthorityGrantID string `json:"parent_authority_grant_id,omitempty"`
	CreatorActorID         string `json:"creator_actor_id,omitempty"`
	ParentTaskID           string `json:"parent_task_id,omitempty"`
}

// AgentRegistrationInfo represents an agent registration
type AgentRegistrationInfo struct {
	Implementation      string                 `json:"implementation"`
	OrchestratorProfile string                 `json:"orchestrator_profile"`
	Description         string                 `json:"description,omitempty"`
	LaunchParams        map[string]interface{} `json:"launch_params"`
	RegisteredAt        time.Time              `json:"registered_at,omitempty"`
	UpdatedAt           time.Time              `json:"updated_at,omitempty"`
}

// OrchestratorProfileInfo represents an orchestrator profile
type OrchestratorProfileInfo struct {
	OrchestratorID string    `json:"orchestrator_id"`
	Profiles       []string  `json:"profiles"`
	ConnectedAt    time.Time `json:"connected_at,omitempty"`
}

// LaunchAgentRequest represents a request to launch an agent via orchestration
type LaunchAgentRequest struct {
	Implementation string `json:"implementation"`
	Specifier      string `json:"specifier"`
	Workspace      string `json:"workspace"`
}

// LaunchAgentResponse represents the response from launching an agent
type LaunchAgentResponse struct {
	TaskID  string `json:"task_id"`
	Message string `json:"message"`
}

// WorkspaceInfo represents a workspace with metadata and statistics
type WorkspaceInfo struct {
	WorkspaceID   string                 `json:"workspace_id"`
	DisplayName   string                 `json:"display_name,omitempty"`
	Description   string                 `json:"description,omitempty"`
	TenantID      string                 `json:"tenant_id,omitempty"`
	CreatedAt     time.Time              `json:"created_at,omitempty"`
	UpdatedAt     time.Time              `json:"updated_at,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	ActiveAgents  int                    `json:"active_agents,omitempty"`
	ActiveTasks   int                    `json:"active_tasks,omitempty"`
	ActiveUsers   int                    `json:"active_users,omitempty"`
	TotalMessages int64                  `json:"total_messages,omitempty"`
}

// KVEntry represents a key-value entry
type KVEntry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Scope     string    `json:"scope"`
	TTL       int64     `json:"ttl,omitempty"` // seconds, 0 = no expiry
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Event represents a real-time event for WebSocket streaming
type Event struct {
	Type      string                 `json:"type"`   // "connection", "task", "message", etc.
	Action    string                 `json:"action"` // "connected", "disconnected", "created", etc.
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// EventTypes for real-time events
const (
	EventTypeConnection = "connection"
	EventTypeTask       = "task"
	EventTypeMessage    = "message"
	EventTypeKV         = "kv"
	EventTypeHealth     = "health"
)

// EventActions
const (
	EventActionConnected    = "connected"
	EventActionDisconnected = "disconnected"
	EventActionCreated      = "created"
	EventActionUpdated      = "updated"
	EventActionCompleted    = "completed"
	EventActionFailed       = "failed"
	EventActionDeleted      = "deleted"
)

// =============================================================================
// Messaging Types
// =============================================================================

// SendMessageRequest represents an admin request to send a message
type SendMessageRequest struct {
	TargetTopic string `json:"target_topic"`
	Payload     string `json:"payload"`
	MessageType string `json:"message_type"` // CHAT, CONTROL, TOOL_CALL, EVENT, METRIC
}

// MonitoredMessage represents a message captured by topic monitoring
type MonitoredMessage struct {
	ID          string    `json:"id"`
	Topic       string    `json:"topic"`
	SourceTopic string    `json:"source_topic,omitempty"`
	Payload     string    `json:"payload"`
	PayloadJSON any       `json:"payload_json,omitempty"`
	MessageType string    `json:"message_type,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

// =============================================================================
// ACL Management Types
// =============================================================================

// ACLRuleFilter for filtering ACL rules
type ACLRuleFilter struct {
	PrincipalType string `json:"principal_type,omitempty"`
	PrincipalID   string `json:"principal_id,omitempty"`
	ResourceType  string `json:"resource_type,omitempty"`
	ResourceID    string `json:"resource_id,omitempty"`
}

// ACLRuleInfo represents an ACL rule
type ACLRuleInfo struct {
	RuleID          string     `json:"rule_id"`
	PrincipalType   string     `json:"principal_type"`
	PrincipalID     string     `json:"principal_id"`
	ResourceType    string     `json:"resource_type"`
	ResourceID      string     `json:"resource_id"`
	AccessLevel     int        `json:"access_level"`
	AccessLevelName string     `json:"access_level_name,omitempty"`
	GrantedBy       string     `json:"granted_by"`
	GrantedAt       time.Time  `json:"granted_at"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	Reason          string     `json:"reason,omitempty"`
}

// GrantACLAccessRequest represents a request to grant ACL access
type GrantACLAccessRequest struct {
	PrincipalType string     `json:"principal_type"`
	PrincipalID   string     `json:"principal_id"`
	ResourceType  string     `json:"resource_type"`
	ResourceID    string     `json:"resource_id"`
	AccessLevel   int        `json:"access_level"`
	GrantedBy     string     `json:"granted_by"`
	Reason        string     `json:"reason,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
}

// PrincipalRef identifies a principal in ACL/admin request and response bodies.
type PrincipalRef struct {
	PrincipalType string `json:"principal_type"`
	PrincipalID   string `json:"principal_id"`
}

// ACLAuthorityGrantFilter filters authority grants.
type ACLAuthorityGrantFilter struct {
	RootGrantID    string `json:"root_grant_id,omitempty"`
	SubjectType    string `json:"subject_type,omitempty"`
	SubjectID      string `json:"subject_id,omitempty"`
	DelegateType   string `json:"delegate_type,omitempty"`
	DelegateID     string `json:"delegate_id,omitempty"`
	AudienceType   string `json:"audience_type,omitempty"`
	AudienceID     string `json:"audience_id,omitempty"`
	IncludeRevoked bool   `json:"include_revoked,omitempty"`
	ActiveOnly     bool   `json:"active_only,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	Offset         int    `json:"offset,omitempty"`
}

// ACLAuthorityGrantResourceScope describes resource-specific scope narrowing.
type ACLAuthorityGrantResourceScope struct {
	ResourceType string   `json:"resource_type"`
	Patterns     []string `json:"patterns,omitempty"`
}

// ACLAuthorityGrantInfo describes a persisted authority grant.
type ACLAuthorityGrantInfo struct {
	GrantID                  string                            `json:"grant_id"`
	RootGrantID              string                            `json:"root_grant_id"`
	Subject                  *PrincipalRef                     `json:"subject"`
	Delegate                 *PrincipalRef                     `json:"delegate"`
	IssuedBy                 *PrincipalRef                     `json:"issued_by"`
	RootSubject              *PrincipalRef                     `json:"root_subject"`
	ParentGrantID            *string                           `json:"parent_grant_id,omitempty"`
	MayDelegate              bool                              `json:"may_delegate"`
	RemainingHops            int                               `json:"remaining_hops"`
	WorkspaceScope           []string                          `json:"workspace_scope,omitempty"`
	ResourceScope            []*ACLAuthorityGrantResourceScope `json:"resource_scope,omitempty"`
	OperationScope           []string                          `json:"operation_scope,omitempty"`
	MaxAccessLevel           int                               `json:"max_access_level"`
	AccessLevelName          string                            `json:"access_level_name,omitempty"`
	AudienceType             string                            `json:"audience_type"`
	AudienceID               string                            `json:"audience_id"`
	ValidWhileAudienceActive bool                              `json:"valid_while_audience_active"`
	ExpiresAt                time.Time                         `json:"expires_at"`
	RenewableUntil           time.Time                         `json:"renewable_until"`
	RenewedAt                *time.Time                        `json:"renewed_at,omitempty"`
	Revoked                  bool                              `json:"revoked"`
	RevokedAt                *time.Time                        `json:"revoked_at,omitempty"`
	Reason                   string                            `json:"reason,omitempty"`
	Metadata                 map[string]interface{}            `json:"metadata,omitempty"`
	CreatedAt                time.Time                         `json:"created_at"`
}

// CreateACLAuthorityGrantRequest represents a request to create an authority grant.
type CreateACLAuthorityGrantRequest struct {
	Subject                  *PrincipalRef                     `json:"subject"`
	Delegate                 *PrincipalRef                     `json:"delegate"`
	IssuedBy                 *PrincipalRef                     `json:"issued_by"`
	RootSubject              *PrincipalRef                     `json:"root_subject,omitempty"`
	ParentGrantID            *string                           `json:"parent_grant_id,omitempty"`
	MayDelegate              bool                              `json:"may_delegate"`
	RemainingHops            int                               `json:"remaining_hops"`
	WorkspaceScope           []string                          `json:"workspace_scope,omitempty"`
	ResourceScope            []*ACLAuthorityGrantResourceScope `json:"resource_scope,omitempty"`
	OperationScope           []string                          `json:"operation_scope,omitempty"`
	MaxAccessLevel           int                               `json:"max_access_level"`
	AudienceType             string                            `json:"audience_type"`
	AudienceID               string                            `json:"audience_id"`
	ValidWhileAudienceActive bool                              `json:"valid_while_audience_active"`
	ExpiresAt                time.Time                         `json:"expires_at"`
	RenewableUntil           time.Time                         `json:"renewable_until"`
	Reason                   string                            `json:"reason,omitempty"`
	Metadata                 map[string]interface{}            `json:"metadata,omitempty"`
}

// RenewACLAuthorityGrantRequest represents a request to extend an authority grant lease.
type RenewACLAuthorityGrantRequest struct {
	GrantID   string    `json:"grant_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// ACLFallbackPolicyInfo represents a fallback policy
type ACLFallbackPolicyInfo struct {
	PolicyID            string    `json:"policy_id"`
	RuleCategory        string    `json:"rule_category"`
	FallbackAccessLevel int       `json:"fallback_access_level"`
	AccessLevelName     string    `json:"access_level_name,omitempty"`
	UpdatedBy           string    `json:"updated_by"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// SetFallbackPolicyRequest represents a request to set a fallback policy
type SetFallbackPolicyRequest struct {
	RuleCategory        string `json:"rule_category"`
	FallbackAccessLevel int    `json:"fallback_access_level"`
	UpdatedBy           string `json:"updated_by"`
}

// ACLAuditLogFilter for filtering audit log entries
type ACLAuditLogFilter struct {
	StartTime     *time.Time `json:"start_time,omitempty"`
	EndTime       *time.Time `json:"end_time,omitempty"`
	PrincipalType string     `json:"principal_type,omitempty"`
	PrincipalID   string     `json:"principal_id,omitempty"`
	ResourceType  string     `json:"resource_type,omitempty"`
	ResourceID    string     `json:"resource_id,omitempty"`
	Decision      string     `json:"decision,omitempty"` // "ALLOW" or "DENY"
	Workspace     string     `json:"workspace,omitempty"`
	Limit         int        `json:"limit,omitempty"`
}

// ACLAuditLogEntryInfo represents an ACL audit log entry
type ACLAuditLogEntryInfo struct {
	AuditID         int64                  `json:"audit_id"`
	Timestamp       time.Time              `json:"timestamp"`
	Decision        string                 `json:"decision"` // "ALLOW" or "DENY"
	AccessLevel     int                    `json:"access_level"`
	AccessLevelName string                 `json:"access_level_name,omitempty"`
	PrincipalType   string                 `json:"principal_type"`
	PrincipalID     string                 `json:"principal_id"`
	ResourceType    string                 `json:"resource_type"`
	ResourceID      string                 `json:"resource_id"`
	Operation       string                 `json:"operation"`
	Workspace       string                 `json:"workspace,omitempty"`
	RuleID          *string                `json:"rule_id,omitempty"`
	FallbackApplied bool                   `json:"fallback_applied"`
	GatewayID       string                 `json:"gateway_id"`
	SessionID       string                 `json:"session_id,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// =============================================================================
// Message Flow Visualization Types
// =============================================================================

// MessageFlowInfo represents a message flow graph for a workspace
type MessageFlowInfo struct {
	WorkspaceID string      `json:"workspace_id"`
	Nodes       []*FlowNode `json:"nodes"`
	Edges       []*FlowEdge `json:"edges"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// FlowNode represents a node in the message flow graph (agent, task, user, etc.)
type FlowNode struct {
	ID             string `json:"id"`              // Unique node identifier
	Label          string `json:"label"`           // Display name
	Type           string `json:"type"`            // "agent", "task", "user", "workflow_engine", etc.
	Status         string `json:"status"`          // "online", "offline"
	Implementation string `json:"implementation"`  // For agents/tasks
	Specifier      string `json:"specifier"`       // For agents/unique tasks
	Topic          string `json:"topic,omitempty"` // The topic this node subscribes to
}

// FlowEdge represents a message flow edge between nodes
type FlowEdge struct {
	From  string `json:"from"`  // Source node ID
	To    string `json:"to"`    // Target node ID
	Label string `json:"label"` // Edge label (e.g., message count)
	Count int64  `json:"count"` // Number of messages
}

// TokenInfo represents an API token (excludes token_hash for security).
type TokenInfo struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	PrincipalType     string     `json:"principal_type"`
	WorkspacePatterns []string   `json:"workspace_patterns"`
	Scopes            []string   `json:"scopes"`
	CreatedBy         string     `json:"created_by"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	LastUsedAt        *time.Time `json:"last_used_at,omitempty"`
	Revoked           bool       `json:"revoked"`
	RevokedAt         *time.Time `json:"revoked_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// CreateTokenRequest contains parameters for creating a new API token.
type CreateTokenRequest struct {
	Name              string
	PrincipalType     string
	WorkspacePatterns []string
	Scopes            []string
	ExpiresInHours    int
	CreatedBy         string
}

// CreateTokenResult is returned when creating a new token.
type CreateTokenResult struct {
	PlaintextToken string     // Only available at creation time
	Token          *TokenInfo // The created token metadata
}
