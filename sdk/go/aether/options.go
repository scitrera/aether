// Package aether configuration types for the Go SDK.
//
// This file provides configuration types for client options, TLS configuration,
// connection settings, and operation parameters.

package aether

import (
	"crypto/tls"
	"time"
)

// =============================================================================
// Connection Options
// =============================================================================

// ConnectionOptions configures connection behavior including retry and backoff.
//
// All fields have sensible defaults. Use functional options or direct struct
// initialization to override specific settings.
type ConnectionOptions struct {
	// MaxRetries is the maximum number of connection attempts.
	// 0 means infinite retries for reconnection scenarios.
	// Default: 5 for initial connection, 0 for reconnection.
	MaxRetries int

	// InitialBackoff is the initial backoff delay before retrying.
	// Default: 1 second.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff delay between retries.
	// Default: 30 seconds.
	MaxBackoff time.Duration

	// BackoffMultiplier is the multiplier for exponential backoff.
	// Default: 2.0.
	BackoffMultiplier float64

	// AutoReconnect enables automatic reconnection on connection loss.
	// Default: true.
	AutoReconnect bool

	// RetryOnDuplicate treats DuplicateIdentityError as recoverable, allowing
	// the client to retry with backoff instead of exiting immediately.
	// This is useful when restarting containers where the old connection's
	// Redis lock (30s TTL) hasn't expired yet.
	// Default: false.
	RetryOnDuplicate bool

	// ConnectTimeout is the timeout for establishing a connection.
	// Default: 30 seconds.
	ConnectTimeout time.Duration

	// KeepAliveInterval is the interval for keepalive pings.
	// Default: 30 seconds.
	KeepAliveInterval time.Duration
}

// DefaultConnectionOptions returns ConnectionOptions with sensible defaults.
func DefaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		MaxRetries:        5,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
		AutoReconnect:     true,
		ConnectTimeout:    30 * time.Second,
		KeepAliveInterval: 30 * time.Second,
	}
}

// =============================================================================
// TLS Configuration
// =============================================================================

// TLSConfig configures TLS/mTLS for secure connections.
//
// For simple TLS (server authentication only), set RootCAs.
// For mTLS (mutual authentication), also set ClientCert and ClientKey.
type TLSConfig struct {
	// Enabled indicates whether TLS is enabled.
	// Default: false (insecure connection).
	Enabled bool

	// RootCAs is PEM-encoded root CA certificates for server verification.
	// If nil, the system's root CA pool is used.
	RootCAs []byte

	// ClientCert is PEM-encoded client certificate for mTLS.
	// Required for mutual TLS authentication.
	ClientCert []byte

	// ClientKey is PEM-encoded client private key for mTLS.
	// Required for mutual TLS authentication.
	ClientKey []byte

	// ServerName overrides the server name for certificate verification.
	// Useful for testing or when the server name differs from the address.
	ServerName string

	// InsecureSkipVerify skips server certificate verification.
	// WARNING: This should only be used for testing, never in production.
	InsecureSkipVerify bool
}

// ToTLSConfig converts TLSConfig to a standard library *tls.Config.
// Returns nil if TLS is not enabled.
func (c *TLSConfig) ToTLSConfig() (*tls.Config, error) {
	if c == nil || !c.Enabled {
		return nil, nil
	}

	config := &tls.Config{
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	// Load client certificate for mTLS if provided
	if len(c.ClientCert) > 0 && len(c.ClientKey) > 0 {
		cert, err := tls.X509KeyPair(c.ClientCert, c.ClientKey)
		if err != nil {
			return nil, NewConnectionError("failed to load client certificate: " + err.Error())
		}
		config.Certificates = []tls.Certificate{cert}
	}

	// Note: RootCAs would need to be parsed from PEM and added here.
	// The actual implementation will be done in the client.go when
	// we have access to the x509 package imports.

	return config, nil
}

// =============================================================================
// Client Options - Base
// =============================================================================

// ClientOptions contains common options for all client types.
type ClientOptions struct {
	// ServerAddr is the gRPC server address (host:port).
	// Required.
	ServerAddr string

	// Connection configures retry and backoff behavior.
	Connection ConnectionOptions

	// TLS configures TLS/mTLS for secure connections.
	TLS *TLSConfig

	// Credentials for authentication.
	// Keys and values are passed to the server as metadata.
	Credentials map[string]string

	// Metadata contains additional metadata to send with the connection.
	Metadata map[string]string
}

// Validate checks that required fields are set.
func (o *ClientOptions) Validate() error {
	if o.ServerAddr == "" {
		return NewInvalidArgumentError("server address is required", "ServerAddr")
	}
	return nil
}

// =============================================================================
// Agent Options
// =============================================================================

// AgentOptions configures an Agent client.
//
// Agents are persistent entities with workspace/implementation/specifier identity.
// Each agent identity can only have one active connection at a time
// (Connection = Lock paradigm).
type AgentOptions struct {
	ClientOptions

	// Workspace is the workspace to connect to.
	// Required.
	Workspace string

	// Implementation is the agent implementation type.
	// Required.
	Implementation string

	// Specifier is the unique specifier for this agent instance.
	// Required.
	Specifier string
}

// Validate checks that required fields are set.
func (o *AgentOptions) Validate() error {
	if err := o.ClientOptions.Validate(); err != nil {
		return err
	}
	if o.Workspace == "" {
		return NewInvalidArgumentError("workspace is required", "Workspace")
	}
	if o.Implementation == "" {
		return NewInvalidArgumentError("implementation is required", "Implementation")
	}
	if o.Specifier == "" {
		return NewInvalidArgumentError("specifier is required", "Specifier")
	}
	return nil
}

// =============================================================================
// Task Options
// =============================================================================

// TaskOptions configures a Task client.
//
// Tasks can be unique (with a specifier) or non-unique (server-assigned ID).
// Non-unique tasks receive broadcast messages via the tb.* topic pattern.
type TaskOptions struct {
	ClientOptions

	// Workspace is the workspace to connect to.
	// Required.
	Workspace string

	// Implementation is the task implementation type.
	// Required.
	Implementation string

	// Specifier is the unique specifier for this task.
	// If empty, the task is non-unique and receives a server-assigned ID.
	Specifier string
}

// IsUnique returns true if this is a unique task (has a specifier).
func (o *TaskOptions) IsUnique() bool {
	return o.Specifier != ""
}

// Validate checks that required fields are set.
func (o *TaskOptions) Validate() error {
	if err := o.ClientOptions.Validate(); err != nil {
		return err
	}
	if o.Workspace == "" {
		return NewInvalidArgumentError("workspace is required", "Workspace")
	}
	if o.Implementation == "" {
		return NewInvalidArgumentError("implementation is required", "Implementation")
	}
	return nil
}

// =============================================================================
// User Options
// =============================================================================

// UserOptions configures a User client.
//
// Users are identified by user_id and window_id, allowing multiple browser tabs.
type UserOptions struct {
	ClientOptions

	// UserID is the user's unique identifier.
	// Required.
	UserID string

	// WindowID is the window/session identifier.
	// Required.
	WindowID string

	// Workspace is the optional initial workspace.
	// Can be switched later with SwitchWorkspace.
	Workspace string
}

// Validate checks that required fields are set.
func (o *UserOptions) Validate() error {
	if err := o.ClientOptions.Validate(); err != nil {
		return err
	}
	if o.UserID == "" {
		return NewInvalidArgumentError("user ID is required", "UserID")
	}
	if o.WindowID == "" {
		return NewInvalidArgumentError("window ID is required", "WindowID")
	}
	return nil
}

// =============================================================================
// Orchestrator Options
// =============================================================================

// OrchestratorOptions configures an Orchestrator client.
//
// Orchestrators handle task assignment and compute provisioning.
type OrchestratorOptions struct {
	ClientOptions

	// Implementation is the orchestrator implementation type.
	// Required.
	Implementation string

	// SupportedProfiles is the list of profiles this orchestrator can handle.
	// Required (at least one profile).
	SupportedProfiles []string

	// Specifier is an optional unique specifier.
	// If empty, the server generates a unique ID.
	Specifier string
}

// Validate checks that required fields are set.
func (o *OrchestratorOptions) Validate() error {
	if err := o.ClientOptions.Validate(); err != nil {
		return err
	}
	if o.Implementation == "" {
		return NewInvalidArgumentError("implementation is required", "Implementation")
	}
	if len(o.SupportedProfiles) == 0 {
		return NewInvalidArgumentError("at least one supported profile is required", "SupportedProfiles")
	}
	return nil
}

// =============================================================================
// Workflow Engine Options
// =============================================================================

// WorkflowEngineOptions configures a WorkflowEngine client.
//
// WorkflowEngines process broadcast events and trigger downstream actions.
type WorkflowEngineOptions struct {
	ClientOptions

	// Specifier is the unique specifier for this workflow engine.
	// If empty, the server generates a unique ID.
	Specifier string
}

// Validate checks that required fields are set.
func (o *WorkflowEngineOptions) Validate() error {
	return o.ClientOptions.Validate()
}

// =============================================================================
// Bridge Options
// =============================================================================

// BridgeOptions configures a Bridge client.
//
// Bridges are cross-workspace message relays identified by implementation and specifier.
// Each bridge identity can only have one active connection at a time
// (Connection = Lock paradigm). Unlike agents and tasks, bridges have no workspace field.
type BridgeOptions struct {
	ClientOptions

	// Implementation is the bridge implementation type.
	// Required.
	Implementation string

	// Specifier is the unique specifier for this bridge instance.
	// Required.
	Specifier string
}

// Validate checks that required fields are set.
func (o *BridgeOptions) Validate() error {
	if err := o.ClientOptions.Validate(); err != nil {
		return err
	}
	if o.Implementation == "" {
		return NewInvalidArgumentError("implementation is required", "Implementation")
	}
	if o.Specifier == "" {
		return NewInvalidArgumentError("specifier is required", "Specifier")
	}
	return nil
}

// =============================================================================
// Metrics Bridge Options
// =============================================================================

// MetricsBridgeOptions configures a MetricsBridge client.
//
// MetricsBridges receive telemetry data from the metric.* topics.
type MetricsBridgeOptions struct {
	ClientOptions

	// Specifier is the unique specifier for this metrics bridge.
	// If empty, the server generates a unique ID.
	Specifier string
}

// Validate checks that required fields are set.
func (o *MetricsBridgeOptions) Validate() error {
	return o.ClientOptions.Validate()
}

// =============================================================================
// KV Scope
// =============================================================================

// KVScope represents the scope of a KV operation.
type KVScope string

const (
	// KVScopeGlobal is the global scope, accessible to all entities.
	KVScopeGlobal KVScope = "global"

	// KVScopeWorkspace is the workspace scope, accessible within a workspace.
	KVScopeWorkspace KVScope = "workspace"

	// KVScopeUser is the user scope, accessible to a specific user.
	KVScopeUser KVScope = "user"

	// KVScopeUserWorkspace is scoped to a user within a workspace.
	KVScopeUserWorkspace KVScope = "user-workspace"

	// KVScopeGlobalExclusive is per-agent, tenant-wide (exclusive to the agent impl+spec).
	KVScopeGlobalExclusive KVScope = "global-exclusive"

	// KVScopeWorkspaceExclusive is per-agent, per-workspace (exclusive to the agent impl+spec).
	KVScopeWorkspaceExclusive KVScope = "workspace-exclusive"

	// KVScopeUserShared is shared across agents, scoped to a specific user.
	KVScopeUserShared KVScope = "user-shared"

	// KVScopeUserWorkspaceShared is shared across agents, scoped to a user within a workspace.
	KVScopeUserWorkspaceShared KVScope = "user-workspace-shared"
)

// Valid returns true if the scope is a valid KVScope value.
func (s KVScope) Valid() bool {
	switch s {
	case KVScopeGlobal, KVScopeWorkspace, KVScopeUser, KVScopeUserWorkspace,
		KVScopeGlobalExclusive, KVScopeWorkspaceExclusive, KVScopeUserShared, KVScopeUserWorkspaceShared:
		return true
	default:
		return false
	}
}

// =============================================================================
// KV Operation Options
// =============================================================================

// KVGetOptions configures a KV GET operation.
type KVGetOptions struct {
	// Key is the key to retrieve.
	// Required.
	Key string

	// Scope is the KV scope.
	// Default: KVScopeWorkspace.
	Scope KVScope

	// UserID is required for KVScopeUser and KVScopeUserWorkspace.
	UserID string

	// Workspace is required for KVScopeWorkspace and KVScopeUserWorkspace.
	// If empty, uses the client's current workspace.
	Workspace string

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// KVPutOptions configures a KV PUT operation.
type KVPutOptions struct {
	// Key is the key to store.
	// Required.
	Key string

	// Value is the value to store.
	// Required.
	Value []byte

	// Scope is the KV scope.
	// Default: KVScopeWorkspace.
	Scope KVScope

	// UserID is required for KVScopeUser and KVScopeUserWorkspace.
	UserID string

	// Workspace is required for KVScopeWorkspace and KVScopeUserWorkspace.
	// If empty, uses the client's current workspace.
	Workspace string

	// TTL is the time-to-live for the key.
	// 0 means no expiration.
	TTL time.Duration

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// KVListOptions configures a KV LIST operation.
type KVListOptions struct {
	// KeyPrefix is the prefix to filter keys.
	// Empty means all keys in the scope.
	KeyPrefix string

	// Scope is the KV scope.
	// Default: KVScopeWorkspace.
	Scope KVScope

	// UserID is required for KVScopeUser and KVScopeUserWorkspace.
	UserID string

	// Workspace is required for KVScopeWorkspace and KVScopeUserWorkspace.
	// If empty, uses the client's current workspace.
	Workspace string

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// KVDeleteOptions configures a KV DELETE operation.
type KVDeleteOptions struct {
	// Key is the key to delete.
	// Required.
	Key string

	// Scope is the KV scope.
	// Default: KVScopeWorkspace.
	Scope KVScope

	// UserID is required for KVScopeUser and KVScopeUserWorkspace.
	UserID string

	// Workspace is required for KVScopeWorkspace and KVScopeUserWorkspace.
	// If empty, uses the client's current workspace.
	Workspace string

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// =============================================================================
// Message Type
// =============================================================================

// MessageType represents the type of a message.
type MessageType string

const (
	// MessageTypeOpaque is for application-defined payloads forwarded verbatim by Aether.
	// This is the default for generic send helpers — callers own the payload schema.
	MessageTypeOpaque MessageType = "OPAQUE"

	// MessageTypeChat is for conversational text messages.
	MessageTypeChat MessageType = "CHAT"

	// MessageTypeControl is for control/command messages.
	MessageTypeControl MessageType = "CONTROL"

	// MessageTypeToolCall is for tool invocation messages.
	MessageTypeToolCall MessageType = "TOOL_CALL"

	// MessageTypeEvent is for broadcast event messages.
	MessageTypeEvent MessageType = "EVENT"

	// MessageTypeMetric is for telemetry/metrics messages.
	MessageTypeMetric MessageType = "METRIC"
)

// Valid returns true if the message type is a valid MessageType value.
func (t MessageType) Valid() bool {
	switch t {
	case MessageTypeOpaque, MessageTypeChat, MessageTypeControl, MessageTypeToolCall, MessageTypeEvent, MessageTypeMetric:
		return true
	default:
		return false
	}
}

// =============================================================================
// Task Assignment Mode
// =============================================================================

// TaskAssignmentMode represents how a task is assigned.
type TaskAssignmentMode string

const (
	// TaskAssignmentSelfAssign means the caller self-assigns the task.
	TaskAssignmentSelfAssign TaskAssignmentMode = "SELF_ASSIGN"

	// TaskAssignmentTargeted means the task targets a specific agent.
	TaskAssignmentTargeted TaskAssignmentMode = "TARGETED"

	// TaskAssignmentPool means the task goes to a pool for claiming.
	TaskAssignmentPool TaskAssignmentMode = "POOL"
)

// Valid returns true if the assignment mode is a valid TaskAssignmentMode value.
func (m TaskAssignmentMode) Valid() bool {
	switch m {
	case TaskAssignmentSelfAssign, TaskAssignmentTargeted, TaskAssignmentPool:
		return true
	default:
		return false
	}
}

// =============================================================================
// Task Creation Options
// =============================================================================

// CreateTaskOptions configures task creation.
type CreateTaskOptions struct {
	// TaskType is the type of task to create.
	// Required.
	TaskType string

	// Workspace is the workspace for the task.
	// If empty, uses the client's current workspace.
	Workspace string

	// TargetAgentID is the agent to assign the task to.
	// Required for TaskAssignmentTargeted mode.
	TargetAgentID string

	// TargetImplementation is the agent implementation type for pool assignment.
	// Required for TaskAssignmentPool mode. When set and AssignmentMode is
	// not explicitly specified, the mode is automatically set to POOL.
	TargetImplementation string

	// LaunchParamOverrides are optional parameter overrides for orchestration.
	LaunchParamOverrides map[string]string

	// Metadata is optional task metadata.
	Metadata map[string]string

	// Payload is optional binary data for the task (e.g., serialized configs, protobuf work items).
	// Subject to server-enforced size limit (default 512KB).
	Payload []byte

	// AssignmentMode determines how the task is assigned.
	// Default: TaskAssignmentSelfAssign.
	AssignmentMode TaskAssignmentMode

	// TargetIdentity is an arbitrary principal address (e.g.
	// "sv::sandbox-sidecar::<id>") that the gateway treats as the assignee
	// when AssignmentMode is TARGETED and the destination is not an Agent.
	// When set AND the gateway issues a per-task token, the token is
	// returned in CreateTaskResponse.TaskToken — the assignee then connects
	// authenticated as TargetIdentity. Required for service-shaped lease
	// flows (sandbox lease, etc.). Use TargetAgentID for the Agent-typed
	// equivalent; the two are mutually exclusive.
	TargetIdentity string
}

// =============================================================================
// Checkpoint Options
// =============================================================================

// CheckpointSaveOptions configures a checkpoint SAVE operation.
type CheckpointSaveOptions struct {
	// Data is the checkpoint data to save.
	// Required.
	Data []byte

	// Key is the checkpoint key.
	// Default: "default".
	Key string

	// TTL is the time-to-live for the checkpoint.
	// -1 means server default, 0 means no expiration.
	TTL time.Duration

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// CheckpointLoadOptions configures a checkpoint LOAD operation.
type CheckpointLoadOptions struct {
	// Key is the checkpoint key.
	// Default: "default".
	Key string

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// CheckpointDeleteOptions configures a checkpoint DELETE operation.
type CheckpointDeleteOptions struct {
	// Key is the checkpoint key to delete.
	// Default: "default".
	Key string

	// Timeout for synchronous operations.
	// If 0, uses a default timeout.
	Timeout time.Duration
}

// =============================================================================
// Send Message Options
// =============================================================================

// SendMessageOptions configures a message send operation.
type SendMessageOptions struct {
	// TargetTopic is the destination topic.
	// Required.
	TargetTopic string

	// Payload is the message payload.
	// Required.
	Payload []byte

	// MessageType is the type of message.
	// Default: MessageTypeOpaque — pass explicit message type for CHAT/CONTROL/etc.
	MessageType MessageType

	// TraceID is an optional trace ID for distributed tracing.
	TraceID string

	// Metadata is optional message metadata.
	Metadata map[string]string
}

// =============================================================================
// Functional Options Pattern
// =============================================================================

// ConnectionOption is a functional option for ConnectionOptions.
type ConnectionOption func(*ConnectionOptions)

// WithMaxRetries sets the maximum number of connection retries.
func WithMaxRetries(n int) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.MaxRetries = n
	}
}

// WithInitialBackoff sets the initial backoff delay.
func WithInitialBackoff(d time.Duration) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.InitialBackoff = d
	}
}

// WithMaxBackoff sets the maximum backoff delay.
func WithMaxBackoff(d time.Duration) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.MaxBackoff = d
	}
}

// WithBackoffMultiplier sets the backoff multiplier.
func WithBackoffMultiplier(m float64) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.BackoffMultiplier = m
	}
}

// WithAutoReconnect enables or disables automatic reconnection.
func WithAutoReconnect(enabled bool) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.AutoReconnect = enabled
	}
}

// WithRetryOnDuplicate treats DuplicateIdentityError as recoverable when enabled,
// allowing the client to retry with exponential backoff. This is useful when
// restarting services where the previous connection's lock hasn't expired yet.
func WithRetryOnDuplicate(enabled bool) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.RetryOnDuplicate = enabled
	}
}

// WithConnectTimeout sets the connection timeout.
func WithConnectTimeout(d time.Duration) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.ConnectTimeout = d
	}
}

// WithKeepAliveInterval sets the keepalive interval.
func WithKeepAliveInterval(d time.Duration) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.KeepAliveInterval = d
	}
}

// ApplyConnectionOptions applies functional options to ConnectionOptions.
func ApplyConnectionOptions(opts *ConnectionOptions, options ...ConnectionOption) {
	for _, opt := range options {
		opt(opts)
	}
}

// =============================================================================
// Credentials
// =============================================================================

// Credentials is a map of authentication credentials.
// Keys and values are passed to the server as gRPC metadata.
type Credentials map[string]string

// NewCredentials creates a new Credentials map.
func NewCredentials() Credentials {
	return make(Credentials)
}

// WithAPIKey adds a long-lived API key credential for authentication. API keys are validated against the server's token store.
func (c Credentials) WithAPIKey(key string) Credentials {
	c["x-api-key"] = key
	return c
}

// WithToken adds an OAuth/JWT bearer token credential. The token is sent with a "Bearer " prefix in the authorization header.
func (c Credentials) WithToken(token string) Credentials {
	c["authorization"] = "Bearer " + token
	return c
}

// WithTaskToken adds a task authentication token credential.
// Task tokens are short-lived tokens generated by the orchestration system
// for agents connecting to execute a specific task.
func (c Credentials) WithTaskToken(token string) Credentials {
	c["token"] = token
	return c
}

// WithTenant adds a tenant ID credential.
func (c Credentials) WithTenant(tenantID string) Credentials {
	c["x-tenant-id"] = tenantID
	return c
}
