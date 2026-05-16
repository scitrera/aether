// Package aether message handler types for the Go SDK.
//
// This file provides callback function types and handler interfaces for
// processing incoming messages and events from the Aether gateway.
//
// All handler functions receive a context.Context as their first parameter,
// allowing for cancellation and deadline propagation. Handlers should respect
// context cancellation and return promptly when the context is done.

package aether

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Message Types (High-Level Wrappers)
// =============================================================================

// Message represents an incoming message from the Aether gateway.
// This is a high-level wrapper around the protobuf IncomingMessage.
type Message struct {
	// SourceTopic is the topic from which the message originated.
	// This identifies the sender (e.g., "ag.prod.worker.instance-1").
	SourceTopic string

	// Payload is the raw message payload.
	Payload []byte

	// MessageType is the type of the message (CHAT, CONTROL, TOOL_CALL, EVENT, METRIC).
	MessageType pb.MessageType

	// ReceivedAt is the local time when the message was received.
	ReceivedAt time.Time
}

// ConfigSnapshot represents a configuration snapshot from the gateway.
// This is sent when a client connects and contains KV store data.
// KV values are opaque bytes; use msgpack (or equivalent) for structured data.
type ConfigSnapshot struct {
	// KV contains workspace-scoped key-value pairs (opaque bytes).
	KV map[string][]byte

	// GlobalKV contains global-scoped key-value pairs (opaque bytes).
	GlobalKV map[string][]byte
}

// Signal represents a signal from the gateway.
type Signal struct {
	// Type is the signal type.
	Type SignalType

	// Reason provides additional context for the signal.
	Reason string
}

// SignalType represents the type of a signal.
type SignalType int

const (
	// SignalForceDisconnect indicates the server is forcing a disconnect.
	SignalForceDisconnect SignalType = iota

	// SignalGracefulDisconnect indicates the server is requesting a graceful disconnect.
	// The client should disconnect and reconnect (e.g., server shutdown, connection age limit).
	SignalGracefulDisconnect
)

// String returns the string representation of the signal type.
func (s SignalType) String() string {
	switch s {
	case SignalForceDisconnect:
		return "FORCE_DISCONNECT"
	case SignalGracefulDisconnect:
		return "GRACEFUL_DISCONNECT"
	default:
		return "UNKNOWN"
	}
}

// KVResponse represents a response to a KV operation.
type KVResponse struct {
	// Success indicates whether the operation succeeded.
	Success bool

	// Value is the retrieved value (for GET operations).
	Value []byte

	// Keys is the list of keys (for LIST operations).
	Keys []string

	// KVMap contains key-value pairs (for LIST operations with values).
	// Type is `map[string][]byte` to match the proto and to round-trip
	// arbitrary binary payloads (msgpack, protobuf, raw blobs). Use
	// `string(v)` if you need a Go string view.
	KVMap map[string][]byte

	// RequestId is the correlation ID echoed from the originating request.
	RequestId string

	// CounterValue is the resulting value after INCREMENT/DECREMENT operations.
	CounterValue int64

	// Applied is true iff an INCREMENT_IF/DECREMENT_IF mutation was applied.
	Applied bool
}

// TaskAssignment represents a task assignment from the gateway.
// This is received by Orchestrators when they need to start a new agent/task.
type TaskAssignment struct {
	// TaskID is the unique identifier for the task.
	TaskID string

	// TaskType identifies the type of task.
	TaskType string

	// AssignedTo is the agent identity receiving the task.
	AssignedTo string

	// Metadata contains task-specific metadata.
	Metadata map[string]string

	// AssignedAt is when the task was assigned.
	AssignedAt time.Time

	// Profile is the orchestration profile (e.g., "kubernetes", "docker").
	Profile string

	// LaunchParams contains parameters for launching the agent.
	LaunchParams map[string]string

	// TargetImplementation is the agent implementation to start.
	TargetImplementation string

	// Workspace is the workspace for the new agent.
	Workspace string

	// Specifier is the agent instance identifier.
	Specifier string

	// Payload is optional binary data carried from the task creator.
	Payload []byte
}

// CheckpointResponse represents a response to a checkpoint operation.
type CheckpointResponse struct {
	// Success indicates whether the operation succeeded.
	Success bool

	// Data is the checkpoint data (for LOAD operations).
	Data []byte

	// Keys is the list of checkpoint keys (for LIST operations).
	Keys []string

	// Error is the error message if the operation failed.
	Error string

	// SavedAt is when the checkpoint was saved.
	SavedAt time.Time

	// RequestId is the correlation ID echoed from the originating request.
	RequestId string
}

// ProgressUpdate represents a progress report from an agent or task.
type ProgressUpdate struct {
	// Source is the identity of the reporting agent/task (topic format).
	Source string

	// TaskID is the task or correlation ID this progress relates to.
	TaskID string

	// State is the current state (e.g., "running", "finishing", "idle").
	State string

	// Completion is the completion fraction 0.0-1.0, or -1 for indeterminate.
	Completion float64

	// Summary is a human-readable description of current activity.
	Summary string

	// Step contains structured step information for multi-step operations.
	Step *ProgressStep

	// TimestampMs is the server timestamp when progress was received.
	TimestampMs int64

	// Workspace is the workspace the progress originated from.
	Workspace string

	// RequestID is the correlation ID from the originating request.
	RequestID string

	// Metadata contains arbitrary key-value pairs from the reporter.
	Metadata map[string]string

	// Recipient is the target identity topic (empty = broadcast).
	Recipient string
}

// ProgressStep describes a discrete step within a multi-step operation.
type ProgressStep struct {
	// Name is the step name/title.
	Name string

	// Detail is a description of what the step is doing.
	Detail string

	// Sequence is the step number (1-based).
	Sequence int32

	// TotalSteps is the total number of steps (0 = unknown).
	TotalSteps int32

	// StepType is a UI rendering hint (e.g., "llm_call", "tool_use").
	StepType string
}

// TaskQueryResponse represents a response to a task query.
type TaskQueryResponse struct {
	// Success indicates whether the operation succeeded.
	Success bool

	// Error is the error message if the operation failed.
	Error string

	// Task is the single task result (for GET operations).
	Task *TaskInfo

	// Tasks is the list of task results (for LIST operations).
	Tasks []*TaskInfo

	// TotalCount is the total number of tasks matching the filter.
	TotalCount int32
}

// TaskInfo represents a task's information.
type TaskInfo struct {
	// TaskID is the unique identifier for the task.
	TaskID string

	// TaskType identifies the type of task.
	TaskType string

	// Status is the current status of the task.
	Status string

	// Workspace is the workspace the task belongs to.
	Workspace string

	// TargetTopic is the routing topic for the task.
	TargetTopic string

	// AssignedTo is the agent identity assigned to the task.
	AssignedTo string

	// CreatedAt is the Unix timestamp when the task was created.
	CreatedAt int64

	// StartedAt is the Unix timestamp when the task was started.
	StartedAt int64

	// CompletedAt is the Unix timestamp when the task completed.
	CompletedAt int64

	// Attempt is the current attempt number.
	Attempt int32

	// MaxAttempts is the maximum number of attempts allowed.
	MaxAttempts int32

	// Error is the error message if the task failed.
	Error string

	// Metadata contains task-specific metadata.
	Metadata map[string]string
}

// TaskOperationResponse represents a response to a task operation.
type TaskOperationResponse struct {
	// Success indicates whether the operation succeeded.
	Success bool

	// Message is a human-readable status message.
	Message string

	// Error is the error message if the operation failed.
	Error string

	// Task is the updated task information after the operation.
	Task *TaskInfo
}

// WorkspaceInfo represents workspace metadata.
type WorkspaceInfo struct {
	WorkspaceID   string
	DisplayName   string
	Description   string
	TenantID      string
	CreatedAt     int64
	UpdatedAt     int64
	Metadata      map[string]string
	ActiveAgents  int32
	ActiveTasks   int32
	ActiveUsers   int32
	TotalMessages int64
}

// WorkspaceResponse represents a response to a workspace operation.
type WorkspaceResponse struct {
	Success    bool
	Error      string
	Message    string
	Workspace  *WorkspaceInfo
	Workspaces []*WorkspaceInfo
	TotalCount int32
}

// AgentRegistrationInfo represents an agent registration.
type AgentRegistrationInfo struct {
	Implementation      string
	OrchestratorProfile string
	Description         string
	LaunchParams        map[string]string
	RegisteredAt        int64
	UpdatedAt           int64
}

// OrchestratorInfo represents a connected orchestrator.
type OrchestratorInfo struct {
	OrchestratorID string
	Profiles       []string
	ConnectedAt    int64
}

// AgentLaunchResult represents the result of launching an agent via orchestration.
type AgentLaunchResult struct {
	TaskID  string
	Message string
}

// AgentResponse represents a response to an agent operation.
type AgentResponse struct {
	Success       bool
	Error         string
	Message       string
	Agent         *AgentRegistrationInfo
	Agents        []*AgentRegistrationInfo
	TotalCount    int32
	Orchestrators []*OrchestratorInfo
	LaunchResult  *AgentLaunchResult
}

// ACLRuleInfo represents an access control rule.
type ACLRuleInfo struct {
	RuleID          string
	PrincipalType   string
	PrincipalID     string
	ResourceType    string
	ResourceID      string
	AccessLevel     int32
	AccessLevelName string
	GrantedBy       string
	GrantedAt       int64
	ExpiresAt       int64
	Reason          string
}

// ACLFallbackPolicyInfo represents a fallback policy.
type ACLFallbackPolicyInfo struct {
	PolicyID                string
	RuleCategory            string
	FallbackAccessLevel     int32
	FallbackAccessLevelName string
	UpdatedBy               string
	UpdatedAt               int64
}

// ACLAuditEntryInfo represents an ACL audit log entry.
type ACLAuditEntryInfo struct {
	AuditID         int64
	Timestamp       int64
	Decision        string
	AccessLevel     int32
	AccessLevelName string
	PrincipalType   string
	PrincipalID     string
	ResourceType    string
	ResourceID      string
	Operation       string
	Workspace       string
	RuleID          string
	FallbackApplied bool
	GatewayID       string
	SessionID       string
	Metadata        map[string]string
}

// ACLCleanupResult represents the result of a cleanup operation.
type ACLCleanupResult struct {
	DeletedCount int64
	Message      string
}

// ACLResponse represents a response to an ACL operation.
type ACLResponse struct {
	Success           bool
	Error             string
	Message           string
	Rule              *ACLRuleInfo
	Rules             []*ACLRuleInfo
	TotalRules        int32
	FallbackPolicy    *ACLFallbackPolicyInfo
	AuditEntries      []*ACLAuditEntryInfo
	TotalAuditEntries int32
	CleanupResult     *ACLCleanupResult
}

// TokenInfo represents an API token.
type TokenInfo struct {
	ID                string
	Name              string
	PrincipalType     string
	WorkspacePatterns []string
	Scopes            []string
	CreatedBy         string
	ExpiresAt         int64
	LastUsedAt        int64
	Revoked           bool
	RevokedAt         int64
	CreatedAt         int64
	UpdatedAt         int64
}

// TokenResponse represents a response to a token management operation.
type TokenResponse struct {
	Success        bool
	Error          string
	Message        string
	Token          *TokenInfo   // For GET
	Tokens         []*TokenInfo // For LIST
	TotalCount     int32
	PlaintextToken string     // For CREATE (only available at creation time)
	CreatedToken   *TokenInfo // For CREATE
	RequestId      string
}

// WorkflowResponse represents a response to a workflow operation.
type WorkflowResponse struct {
	// Success indicates whether the operation succeeded.
	Success bool

	// Error is the error message if the operation failed.
	Error string

	// Message is a human-readable status message.
	Message string

	// Data is the response payload (JSON-encoded resource data).
	Data []byte

	// TotalCount is the total number of items (for list operations).
	TotalCount int32

	// RequestId is the correlation ID echoed from the originating request.
	RequestId string
}

// CreateTaskResponse represents the server's response to a CreateTaskRequest
// that carried a non-empty request_id. Gives the creator the server-assigned
// task_id so it can later COMPLETE/FAIL/CANCEL the task.
type CreateTaskResponse struct {
	// Success indicates whether the task was created successfully.
	Success bool

	// TaskID is the server-assigned task identifier on success.
	TaskID string

	// Status is the initial task status (e.g., "pending", "assigned", "pending_pool").
	Status string

	// ErrorCode is populated on failure (e.g. "ERR_PERMISSION_DENIED").
	ErrorCode string

	// ErrorMessage is the human-readable error description.
	ErrorMessage string

	// RequestId is echoed from the originating CreateTaskRequest for correlation.
	RequestId string

	// AssignedTo is the receiving identity string for TARGETED tasks on success.
	// Empty for SELF_ASSIGN or if not yet assigned.
	AssignedTo string

	// TaskToken is the per-task authentication token issued by the gateway
	// when the originating CreateTaskRequest carried a TargetIdentity AND
	// the issue-token check succeeded. The assignee presents this token to
	// connect as TargetIdentity. Empty when the task did not request a
	// token (no TargetIdentity) or the issue-token check denied it.
	TaskToken string
}

// CreateTaskResponseHandler handles create task responses.
type CreateTaskResponseHandler func(ctx context.Context, resp *CreateTaskResponse) error

// TaskQueryResponseHandler handles task query responses.
type TaskQueryResponseHandler func(ctx context.Context, resp *TaskQueryResponse) error

// TaskOperationResponseHandler handles task operation responses.
type TaskOperationResponseHandler func(ctx context.Context, resp *TaskOperationResponse) error

// TokenResponseHandler is called when a token operation response is received.
type TokenResponseHandler func(ctx context.Context, resp *TokenResponse) error

// WorkflowResponseHandler is called when a workflow operation response is received.
type WorkflowResponseHandler func(ctx context.Context, resp *WorkflowResponse) error

// WorkspaceResponseHandler is called when a workspace operation response is received.
type WorkspaceResponseHandler func(ctx context.Context, resp *WorkspaceResponse) error

// AgentResponseHandler is called when an agent operation response is received.
type AgentResponseHandler func(ctx context.Context, resp *AgentResponse) error

// ACLResponseHandler is called when an ACL operation response is received.
type ACLResponseHandler func(ctx context.Context, resp *ACLResponse) error

// AuthorityGrantResponseHandler is called when a runtime authority-grant
// response is received that is not consumed by a synchronous SendOpSync
// caller.
type AuthorityGrantResponseHandler func(ctx context.Context, resp *pb.AuthorityGrantResponse) error

// AuthorityGrantRevocationHandler is called when the gateway pushes an
// AuthorityGrantRevocation event. AuthorityGrantCache instances registered
// via BaseClient.MakeAuthorityCache are invoked first.
type AuthorityGrantRevocationHandler func(ctx context.Context, evt *pb.AuthorityGrantRevocation) error

// WorkflowOperationHandler is called on the workflow engine when it receives
// a forwarded workflow operation from the gateway.
type WorkflowOperationHandler func(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error)

// Service-side proxy/tunnel handlers. When set, dispatchResponse fires the
// matching handler INSTEAD of the default caller-side handlers (which own
// the tunnel-dialer state map). Service principals (e.g. proxy-sidecar
// terminators) register these to receive envelopes the gateway publishes to
// their service topic; caller clients leave them nil.
type (
	ProxyHttpRequestHandler   func(ctx context.Context, req *pb.ProxyHttpRequest) error
	ProxyHttpBodyChunkHandler func(ctx context.Context, chunk *pb.ProxyHttpBodyChunk) error
	TunnelDataInboundHandler  func(ctx context.Context, frame *pb.TunnelData) error
	TunnelAckInboundHandler   func(ctx context.Context, ack *pb.TunnelAck) error
	TunnelCloseInboundHandler func(ctx context.Context, cm *pb.TunnelClose) error
)

// ConnectionAck represents the acknowledgment received after successful connection.
type ConnectionAck struct {
	// SessionID is the server-assigned session identifier.
	// Store this for session resumption on reconnection.
	SessionID string

	// Resumed indicates if this was a resumed session.
	Resumed bool
}

// ErrorInfo represents an error response from the gateway.
type ErrorInfo struct {
	// Code is the error code.
	Code string

	// Message is the human-readable error message.
	Message string

	// Retryable indicates whether the error is retryable.
	Retryable bool

	// RetryAfterMs is the suggested retry delay in milliseconds.
	RetryAfterMs int64
}

// =============================================================================
// Core Message Handlers
// =============================================================================

// MessageHandler is called when an incoming message is received.
//
// The handler receives the message context and the message itself.
// Return an error to indicate processing failed; the error will be logged
// but will not affect the connection.
//
// Example:
//
//	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
//	    fmt.Printf("Received from %s: %s\n", msg.SourceTopic, msg.Payload)
//	    return nil
//	})
type MessageHandler func(ctx context.Context, msg *Message) error

// ConfigHandler is called when a configuration snapshot is received.
//
// This is typically called once when the connection is established,
// providing the initial KV store state for the workspace.
//
// Example:
//
//	client.OnConfig(func(ctx context.Context, config *aether.ConfigSnapshot) error {
//	    for key, value := range config.KV {
//	        fmt.Printf("Config: %s = %s\n", key, value)
//	    }
//	    return nil
//	})
type ConfigHandler func(ctx context.Context, config *ConfigSnapshot) error

// SignalHandler is called when a signal is received from the gateway.
//
// Signals are used for control messages like force disconnect.
//
// Example:
//
//	client.OnSignal(func(ctx context.Context, signal *aether.Signal) error {
//	    if signal.Type == aether.SignalForceDisconnect {
//	        log.Printf("Forced disconnect: %s", signal.Reason)
//	    }
//	    return nil
//	})
type SignalHandler func(ctx context.Context, signal *Signal) error

// ErrorHandler is called when an error response is received from the gateway.
//
// This is for protocol-level errors, not connection errors.
//
// Example:
//
//	client.OnError(func(ctx context.Context, err *aether.ErrorInfo) error {
//	    log.Printf("Server error [%s]: %s", err.Code, err.Message)
//	    return nil
//	})
type ErrorHandler func(ctx context.Context, err *ErrorInfo) error

// =============================================================================
// Operation Response Handlers (Async Mode)
// =============================================================================

// KVResponseHandler is called when a KV operation response is received.
//
// This is used in asynchronous mode when KV operations are fire-and-forget
// with a callback for the response.
//
// Example:
//
//	client.KVAsync().Get("my-key", func(ctx context.Context, resp *aether.KVResponse) error {
//	    if resp.Success {
//	        fmt.Printf("Value: %s\n", resp.Value)
//	    }
//	    return nil
//	})
type KVResponseHandler func(ctx context.Context, resp *KVResponse) error

// CheckpointResponseHandler is called when a checkpoint operation response is received.
//
// This is used in asynchronous mode for checkpoint operations.
//
// Example:
//
//	client.CheckpointAsync().Load("state", func(ctx context.Context, resp *aether.CheckpointResponse) error {
//	    if resp.Success {
//	        // Restore state from resp.Data
//	    }
//	    return nil
//	})
type CheckpointResponseHandler func(ctx context.Context, resp *CheckpointResponse) error

// =============================================================================
// Task and Orchestration Handlers
// =============================================================================

// ProgressHandler is called when a progress update is received from an agent or task.
//
// Progress updates are delivered via the pg::{workspace} stream with
// server-side recipient filtering.
//
// Example:
//
//	client.OnProgress(func(ctx context.Context, update *aether.ProgressUpdate) error {
//	    fmt.Printf("Task %s: %s (%.0f%%)\n", update.TaskID, update.Summary, update.Completion*100)
//	    return nil
//	})
type ProgressHandler func(ctx context.Context, update *ProgressUpdate) error

// TaskAssignmentHandler is called when a task assignment is received.
//
// This is used by Orchestrators to receive task assignments that require
// starting new agent or task instances.
//
// Example:
//
//	orchestrator.OnTaskAssignment(func(ctx context.Context, task *aether.TaskAssignment) error {
//	    log.Printf("Starting %s for task %s", task.TargetImplementation, task.TaskID)
//	    // Launch container/process based on task.LaunchParams
//	    return nil
//	})
type TaskAssignmentHandler func(ctx context.Context, task *TaskAssignment) error

// =============================================================================
// Connection Lifecycle Handlers
// =============================================================================

// ConnectHandler is called when the client successfully connects to the gateway.
//
// This is called both for initial connection and for reconnections.
// The ack parameter contains the session ID which should be stored for
// session resumption.
//
// Example:
//
//	client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
//	    log.Printf("Connected with session %s (resumed: %v)", ack.SessionID, ack.Resumed)
//	    return nil
//	})
type ConnectHandler func(ctx context.Context, ack *ConnectionAck) error

// DisconnectHandler is called when the client disconnects from the gateway.
//
// The reason parameter describes why the disconnection occurred.
// This is called before any automatic reconnection attempt.
//
// Example:
//
//	client.OnDisconnect(func(ctx context.Context, reason string) error {
//	    log.Printf("Disconnected: %s", reason)
//	    return nil
//	})
type DisconnectHandler func(ctx context.Context, reason string) error

// ReconnectingHandler is called when the client is attempting to reconnect.
//
// The attempt parameter indicates which reconnection attempt this is.
// Return an error to abort the reconnection process.
//
// Example:
//
//	client.OnReconnecting(func(ctx context.Context, attempt int) error {
//	    log.Printf("Reconnection attempt %d", attempt)
//	    if attempt > 10 {
//	        return errors.New("too many reconnection attempts")
//	    }
//	    return nil
//	})
type ReconnectingHandler func(ctx context.Context, attempt int) error

// =============================================================================
// Handler Registry
// =============================================================================

// Handlers holds all the callback handlers for a client.
//
// This struct is typically not used directly. Instead, use the On*() methods
// on the client to register handlers.
type Handlers struct {
	// Message handlers
	OnMessage MessageHandler
	OnConfig  ConfigHandler
	OnSignal  SignalHandler
	OnError   ErrorHandler

	// Typed message handlers (dispatched by MessageType in addition to OnMessage)
	OnChatMessage    MessageHandler
	OnControlMessage MessageHandler
	OnToolCall       MessageHandler
	OnEvent          MessageHandler
	OnMetric         MessageHandler

	// Async operation handlers
	OnKVResponse            KVResponseHandler
	OnCheckpointResponse    CheckpointResponseHandler
	OnCreateTaskResponse    CreateTaskResponseHandler
	OnTaskQueryResponse     TaskQueryResponseHandler
	OnTaskOperationResponse TaskOperationResponseHandler
	OnWorkflowResponse      WorkflowResponseHandler
	OnWorkflowOperation     WorkflowOperationHandler // Server-side: workflow engine handles forwarded ops
	OnWorkspaceResponse     WorkspaceResponseHandler
	OnTokenResponse         TokenResponseHandler
	OnAgentResponse         AgentResponseHandler
	OnACLResponse           ACLResponseHandler
	OnAdminResponse         AdminResponseHandler
	OnSessionResponse       SessionResponseHandler

	// Authority grant handlers
	OnAuthorityGrantResponse   AuthorityGrantResponseHandler
	OnAuthorityGrantRevocation AuthorityGrantRevocationHandler

	// Progress handler
	OnProgress ProgressHandler

	// Orchestration handlers
	OnTaskAssignment TaskAssignmentHandler

	// Connection lifecycle handlers
	OnConnect      ConnectHandler
	OnDisconnect   DisconnectHandler
	OnReconnecting ReconnectingHandler

	// Service-side proxy/tunnel handlers. Set by proxy-sidecar terminators
	// (and similar service principals) to receive envelopes routed to their
	// sv:: topic. When nil, dispatchResponse falls back to the default
	// caller-side handlers (handleTunnelData/Ack/Close) which own the
	// tunnel-dialer state.
	OnProxyHttpRequest   ProxyHttpRequestHandler
	OnProxyHttpBodyChunk ProxyHttpBodyChunkHandler
	OnTunnelDataIn       TunnelDataInboundHandler
	OnTunnelAckIn        TunnelAckInboundHandler
	OnTunnelCloseIn      TunnelCloseInboundHandler
}

// NewHandlers creates a new Handlers instance with no-op defaults.
//
// All handlers are initialized to functions that do nothing, ensuring
// that unregistered handlers can be safely called.
func NewHandlers() *Handlers {
	return &Handlers{
		OnMessage:               func(ctx context.Context, msg *Message) error { return nil },
		OnConfig:                func(ctx context.Context, config *ConfigSnapshot) error { return nil },
		OnSignal:                func(ctx context.Context, signal *Signal) error { return nil },
		OnError:                 func(ctx context.Context, err *ErrorInfo) error { return nil },
		OnKVResponse:            func(ctx context.Context, resp *KVResponse) error { return nil },
		OnCheckpointResponse:    func(ctx context.Context, resp *CheckpointResponse) error { return nil },
		OnCreateTaskResponse:    func(ctx context.Context, resp *CreateTaskResponse) error { return nil },
		OnTaskQueryResponse:     func(ctx context.Context, resp *TaskQueryResponse) error { return nil },
		OnTaskOperationResponse: func(ctx context.Context, resp *TaskOperationResponse) error { return nil },
		OnWorkflowResponse:      func(ctx context.Context, resp *WorkflowResponse) error { return nil },
		OnWorkflowOperation:     nil, // Only set by workflow engine clients
		OnWorkspaceResponse:     func(ctx context.Context, resp *WorkspaceResponse) error { return nil },
		OnTokenResponse:         func(ctx context.Context, resp *TokenResponse) error { return nil },
		OnAgentResponse:         func(ctx context.Context, resp *AgentResponse) error { return nil },
		OnACLResponse:           func(ctx context.Context, resp *ACLResponse) error { return nil },
		OnAdminResponse:         func(ctx context.Context, resp *AdminResponse) error { return nil },
		OnSessionResponse:       func(ctx context.Context, resp *SessionOperationResponse) error { return nil },
		OnProgress:              func(ctx context.Context, update *ProgressUpdate) error { return nil },
		OnTaskAssignment:        func(ctx context.Context, task *TaskAssignment) error { return nil },
		OnConnect:               func(ctx context.Context, ack *ConnectionAck) error { return nil },
		OnDisconnect:            func(ctx context.Context, reason string) error { return nil },
		OnReconnecting:          func(ctx context.Context, attempt int) error { return nil },
		// Typed message handlers default to nil (optional, not called if unset)
		OnChatMessage:    nil,
		OnControlMessage: nil,
		OnToolCall:       nil,
		OnEvent:          nil,
		OnMetric:         nil,
	}
}

// =============================================================================
// Handler Interface
// =============================================================================

// Handler is an interface for types that can handle messages.
//
// This interface allows for more flexible message handling patterns,
// such as struct-based handlers with state.
//
// Example:
//
//	type MyHandler struct {
//	    messageCount int
//	}
//
//	func (h *MyHandler) HandleMessage(ctx context.Context, msg *aether.Message) error {
//	    h.messageCount++
//	    return nil
//	}
type Handler interface {
	HandleMessage(ctx context.Context, msg *Message) error
}

// ConfigHandlerInterface is an interface for types that can handle config snapshots.
type ConfigHandlerInterface interface {
	HandleConfig(ctx context.Context, config *ConfigSnapshot) error
}

// SignalHandlerInterface is an interface for types that can handle signals.
type SignalHandlerInterface interface {
	HandleSignal(ctx context.Context, signal *Signal) error
}

// TaskAssignmentHandlerInterface is an interface for types that can handle task assignments.
//
// This is typically implemented by orchestrator implementations.
type TaskAssignmentHandlerInterface interface {
	HandleTaskAssignment(ctx context.Context, task *TaskAssignment) error
}

// ConnectionHandlerInterface is an interface for types that handle connection lifecycle.
type ConnectionHandlerInterface interface {
	HandleConnect(ctx context.Context, ack *ConnectionAck) error
	HandleDisconnect(ctx context.Context, reason string) error
}

// =============================================================================
// Composite Handler Interface
// =============================================================================

// FullHandler combines all handler interfaces.
//
// Implement this interface for a handler that processes all event types.
type FullHandler interface {
	Handler
	ConfigHandlerInterface
	SignalHandlerInterface
	ConnectionHandlerInterface
}
