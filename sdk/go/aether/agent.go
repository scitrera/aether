// Package aether agent client implementation.
//
// This file provides the AgentClient type for connecting to the Aether gateway
// as an agent. Agents are persistent entities with workspace/implementation/specifier
// identity that can send and receive messages, manage state, and participate in
// task orchestration.

package aether

import (
	"fmt"
	"sync"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/protobuf/proto"
)

// =============================================================================
// Agent Client
// =============================================================================

// AgentClient is a client for connecting to the Aether gateway as an agent.
//
// Agents are persistent entities identified by workspace, implementation, and specifier.
// Each agent identity can only have one active connection at a time
// (Connection = Lock paradigm).
//
// AgentClient embeds BaseClient and adds agent-specific functionality:
//   - Identity management (workspace, implementation, specifier)
//   - Messaging helpers (SendToAgent, SendToUser, SendToTask, Broadcast)
//   - Event and metric publishing
//   - Workspace switching
//
// Example usage:
//
//	client, err := aether.NewAgentClient(aether.AgentOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	    Workspace:      "prod",
//	    Implementation: "data-processor",
//	    Specifier:      "instance-1",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
//	    fmt.Printf("Received from %s: %s\n", msg.SourceTopic, msg.Payload)
//	    return nil
//	})
//
//	if err := client.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Start the message loop (blocks until disconnection)
//	if err := client.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
type AgentClient struct {
	*BaseClient

	// Identity fields
	workspace      string
	implementation string
	specifier      string
	credentials    map[string]string

	// Mutex for workspace updates
	workspaceMu sync.RWMutex
}

// NewAgentClient creates a new AgentClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// Returns an error if required options are missing (ServerAddr, Workspace,
// Implementation, Specifier).
func NewAgentClient(opts AgentOptions) (*AgentClient, error) {
	// Validate options
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	// Create base client configuration
	cfg := BaseClientConfig{
		ServerAddr:  opts.ServerAddr,
		Connection:  opts.Connection,
		TLS:         opts.TLS,
		Credentials: opts.Credentials,
	}

	// Create base client
	base, err := NewBaseClient(cfg)
	if err != nil {
		return nil, err
	}

	// Create agent client
	ac := &AgentClient{
		BaseClient:     base,
		workspace:      opts.Workspace,
		implementation: opts.Implementation,
		specifier:      opts.Specifier,
		credentials:    opts.Credentials,
	}

	// Set the init message builder
	base.initMsgBuilder = ac.buildInitMessage

	return ac, nil
}

// buildInitMessage creates the InitConnection message for agent identity.
func (c *AgentClient) buildInitMessage() *pb.InitConnection {
	c.workspaceMu.RLock()
	workspace := c.workspace
	c.workspaceMu.RUnlock()

	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      workspace,
				Implementation: c.implementation,
				Specifier:      c.specifier,
			},
		},
		Credentials: c.credentials,
	}
}

// =============================================================================
// Identity Accessors
// =============================================================================

// Workspace returns the agent's current workspace.
func (c *AgentClient) Workspace() string {
	c.workspaceMu.RLock()
	defer c.workspaceMu.RUnlock()
	return c.workspace
}

// Implementation returns the agent's implementation type.
func (c *AgentClient) Implementation() string {
	return c.implementation
}

// Specifier returns the agent's specifier (instance identifier).
func (c *AgentClient) Specifier() string {
	return c.specifier
}

// Topic returns this agent's topic address.
//
// Format: ag::{workspace}::{implementation}::{specifier}
func (c *AgentClient) Topic() string {
	c.workspaceMu.RLock()
	defer c.workspaceMu.RUnlock()
	return AgentTopic(c.workspace, c.implementation, c.specifier)
}

// =============================================================================
// Workspace Management
// =============================================================================

// SwitchWorkspace changes the agent's workspace.
//
// This sends a SwitchWorkspace message to the gateway, which will:
//   - Update the agent's topic subscription
//   - Optionally send a new ConfigSnapshot for the new workspace
//
// The workspace change takes effect after the gateway processes the request.
func (c *AgentClient) SwitchWorkspace(newWorkspace string) error {
	if newWorkspace == "" {
		return NewInvalidArgumentError("workspace cannot be empty", "newWorkspace")
	}

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_SwitchWorkspace{
			SwitchWorkspace: &pb.SwitchWorkspace{
				NewWorkspaceId: newWorkspace,
			},
		},
	}

	if err := c.Send(msg); err != nil {
		return err
	}

	// Update local workspace state
	c.workspaceMu.Lock()
	c.workspace = newWorkspace
	c.workspaceMu.Unlock()

	return nil
}

// =============================================================================
// Message Sending Helpers
// =============================================================================

// SendToAgent sends a message to a specific agent.
//
// Parameters:
//   - workspace: Target agent's workspace
//   - implementation: Target agent's implementation type
//   - specifier: Target agent's specifier
//   - payload: Message payload (bytes)
//
// Uses CHAT message type by default. Use SendToAgentWithType for other types.
func (c *AgentClient) SendToAgent(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToAgentWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToAgentWithType sends a message to a specific agent with a custom message type.
func (c *AgentClient) SendToAgentWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
	topic := AgentTopic(workspace, implementation, specifier)
	return c.sendMessage(topic, payload, msgType)
}

// SendToTask sends a message to a specific task.
//
// For unique tasks (with specifier):
//   - Uses tu::{workspace}::{implementation}::{specifier} topic
//
// For non-unique tasks (empty specifier):
//   - Uses tb::{workspace}::{implementation} broadcast topic for load balancing
//
// Parameters:
//   - workspace: Target task's workspace
//   - implementation: Target task's implementation type
//   - specifier: Target task's specifier (empty for broadcast to non-unique tasks)
//   - payload: Message payload (bytes)
//
// Uses CHAT message type by default. Use SendToTaskWithType for other types.
func (c *AgentClient) SendToTask(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToTaskWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToTaskWithType sends a message to a specific task with a custom message type.
func (c *AgentClient) SendToTaskWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
	var topic string
	if specifier != "" {
		// Unique task
		topic = UniqueTaskTopic(workspace, implementation, specifier)
	} else {
		// Non-unique task broadcast for load balancing
		topic = TaskBroadcastTopic(workspace, implementation)
	}
	return c.sendMessage(topic, payload, msgType)
}

// SendToUser sends a message to a specific user session.
//
// Parameters:
//   - userID: Target user's ID
//   - windowID: Target user's window/session ID
//   - payload: Message payload (bytes)
//
// Uses CHAT message type by default. Use SendToUserWithType for other types.
func (c *AgentClient) SendToUser(userID, windowID string, payload []byte) error {
	return c.SendToUserWithType(userID, windowID, payload, pb.MessageType_OPAQUE)
}

// SendToUserWithType sends a message to a specific user session with a custom message type.
func (c *AgentClient) SendToUserWithType(userID, windowID string, payload []byte, msgType pb.MessageType) error {
	topic := UserTopic(userID, windowID)
	return c.sendMessage(topic, payload, msgType)
}

// SendToUserWorkspace sends a message to a user's workspace scope.
//
// This reaches the user regardless of which window/tab they're using.
//
// Parameters:
//   - userID: Target user's ID
//   - workspace: Target workspace
//   - payload: Message payload (bytes)
func (c *AgentClient) SendToUserWorkspace(userID, workspace string, payload []byte) error {
	return c.SendToUserWorkspaceWithType(userID, workspace, payload, pb.MessageType_OPAQUE)
}

// SendToUserWorkspaceWithType sends a message to a user's workspace scope with a custom message type.
func (c *AgentClient) SendToUserWorkspaceWithType(userID, workspace string, payload []byte, msgType pb.MessageType) error {
	topic := UserWorkspaceTopic(userID, workspace)
	return c.sendMessage(topic, payload, msgType)
}

// =============================================================================
// Broadcast Helpers
// =============================================================================

// BroadcastToAgents sends a message to all agents in a workspace.
//
// Parameters:
//   - workspace: Target workspace (all agents in this workspace receive the message)
//   - payload: Message payload (bytes)
//
// Uses CHAT message type by default. Use BroadcastToAgentsWithType for other types.
func (c *AgentClient) BroadcastToAgents(workspace string, payload []byte) error {
	return c.BroadcastToAgentsWithType(workspace, payload, pb.MessageType_OPAQUE)
}

// BroadcastToAgentsWithType sends a message to all agents in a workspace with a custom message type.
func (c *AgentClient) BroadcastToAgentsWithType(workspace string, payload []byte, msgType pb.MessageType) error {
	topic := GlobalAgentsTopic(workspace)
	return c.sendMessage(topic, payload, msgType)
}

// BroadcastToUsers sends a message to all users in a workspace.
//
// Parameters:
//   - workspace: Target workspace (all users in this workspace receive the message)
//   - payload: Message payload (bytes)
//
// Uses CHAT message type by default. Use BroadcastToUsersWithType for other types.
func (c *AgentClient) BroadcastToUsers(workspace string, payload []byte) error {
	return c.BroadcastToUsersWithType(workspace, payload, pb.MessageType_OPAQUE)
}

// BroadcastToUsersWithType sends a message to all users in a workspace with a custom message type.
func (c *AgentClient) BroadcastToUsersWithType(workspace string, payload []byte, msgType pb.MessageType) error {
	topic := GlobalUsersTopic(workspace)
	return c.sendMessage(topic, payload, msgType)
}

// =============================================================================
// Event and Metric Publishing
// =============================================================================

// SendEvent publishes an event to the workflow engine.
//
// Events are broadcast to event.* topics and processed by the workflow engine
// for triggering downstream actions.
//
// Parameters:
//   - payload: Event payload (bytes) - typically JSON-encoded event data
func (c *AgentClient) SendEvent(payload []byte) error {
	topic := EventWildcardTopic()
	return c.sendMessage(topic, payload, pb.MessageType_EVENT)
}

// SendMetric publishes a metric to the metrics bridge.
//
// Metrics are broadcast to metric.* topics and collected by the metrics bridge
// for telemetry processing. All entries are additive deltas; negative qty values
// require the `capability/metric_credit` ACL permission on the sender.
//
// Parameters:
//   - metric: Structured metric to publish — use NewMetric() to build one.
func (c *AgentClient) SendMetric(metric *pb.Metric) error {
	if metric == nil {
		return fmt.Errorf("metric must not be nil")
	}
	buf, err := proto.Marshal(metric)
	if err != nil {
		return fmt.Errorf("marshal metric: %w", err)
	}
	return c.sendMessage(MetricWildcardTopic(), buf, pb.MessageType_METRIC)
}

// =============================================================================
// Progress Reporting
// =============================================================================

// ReportProgress sends a progress report to the gateway.
//
// Progress reports are supplemental to the task lifecycle — they describe what
// the agent is currently doing while running. Connection liveness handles death
// detection separately.
//
// Only agents and tasks may send progress reports; the gateway will reject
// progress from other principal types.
func (c *AgentClient) ReportProgress(opts ReportProgressOptions) error {
	return c.BaseClient.ReportProgress(opts)
}

// =============================================================================
// Task Creation
// =============================================================================

// CreateTask creates a new task with the specified parameters.
//
// Parameters:
//   - opts: Task creation options including type, workspace, and assignment mode
//
// For SELF_ASSIGN mode, the calling agent receives the task assignment.
// For TARGETED mode, the task is assigned to the specified target agent.
// For POOL mode, the task is broadcast to all available workers (future feature).
func (c *AgentClient) CreateTask(opts CreateTaskOptions) error {
	if opts.TaskType == "" {
		return NewInvalidArgumentError("task type is required", "TaskType")
	}

	// Use agent's current workspace if not specified
	workspace := opts.Workspace
	if workspace == "" {
		c.workspaceMu.RLock()
		workspace = c.workspace
		c.workspaceMu.RUnlock()
	}

	// Convert assignment mode
	var pbMode pb.TaskAssignmentMode
	switch opts.AssignmentMode {
	case TaskAssignmentSelfAssign, "": // Default to self-assign
		pbMode = pb.TaskAssignmentMode_SELF_ASSIGN
	case TaskAssignmentTargeted:
		pbMode = pb.TaskAssignmentMode_TARGETED
	case TaskAssignmentPool:
		pbMode = pb.TaskAssignmentMode_POOL
	default:
		return NewInvalidArgumentError("invalid assignment mode", "AssignmentMode")
	}

	// Auto-set TARGETED mode if target is specified
	if opts.TargetAgentID != "" && pbMode == pb.TaskAssignmentMode_SELF_ASSIGN {
		pbMode = pb.TaskAssignmentMode_TARGETED
	}

	// Auto-set POOL mode if target implementation is specified
	if opts.TargetImplementation != "" && pbMode == pb.TaskAssignmentMode_SELF_ASSIGN {
		pbMode = pb.TaskAssignmentMode_POOL
	}

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CreateTask{
			CreateTask: &pb.CreateTaskRequest{
				TaskType:             opts.TaskType,
				Workspace:            workspace,
				AssignmentMode:       pbMode,
				TargetAgentId:        opts.TargetAgentID,
				TargetImplementation: opts.TargetImplementation,
				LaunchParamOverrides: opts.LaunchParamOverrides,
				Metadata:             opts.Metadata,
				Payload:              opts.Payload,
			},
		},
	}

	return c.Send(msg)
}

// =============================================================================
// Generic Message Sending
// =============================================================================

// SendMessage sends a message to a specified topic.
//
// This is the low-level sending method. For most use cases, prefer the
// type-specific helpers (SendToAgent, SendToUser, SendToTask, etc.).
//
// Parameters:
//   - targetTopic: The destination topic string
//   - payload: Message payload (bytes)
//   - msgType: Message type (CHAT, CONTROL, TOOL_CALL, EVENT, METRIC)
func (c *AgentClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *AgentClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *AgentClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendToolCallMessage sends a TOOL_CALL message to a specified topic.
func (c *AgentClient) SendToolCallMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_TOOL_CALL)
}
