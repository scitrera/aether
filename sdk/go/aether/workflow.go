// Package aether workflow engine client implementation.
//
// This file provides the WorkflowEngineClient type for connecting to the Aether
// gateway as a workflow engine. The workflow engine is the sole subscriber to
// event.* topics and processes broadcast events to trigger downstream actions
// by sending commands to agents/tasks.

package aether

import (
	"fmt"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// =============================================================================
// Workflow Engine Client
// =============================================================================

// WorkflowEngineClient is a client for connecting to the Aether gateway as a workflow engine.
//
// The workflow engine is a special client type that:
//   - Subscribes to event.* topics (sole subscriber for broadcast events)
//   - Processes events and triggers downstream actions
//   - Sends commands to agents and tasks
//   - Can broadcast to agents and users in workspaces
//   - Can send metrics to the metrics bridge
//
// Workflow engines are NOT part of the gateway - they are external clients that
// receive events and orchestrate actions across the system.
//
// WorkflowEngineClient embeds BaseClient and adds workflow-specific functionality:
//   - Event processing via OnMessage callback
//   - Command sending to agents and tasks
//   - Broadcasting to agents and users
//   - Metric publishing
//
// Example usage:
//
//	client, err := aether.NewWorkflowEngineClient(aether.WorkflowEngineOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Handle incoming events
//	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
//	    fmt.Printf("Received event from %s\n", msg.SourceTopic)
//	    // Process event and trigger downstream actions
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
type WorkflowEngineClient struct {
	*BaseClient

	// Specifier for this workflow engine instance
	specifier   string
	credentials map[string]string
}

// NewWorkflowEngineClient creates a new WorkflowEngineClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// Returns an error if required options are missing (ServerAddr).
func NewWorkflowEngineClient(opts WorkflowEngineOptions) (*WorkflowEngineClient, error) {
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

	return newWorkflowEngineClientFromConfig(cfg, opts.Specifier, opts.Credentials)
}

// NewWorkflowEngineClientWithConn creates a WorkflowEngineClient that uses the
// provided pre-dialed *grpc.ClientConn instead of dialing an address.
//
// This is intended for embedded callers (e.g. AetherLite's workflow engine)
// that already have a *grpc.ClientConn pointing at an in-process bufconn-
// backed gRPC server. Skipping the dial step avoids the TLS-over-loopback
// trap that previously caused the embedded workflow engine to fail handshake
// and enter a reconnect loop.
//
// Lifetime: by default the conn is caller-managed — Close() on the resulting
// client does NOT close conn. Pass ownsConn=true to transfer ownership.
//
// opts.ServerAddr may be empty when using this constructor; opts.TLS is
// ignored (the conn provides its own transport).
func NewWorkflowEngineClientWithConn(conn *grpc.ClientConn, opts WorkflowEngineOptions, ownsConn bool) (*WorkflowEngineClient, error) {
	if conn == nil {
		return nil, NewInvalidArgumentError("conn must not be nil", "conn")
	}
	// Bypass opts.Validate()'s ServerAddr requirement — the conn is the
	// connection. Other Workflow-engine-specific validation in
	// WorkflowEngineOptions.Validate is currently just ClientOptions.Validate,
	// so this is the only check we skip.

	cfg := BaseClientConfig{
		Connection:        opts.Connection,
		Credentials:       opts.Credentials,
		PreDialedConn:     conn,
		OwnsPreDialedConn: ownsConn,
	}
	return newWorkflowEngineClientFromConfig(cfg, opts.Specifier, opts.Credentials)
}

// newWorkflowEngineClientFromConfig is the shared post-dial setup used by
// both NewWorkflowEngineClient and NewWorkflowEngineClientWithConn.
func newWorkflowEngineClientFromConfig(cfg BaseClientConfig, specifier string, creds map[string]string) (*WorkflowEngineClient, error) {
	base, err := NewBaseClient(cfg)
	if err != nil {
		return nil, err
	}

	wc := &WorkflowEngineClient{
		BaseClient:  base,
		specifier:   specifier,
		credentials: creds,
	}

	// Set the init message builder
	base.initMsgBuilder = wc.buildInitMessage

	return wc, nil
}

// buildInitMessage creates the InitConnection message for workflow engine identity.
//
// Note: Workflow engines use a structured identity message (WorkflowEngineIdentity)
// for identification. The specifier field is stored locally but not sent to the server.
func (c *WorkflowEngineClient) buildInitMessage() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_WorkflowEngine{
			WorkflowEngine: &pb.WorkflowEngineIdentity{},
		},
		Credentials: c.credentials,
	}
}

// =============================================================================
// Identity Accessors
// =============================================================================

// Specifier returns the workflow engine's specifier.
//
// If no specifier was provided during creation, this may be empty until
// the server assigns one upon connection.
func (c *WorkflowEngineClient) Specifier() string {
	return c.specifier
}

// =============================================================================
// Command Sending - Agents
// =============================================================================

// SendCommandToAgent sends a control/command message to a specific agent.
//
// This is the primary method for workflow engines to trigger actions on agents
// based on received events.
//
// Parameters:
//   - workspace: Target agent's workspace
//   - implementation: Target agent's implementation type
//   - specifier: Target agent's specifier
//   - payload: Command payload (bytes)
//
// Uses CONTROL message type. Use SendCommandToAgentWithType for other types.
func (c *WorkflowEngineClient) SendCommandToAgent(workspace, implementation, specifier string, payload []byte) error {
	return c.SendCommandToAgentWithType(workspace, implementation, specifier, payload, pb.MessageType_CONTROL)
}

// SendCommandToAgentWithType sends a command to a specific agent with a custom message type.
func (c *WorkflowEngineClient) SendCommandToAgentWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
	topic := AgentTopic(workspace, implementation, specifier)
	return c.sendMessage(topic, payload, msgType)
}

// =============================================================================
// Command Sending - Tasks
// =============================================================================

// SendCommandToTask sends a control/command message to a specific task.
//
// For unique tasks (with specifier):
//   - Uses tu.{workspace}.{implementation}.{specifier} topic
//
// For non-unique tasks (empty specifier):
//   - Uses tb.{workspace}.{implementation} broadcast topic for load balancing
//
// Parameters:
//   - workspace: Target task's workspace
//   - implementation: Target task's implementation type
//   - specifier: Target task's specifier (empty for broadcast to non-unique tasks)
//   - payload: Command payload (bytes)
//
// Uses CONTROL message type. Use SendCommandToTaskWithType for other types.
func (c *WorkflowEngineClient) SendCommandToTask(workspace, implementation, specifier string, payload []byte) error {
	return c.SendCommandToTaskWithType(workspace, implementation, specifier, payload, pb.MessageType_CONTROL)
}

// SendCommandToTaskWithType sends a command to a specific task with a custom message type.
func (c *WorkflowEngineClient) SendCommandToTaskWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
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

// =============================================================================
// Broadcast Helpers
// =============================================================================

// BroadcastToAgents sends a message to all agents in a workspace.
//
// Parameters:
//   - workspace: Target workspace (all agents in this workspace receive the message)
//   - payload: Message payload (bytes)
//
// Uses CONTROL message type. Use BroadcastToAgentsWithType for other types.
func (c *WorkflowEngineClient) BroadcastToAgents(workspace string, payload []byte) error {
	return c.BroadcastToAgentsWithType(workspace, payload, pb.MessageType_CONTROL)
}

// BroadcastToAgentsWithType sends a message to all agents in a workspace with a custom message type.
func (c *WorkflowEngineClient) BroadcastToAgentsWithType(workspace string, payload []byte, msgType pb.MessageType) error {
	topic := GlobalAgentsTopic(workspace)
	return c.sendMessage(topic, payload, msgType)
}

// BroadcastToUsers sends a message to all users in a workspace.
//
// Parameters:
//   - workspace: Target workspace (all users in this workspace receive the message)
//   - payload: Message payload (bytes)
//
// Uses CHAT message type. Use BroadcastToUsersWithType for other types.
func (c *WorkflowEngineClient) BroadcastToUsers(workspace string, payload []byte) error {
	return c.BroadcastToUsersWithType(workspace, payload, pb.MessageType_OPAQUE)
}

// BroadcastToUsersWithType sends a message to all users in a workspace with a custom message type.
func (c *WorkflowEngineClient) BroadcastToUsersWithType(workspace string, payload []byte, msgType pb.MessageType) error {
	topic := GlobalUsersTopic(workspace)
	return c.sendMessage(topic, payload, msgType)
}

// =============================================================================
// User Messaging
// =============================================================================

// SendToUser sends a message to a specific user session.
//
// Parameters:
//   - userID: Target user's ID
//   - windowID: Target user's window/session ID
//   - payload: Message payload (bytes)
//
// Uses CHAT message type. Use SendToUserWithType for other types.
func (c *WorkflowEngineClient) SendToUser(userID, windowID string, payload []byte) error {
	return c.SendToUserWithType(userID, windowID, payload, pb.MessageType_OPAQUE)
}

// SendToUserWithType sends a message to a specific user session with a custom message type.
func (c *WorkflowEngineClient) SendToUserWithType(userID, windowID string, payload []byte, msgType pb.MessageType) error {
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
func (c *WorkflowEngineClient) SendToUserWorkspace(userID, workspace string, payload []byte) error {
	return c.SendToUserWorkspaceWithType(userID, workspace, payload, pb.MessageType_OPAQUE)
}

// SendToUserWorkspaceWithType sends a message to a user's workspace scope with a custom message type.
func (c *WorkflowEngineClient) SendToUserWorkspaceWithType(userID, workspace string, payload []byte, msgType pb.MessageType) error {
	topic := UserWorkspaceTopic(userID, workspace)
	return c.sendMessage(topic, payload, msgType)
}

// =============================================================================
// Metric Publishing
// =============================================================================

// SendMetric publishes a metric to the metrics bridge.
//
// Metrics are broadcast to metric.* topics and collected by the metrics bridge
// for telemetry processing. All entries are additive deltas; negative qty values
// require the `capability/metric_credit` ACL permission on the sender.
//
// Parameters:
//   - metric: Structured metric to publish — use NewMetric() to build one.
func (c *WorkflowEngineClient) SendMetric(metric *pb.Metric) error {
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
// Generic Message Sending
// =============================================================================

// SendMessage sends a message to a specified topic.
//
// This is the low-level sending method. For most use cases, prefer the
// type-specific helpers (SendCommandToAgent, SendCommandToTask, etc.).
//
// Parameters:
//   - targetTopic: The destination topic string
//   - payload: Message payload (bytes)
//   - msgType: Message type (CHAT, CONTROL, TOOL_CALL, EVENT, METRIC)
func (c *WorkflowEngineClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *WorkflowEngineClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *WorkflowEngineClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}

// SendToolCallMessage sends a TOOL_CALL message to a specified topic.
func (c *WorkflowEngineClient) SendToolCallMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_TOOL_CALL)
}

// =============================================================================
// Workflow Operation Handling
// =============================================================================

// OnWorkflowOperation registers a handler for incoming workflow operations forwarded
// by the gateway. The workflow engine is responsible for handling these operations
// and returning a response.
func (c *WorkflowEngineClient) OnWorkflowOperation(handler WorkflowOperationHandler) {
	c.handlers.OnWorkflowOperation = handler
}
