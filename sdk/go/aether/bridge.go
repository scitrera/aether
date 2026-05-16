// Package aether bridge client implementation.
//
// This file provides the BridgeClient type for connecting to the Aether gateway
// as a cross-workspace message bridge. Bridges relay messages across workspace
// boundaries using the PrincipalBridge principal type.

package aether

import (
	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Bridge Client
// =============================================================================

// BridgeClient is a client for connecting to the Aether gateway as a cross-workspace
// message bridge.
//
// Bridges are identified by implementation and specifier (no workspace field),
// allowing them to operate across workspace boundaries. Each bridge identity can
// only have one active connection at a time (Connection = Lock paradigm).
//
// BridgeClient embeds BaseClient and adds bridge-specific functionality:
//   - Identity management (implementation, specifier)
//   - Messaging helpers (SendToAgent, SendToUser, SendToTask, Broadcast)
//
// Example usage:
//
//	client, err := aether.NewBridgeClient(aether.BridgeOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	    Implementation: "aether-msgbridge",
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
type BridgeClient struct {
	*BaseClient

	// Identity fields
	implementation string
	specifier      string
	credentials    map[string]string
}

// NewBridgeClient creates a new BridgeClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// Returns an error if required options are missing (ServerAddr, Implementation, Specifier).
func NewBridgeClient(opts BridgeOptions) (*BridgeClient, error) {
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

	// Create bridge client
	bc := &BridgeClient{
		BaseClient:     base,
		implementation: opts.Implementation,
		specifier:      opts.Specifier,
		credentials:    opts.Credentials,
	}

	// Set the init message builder
	base.initMsgBuilder = bc.buildInitMessage

	return bc, nil
}

// buildInitMessage creates the InitConnection message for bridge identity.
func (c *BridgeClient) buildInitMessage() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Bridge{
			Bridge: &pb.BridgeIdentity{
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

// Implementation returns the bridge's implementation type.
func (c *BridgeClient) Implementation() string {
	return c.implementation
}

// Specifier returns the bridge's specifier (instance identifier).
func (c *BridgeClient) Specifier() string {
	return c.specifier
}

// Topic returns this bridge's topic address.
//
// Format: br::{implementation}::{specifier}
func (c *BridgeClient) Topic() string {
	return BridgeTopic(c.implementation, c.specifier)
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
func (c *BridgeClient) SendToAgent(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToAgentWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToAgentWithType sends a message to a specific agent with a custom message type.
func (c *BridgeClient) SendToAgentWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
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
func (c *BridgeClient) SendToTask(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToTaskWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToTaskWithType sends a message to a specific task with a custom message type.
func (c *BridgeClient) SendToTaskWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
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
func (c *BridgeClient) SendToUser(userID, windowID string, payload []byte) error {
	return c.SendToUserWithType(userID, windowID, payload, pb.MessageType_OPAQUE)
}

// SendToUserWithType sends a message to a specific user session with a custom message type.
func (c *BridgeClient) SendToUserWithType(userID, windowID string, payload []byte, msgType pb.MessageType) error {
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
//
// Uses CHAT message type by default. Use SendToUserWorkspaceWithType for other types.
func (c *BridgeClient) SendToUserWorkspace(userID, workspace string, payload []byte) error {
	return c.SendToUserWorkspaceWithType(userID, workspace, payload, pb.MessageType_OPAQUE)
}

// SendToUserWorkspaceWithType sends a message to a user's workspace scope with a custom message type.
func (c *BridgeClient) SendToUserWorkspaceWithType(userID, workspace string, payload []byte, msgType pb.MessageType) error {
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
func (c *BridgeClient) BroadcastToAgents(workspace string, payload []byte) error {
	return c.BroadcastToAgentsWithType(workspace, payload, pb.MessageType_OPAQUE)
}

// BroadcastToAgentsWithType sends a message to all agents in a workspace with a custom message type.
func (c *BridgeClient) BroadcastToAgentsWithType(workspace string, payload []byte, msgType pb.MessageType) error {
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
func (c *BridgeClient) BroadcastToUsers(workspace string, payload []byte) error {
	return c.BroadcastToUsersWithType(workspace, payload, pb.MessageType_OPAQUE)
}

// BroadcastToUsersWithType sends a message to all users in a workspace with a custom message type.
func (c *BridgeClient) BroadcastToUsersWithType(workspace string, payload []byte, msgType pb.MessageType) error {
	topic := GlobalUsersTopic(workspace)
	return c.sendMessage(topic, payload, msgType)
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
func (c *BridgeClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *BridgeClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *BridgeClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendToolCallMessage sends a TOOL_CALL message to a specified topic.
func (c *BridgeClient) SendToolCallMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_TOOL_CALL)
}
