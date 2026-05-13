// Package aether user client implementation.
//
// This file provides the UserClient type for connecting to the Aether gateway
// as a user. Users are identified by user_id and window_id, allowing multiple
// browser tabs or sessions per user.

package aether

import (
	"sync"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// User Client
// =============================================================================

// UserClient is a client for connecting to the Aether gateway as a user.
//
// Users are identified by user_id and window_id, allowing multiple browser
// tabs or sessions per user. Each user session (user_id + window_id combination)
// is unique and can only have one active connection at a time.
//
// UserClient embeds BaseClient and adds user-specific functionality:
//   - Identity management (user_id, window_id)
//   - Messaging helpers (SendToAgent, SendToUser, SendToTask)
//   - Optional workspace association for workspace-scoped operations
//
// Note: Per the Aether specification, users can only send direct messages.
// They cannot publish events or metrics like agents and tasks can.
//
// Example usage:
//
//	client, err := aether.NewUserClient(aether.UserOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	    UserID:   "alice",
//	    WindowID: "tab-1",
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
type UserClient struct {
	*BaseClient

	// Identity fields
	userID      string
	windowID    string
	workspace   string // Optional workspace association
	credentials map[string]string

	// Mutex for workspace updates
	workspaceMu sync.RWMutex
}

// NewUserClient creates a new UserClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// Returns an error if required options are missing (ServerAddr, UserID,
// WindowID).
func NewUserClient(opts UserOptions) (*UserClient, error) {
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

	// Create user client
	uc := &UserClient{
		BaseClient:  base,
		userID:      opts.UserID,
		windowID:    opts.WindowID,
		workspace:   opts.Workspace,
		credentials: opts.Credentials,
	}

	// Set the init message builder
	base.initMsgBuilder = uc.buildInitMessage

	return uc, nil
}

// buildInitMessage creates the InitConnection message for user identity.
func (c *UserClient) buildInitMessage() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_User{
			User: &pb.UserIdentity{
				UserId:   c.userID,
				WindowId: c.windowID,
			},
		},
		Credentials: c.credentials,
	}
}

// =============================================================================
// Identity Accessors
// =============================================================================

// UserID returns the user's unique identifier.
func (c *UserClient) UserID() string {
	return c.userID
}

// WindowID returns the user's window/session identifier.
func (c *UserClient) WindowID() string {
	return c.windowID
}

// Workspace returns the user's current workspace (if set).
func (c *UserClient) Workspace() string {
	c.workspaceMu.RLock()
	defer c.workspaceMu.RUnlock()
	return c.workspace
}

// Topic returns this user's topic address.
//
// Format: us.{user_id}.{window_id}
func (c *UserClient) Topic() string {
	return UserTopic(c.userID, c.windowID)
}

// WorkspaceTopic returns this user's workspace-scoped topic address.
//
// Format: uw.{user_id}.{workspace}
//
// Returns an empty string if no workspace is set.
func (c *UserClient) WorkspaceTopic() string {
	c.workspaceMu.RLock()
	workspace := c.workspace
	c.workspaceMu.RUnlock()

	if workspace == "" {
		return ""
	}
	return UserWorkspaceTopic(c.userID, workspace)
}

// =============================================================================
// Workspace Management
// =============================================================================

// SetWorkspace sets the user's current workspace for workspace-scoped operations.
//
// This is a local operation that does not notify the gateway. It's used for
// tracking which workspace the user is currently associated with for methods
// that require a workspace context.
func (c *UserClient) SetWorkspace(workspace string) {
	c.workspaceMu.Lock()
	c.workspace = workspace
	c.workspaceMu.Unlock()
}

// SwitchWorkspace sends a workspace switch request to the gateway and updates
// the local workspace field on success.
//
// This notifies the gateway that the user is switching to a different workspace,
// which updates subscriptions and workspace-scoped operations accordingly.
func (c *UserClient) SwitchWorkspace(workspace string) error {
	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_SwitchWorkspace{
			SwitchWorkspace: &pb.SwitchWorkspace{
				NewWorkspaceId: workspace,
			},
		},
	}
	if err := c.Send(msg); err != nil {
		return err
	}
	c.workspaceMu.Lock()
	c.workspace = workspace
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
func (c *UserClient) SendToAgent(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToAgentWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToAgentWithType sends a message to a specific agent with a custom message type.
func (c *UserClient) SendToAgentWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
	topic := AgentTopic(workspace, implementation, specifier)
	return c.sendMessage(topic, payload, msgType)
}

// SendToTask sends a message to a specific task.
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
//   - payload: Message payload (bytes)
//
// Uses CHAT message type by default. Use SendToTaskWithType for other types.
func (c *UserClient) SendToTask(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToTaskWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToTaskWithType sends a message to a specific task with a custom message type.
func (c *UserClient) SendToTaskWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
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
func (c *UserClient) SendToUser(userID, windowID string, payload []byte) error {
	return c.SendToUserWithType(userID, windowID, payload, pb.MessageType_OPAQUE)
}

// SendToUserWithType sends a message to a specific user session with a custom message type.
func (c *UserClient) SendToUserWithType(userID, windowID string, payload []byte, msgType pb.MessageType) error {
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
func (c *UserClient) SendToUserWorkspace(userID, workspace string, payload []byte) error {
	return c.SendToUserWorkspaceWithType(userID, workspace, payload, pb.MessageType_OPAQUE)
}

// SendToUserWorkspaceWithType sends a message to a user's workspace scope with a custom message type.
func (c *UserClient) SendToUserWorkspaceWithType(userID, workspace string, payload []byte, msgType pb.MessageType) error {
	topic := UserWorkspaceTopic(userID, workspace)
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
// Note: Users can only send direct messages. Sending to event.* or metric.*
// topics is not permitted and will be rejected by the gateway.
//
// Parameters:
//   - targetTopic: The destination topic string
//   - payload: Message payload (bytes)
//   - msgType: Message type (CHAT, CONTROL, TOOL_CALL)
func (c *UserClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *UserClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *UserClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendToolCallMessage sends a TOOL_CALL message to a specified topic.
func (c *UserClient) SendToolCallMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_TOOL_CALL)
}
