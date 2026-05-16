// Package aether task client implementation.
//
// This file provides the TaskClient type for connecting to the Aether gateway
// as a task. Tasks can be unique (with a specifier) or non-unique (server-assigned ID).
// Non-unique tasks subscribe to broadcast topics for work claiming.

package aether

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/protobuf/proto"
)

// =============================================================================
// Task Client
// =============================================================================

// TaskClient is a client for connecting to the Aether gateway as a task.
//
// Tasks are work units identified by workspace, implementation, and optionally
// a unique specifier. Tasks can be:
//
//   - Unique tasks: Have a specifier, can only have one active connection
//     at a time (similar to agents). Topic format: tu::{workspace}::{impl}::{spec}
//
//   - Non-unique tasks: No specifier, receive server-assigned IDs, multiple
//     instances can run simultaneously. They subscribe to both their specific
//     topic (ta::{workspace}::{impl}::{id}) and a broadcast topic
//     (tb::{workspace}::{impl}) for work claiming.
//
// TaskClient embeds BaseClient and adds task-specific functionality:
//   - Identity management (workspace, implementation, specifier)
//   - Messaging helpers (SendToAgent, SendToUser, SendToTask)
//   - Event and metric publishing
//   - Workspace switching
//
// Example usage (unique task):
//
//	client, err := aether.NewTaskClient(aether.TaskOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	    Workspace:      "prod",
//	    Implementation: "report-generator",
//	    Specifier:      "daily-report",  // Unique task
//	})
//
// Example usage (non-unique task):
//
//	client, err := aether.NewTaskClient(aether.TaskOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	    Workspace:      "prod",
//	    Implementation: "data-processor",
//	    // Specifier omitted - server assigns ID
//	})
type TaskClient struct {
	*BaseClient

	// Identity fields
	workspace      string
	implementation string
	specifier      string // Empty for non-unique tasks
	credentials    map[string]string

	// Server-assigned ID for non-unique tasks
	// This is populated from the ConnectionAck
	assignedID   string
	assignedIDMu sync.RWMutex

	// Mutex for workspace updates
	workspaceMu sync.RWMutex
}

// NewTaskClient creates a new TaskClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// For unique tasks (with Specifier set), the task identity is fixed.
// For non-unique tasks (without Specifier), the server assigns a unique ID
// upon connection.
//
// Returns an error if required options are missing (ServerAddr, Workspace,
// Implementation).
func NewTaskClient(opts TaskOptions) (*TaskClient, error) {
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

	// Create task client
	tc := &TaskClient{
		BaseClient:     base,
		workspace:      opts.Workspace,
		implementation: opts.Implementation,
		specifier:      opts.Specifier,
		credentials:    opts.Credentials,
	}

	// Set the init message builder
	base.initMsgBuilder = tc.buildInitMessage

	// Hook into connection ack to capture assigned ID for non-unique tasks
	origHandler := base.handlers.OnConnect
	base.handlers.OnConnect = func(ctx context.Context, ack *ConnectionAck) error {
		// For non-unique tasks, the session ID contains the assigned task ID
		// The server uses a convention where the assigned ID is part of the session
		if tc.specifier == "" && ack != nil {
			// Note: The assigned ID comes from the server's session management
			// For non-unique tasks, the session ID format includes the task ID
			tc.setAssignedID(ack.SessionID)
		}
		// Call original handler if it existed
		if origHandler != nil {
			return origHandler(ctx, ack)
		}
		return nil
	}

	return tc, nil
}

// buildInitMessage creates the InitConnection message for task identity.
func (c *TaskClient) buildInitMessage() *pb.InitConnection {
	c.workspaceMu.RLock()
	workspace := c.workspace
	c.workspaceMu.RUnlock()

	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Task{
			Task: &pb.TaskIdentity{
				Workspace:       workspace,
				Implementation:  c.implementation,
				UniqueSpecifier: c.specifier,
			},
		},
		Credentials: c.credentials,
	}
}

// =============================================================================
// Identity Accessors
// =============================================================================

// Workspace returns the task's current workspace.
func (c *TaskClient) Workspace() string {
	c.workspaceMu.RLock()
	defer c.workspaceMu.RUnlock()
	return c.workspace
}

// Implementation returns the task's implementation type.
func (c *TaskClient) Implementation() string {
	return c.implementation
}

// Specifier returns the task's specifier (empty for non-unique tasks).
func (c *TaskClient) Specifier() string {
	return c.specifier
}

// IsUnique returns true if this is a unique task (has a specifier).
func (c *TaskClient) IsUnique() bool {
	return c.specifier != ""
}

// AssignedID returns the server-assigned ID for non-unique tasks.
// Returns an empty string for unique tasks or if not yet assigned.
func (c *TaskClient) AssignedID() string {
	c.assignedIDMu.RLock()
	defer c.assignedIDMu.RUnlock()
	return c.assignedID
}

// setAssignedID sets the server-assigned ID (called when ConnectionAck is received).
func (c *TaskClient) setAssignedID(id string) {
	c.assignedIDMu.Lock()
	defer c.assignedIDMu.Unlock()
	c.assignedID = id
}

// Topic returns this task's topic address.
//
// For unique tasks:
//
//	Format: tu::{workspace}::{implementation}::{specifier}
//
// For non-unique tasks:
//
//	Format: ta::{workspace}::{implementation}::{assignedID}
//
// Note: For non-unique tasks, this returns an empty string until the
// server assigns an ID upon connection.
func (c *TaskClient) Topic() string {
	c.workspaceMu.RLock()
	workspace := c.workspace
	c.workspaceMu.RUnlock()

	if c.specifier != "" {
		// Unique task
		return UniqueTaskTopic(workspace, c.implementation, c.specifier)
	}

	// Non-unique task
	c.assignedIDMu.RLock()
	assignedID := c.assignedID
	c.assignedIDMu.RUnlock()

	if assignedID == "" {
		// Not yet assigned
		return ""
	}
	return TaskTopic(workspace, c.implementation, assignedID)
}

// BroadcastTopic returns the broadcast topic for this task's implementation.
//
// Format: tb::{workspace}::{implementation}
//
// This topic is used for work claiming by non-unique tasks. Messages sent
// to this topic are load-balanced across all available workers of this
// implementation type.
func (c *TaskClient) BroadcastTopic() string {
	c.workspaceMu.RLock()
	workspace := c.workspace
	c.workspaceMu.RUnlock()
	return TaskBroadcastTopic(workspace, c.implementation)
}

// =============================================================================
// Workspace Management
// =============================================================================

// SwitchWorkspace changes the task's workspace.
//
// This sends a SwitchWorkspace message to the gateway, which will:
//   - Update the task's topic subscription
//   - Optionally send a new ConfigSnapshot for the new workspace
//
// The workspace change takes effect after the gateway processes the request.
func (c *TaskClient) SwitchWorkspace(newWorkspace string) error {
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
func (c *TaskClient) SendToAgent(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToAgentWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToAgentWithType sends a message to a specific agent with a custom message type.
func (c *TaskClient) SendToAgentWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
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
func (c *TaskClient) SendToTask(workspace, implementation, specifier string, payload []byte) error {
	return c.SendToTaskWithType(workspace, implementation, specifier, payload, pb.MessageType_OPAQUE)
}

// SendToTaskWithType sends a message to a specific task with a custom message type.
func (c *TaskClient) SendToTaskWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
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
func (c *TaskClient) SendToUser(userID, windowID string, payload []byte) error {
	return c.SendToUserWithType(userID, windowID, payload, pb.MessageType_OPAQUE)
}

// SendToUserWithType sends a message to a specific user session with a custom message type.
func (c *TaskClient) SendToUserWithType(userID, windowID string, payload []byte, msgType pb.MessageType) error {
	topic := UserTopic(userID, windowID)
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
func (c *TaskClient) SendEvent(payload []byte) error {
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
func (c *TaskClient) SendMetric(metric *pb.Metric) error {
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
// the task is currently doing while running. Connection liveness handles death
// detection separately.
//
// Only agents and tasks may send progress reports; the gateway will reject
// progress from other principal types.
func (c *TaskClient) ReportProgress(opts ReportProgressOptions) error {
	return c.BaseClient.ReportProgress(opts)
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
func (c *TaskClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *TaskClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *TaskClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendToolCallMessage sends a TOOL_CALL message to a specified topic.
func (c *TaskClient) SendToolCallMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_TOOL_CALL)
}
