// Package aether orchestrator client implementation.
//
// This file provides the OrchestratorClient type for connecting to the Aether
// gateway as an orchestrator. Orchestrators are responsible for managing
// agent/task lifecycle, including launching compute resources and handling
// task assignments.

package aether

import (
	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Orchestrator Client
// =============================================================================

// OrchestratorClient is a client for connecting to the Aether gateway as an orchestrator.
//
// Orchestrators are responsible for managing agent/task lifecycle:
//   - Receiving startup requests when targeted agents are offline
//   - Launching compute resources (containers, VMs, etc.)
//   - Managing agent pools and scaling
//   - Processing task assignments based on supported profiles
//
// Orchestrators register with a list of profiles they support. When the gateway
// needs to start an agent matching one of these profiles, it sends a TaskAssignment
// to the appropriate orchestrator.
//
// OrchestratorClient embeds BaseClient and adds orchestrator-specific functionality:
//   - Profile management (list of supported profiles)
//   - Task assignment handling via OnTaskAssignment callback
//   - Status/control message sending to agents and tasks
//
// Example usage:
//
//	client, err := aether.NewOrchestratorClient(aether.OrchestratorOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	    Implementation:    "kubernetes-orchestrator",
//	    SupportedProfiles: []string{"k8s-worker", "k8s-agent"},
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	client.OnTaskAssignment(func(ctx context.Context, assignment *aether.TaskAssignment) error {
//	    fmt.Printf("Received task assignment: %s (profile: %s)\n",
//	        assignment.TaskID, assignment.Profile)
//	    // Launch the appropriate compute resource based on the assignment
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
type OrchestratorClient struct {
	*BaseClient

	// Identity fields
	implementation    string
	specifier         string   // Optional; server generates if empty
	supportedProfiles []string // Required; at least one profile
	credentials       map[string]string
}

// NewOrchestratorClient creates a new OrchestratorClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// Returns an error if required options are missing (ServerAddr, Implementation,
// at least one SupportedProfile).
func NewOrchestratorClient(opts OrchestratorOptions) (*OrchestratorClient, error) {
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

	// Copy supported profiles slice to avoid aliasing issues
	profiles := make([]string, len(opts.SupportedProfiles))
	copy(profiles, opts.SupportedProfiles)

	// Create orchestrator client
	oc := &OrchestratorClient{
		BaseClient:        base,
		implementation:    opts.Implementation,
		specifier:         opts.Specifier,
		supportedProfiles: profiles,
		credentials:       opts.Credentials,
	}

	// Set the init message builder
	base.initMsgBuilder = oc.buildInitMessage

	return oc, nil
}

// buildInitMessage creates the InitConnection message for orchestrator identity.
func (c *OrchestratorClient) buildInitMessage() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Orchestrator{
			Orchestrator: &pb.OrchestratorIdentity{
				Implementation:    c.implementation,
				Specifier:         c.specifier,
				SupportedProfiles: c.supportedProfiles,
			},
		},
		Credentials: c.credentials,
	}
}

// =============================================================================
// Identity Accessors
// =============================================================================

// Implementation returns the orchestrator's implementation type.
//
// Example: "kubernetes-orchestrator", "docker-orchestrator"
func (c *OrchestratorClient) Implementation() string {
	return c.implementation
}

// Specifier returns the orchestrator's specifier (unique instance identifier).
//
// If no specifier was provided during creation, this may be empty until
// the server assigns one upon connection.
func (c *OrchestratorClient) Specifier() string {
	return c.specifier
}

// SupportedProfiles returns a copy of the profiles this orchestrator supports.
//
// Profiles define which types of agents/tasks this orchestrator can spawn.
// The gateway routes task assignments to orchestrators based on matching profiles.
func (c *OrchestratorClient) SupportedProfiles() []string {
	// Return a copy to prevent external modification
	profiles := make([]string, len(c.supportedProfiles))
	copy(profiles, c.supportedProfiles)
	return profiles
}

// SupportsProfile returns true if this orchestrator supports the given profile.
func (c *OrchestratorClient) SupportsProfile(profile string) bool {
	for _, p := range c.supportedProfiles {
		if p == profile {
			return true
		}
	}
	return false
}

// =============================================================================
// Message Sending Helpers
// =============================================================================

// SendStatusToAgent sends a status/control message to a specific agent.
//
// This is typically used to communicate orchestration status updates,
// such as startup confirmations, health checks, or shutdown notices.
//
// Parameters:
//   - workspace: Target agent's workspace
//   - implementation: Target agent's implementation type
//   - specifier: Target agent's specifier
//   - payload: Message payload (bytes)
//
// Uses CONTROL message type. Use SendStatusToAgentWithType for other types.
func (c *OrchestratorClient) SendStatusToAgent(workspace, implementation, specifier string, payload []byte) error {
	return c.SendStatusToAgentWithType(workspace, implementation, specifier, payload, pb.MessageType_CONTROL)
}

// SendStatusToAgentWithType sends a status message to a specific agent with a custom message type.
func (c *OrchestratorClient) SendStatusToAgentWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
	topic := AgentTopic(workspace, implementation, specifier)
	return c.sendMessage(topic, payload, msgType)
}

// SendStatusToTask sends a status/control message to a specific task.
//
// For unique tasks (with specifier):
//   - Uses tu::{workspace}::{implementation}::{specifier} topic
//
// For non-unique tasks (empty specifier):
//   - Uses tb::{workspace}::{implementation} broadcast topic
//
// Parameters:
//   - workspace: Target task's workspace
//   - implementation: Target task's implementation type
//   - specifier: Target task's specifier (empty for broadcast to non-unique tasks)
//   - payload: Message payload (bytes)
//
// Uses CONTROL message type. Use SendStatusToTaskWithType for other types.
func (c *OrchestratorClient) SendStatusToTask(workspace, implementation, specifier string, payload []byte) error {
	return c.SendStatusToTaskWithType(workspace, implementation, specifier, payload, pb.MessageType_CONTROL)
}

// SendStatusToTaskWithType sends a status message to a specific task with a custom message type.
func (c *OrchestratorClient) SendStatusToTaskWithType(workspace, implementation, specifier string, payload []byte, msgType pb.MessageType) error {
	var topic string
	if specifier != "" {
		// Unique task
		topic = UniqueTaskTopic(workspace, implementation, specifier)
	} else {
		// Non-unique task broadcast
		topic = TaskBroadcastTopic(workspace, implementation)
	}
	return c.sendMessage(topic, payload, msgType)
}

// =============================================================================
// Generic Message Sending
// =============================================================================

// SendMessage sends a message to a specified topic.
//
// This is the low-level sending method. For most use cases, prefer the
// type-specific helpers (SendStatusToAgent, SendStatusToTask, etc.).
//
// Note: Per the Aether specification, orchestrators primarily send status
// updates to agents and tasks. They typically do not send user-facing
// messages or publish events/metrics.
//
// Parameters:
//   - targetTopic: The destination topic string
//   - payload: Message payload (bytes)
//   - msgType: Message type (CHAT, CONTROL, TOOL_CALL, EVENT, METRIC)
func (c *OrchestratorClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *OrchestratorClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *OrchestratorClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}
