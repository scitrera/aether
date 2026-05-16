// Package aether metrics bridge client implementation.
//
// This file provides the MetricsBridgeClient type for connecting to the Aether
// gateway as a metrics bridge. The metrics bridge subscribes to metric.* topics
// and collects telemetry data from agents and tasks.

package aether

import (
	"fmt"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/protobuf/proto"
)

// =============================================================================
// Metrics Bridge Client
// =============================================================================

// MetricsBridgeClient is a client for connecting to the Aether gateway as a metrics bridge.
//
// The metrics bridge is a special client type that:
//   - Subscribes to metric.* topics (telemetry collection)
//   - Receives metrics from agents, tasks, and workflow engines
//   - Is primarily receive-only (processes incoming telemetry)
//   - Can send acknowledgments back to metric sources if needed
//
// MetricsBridges are typically used to:
//   - Forward metrics to external observability platforms (Prometheus, Datadog, etc.)
//   - Aggregate and process telemetry data
//   - Generate alerts based on metric thresholds
//   - Store metrics for historical analysis
//
// MetricsBridgeClient embeds BaseClient and adds metrics-specific functionality:
//   - Metric processing via OnMessage callback
//   - Acknowledgment sending to metric sources
//
// Example usage:
//
//	client, err := aether.NewMetricsBridgeClient(aether.MetricsBridgeOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Handle incoming metrics
//	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
//	    fmt.Printf("Received metric from %s\n", msg.SourceTopic)
//	    // Process metric data and forward to observability platform
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
type MetricsBridgeClient struct {
	*BaseClient

	// Specifier for this metrics bridge instance
	specifier   string
	credentials map[string]string
}

// NewMetricsBridgeClient creates a new MetricsBridgeClient with the given options.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
//
// Returns an error if required options are missing (ServerAddr).
func NewMetricsBridgeClient(opts MetricsBridgeOptions) (*MetricsBridgeClient, error) {
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

	// Create metrics bridge client
	mc := &MetricsBridgeClient{
		BaseClient:  base,
		specifier:   opts.Specifier,
		credentials: opts.Credentials,
	}

	// Set the init message builder
	base.initMsgBuilder = mc.buildInitMessage

	return mc, nil
}

// buildInitMessage creates the InitConnection message for metrics bridge identity.
//
// Note: Metrics bridges use a structured identity message (MetricsBridgeIdentity)
// for identification. The specifier field is stored locally but not sent to the server.
func (c *MetricsBridgeClient) buildInitMessage() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_MetricsBridge{
			MetricsBridge: &pb.MetricsBridgeIdentity{},
		},
		Credentials: c.credentials,
	}
}

// =============================================================================
// Identity Accessors
// =============================================================================

// Specifier returns the metrics bridge's specifier.
//
// If no specifier was provided during creation, this may be empty until
// the server assigns one upon connection.
func (c *MetricsBridgeClient) Specifier() string {
	return c.specifier
}

// =============================================================================
// Acknowledgment Sending
// =============================================================================

// SendAcknowledgment sends an acknowledgment/response message to a source topic.
//
// This is the primary method for metrics bridges to respond to metric sources
// if needed (e.g., confirming receipt of critical metrics).
//
// Parameters:
//   - targetTopic: The destination topic (typically the metric source)
//   - payload: Acknowledgment payload (bytes)
//
// Uses CONTROL message type.
func (c *MetricsBridgeClient) SendAcknowledgment(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendAcknowledgmentWithType sends an acknowledgment with a custom message type.
//
// Parameters:
//   - targetTopic: The destination topic
//   - payload: Acknowledgment payload (bytes)
//   - msgType: Message type (CONTROL, CHAT, etc.)
func (c *MetricsBridgeClient) SendAcknowledgmentWithType(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// =============================================================================
// Response Sending - Agents
// =============================================================================

// SendResponseToAgent sends a response/acknowledgment to a specific agent.
//
// Use this when you need to acknowledge receipt of metrics from a specific agent.
//
// Parameters:
//   - workspace: Target agent's workspace
//   - implementation: Target agent's implementation type
//   - specifier: Target agent's specifier
//   - payload: Response payload (bytes)
func (c *MetricsBridgeClient) SendResponseToAgent(workspace, implementation, specifier string, payload []byte) error {
	topic := AgentTopic(workspace, implementation, specifier)
	return c.sendMessage(topic, payload, pb.MessageType_CONTROL)
}

// =============================================================================
// Response Sending - Tasks
// =============================================================================

// SendResponseToTask sends a response/acknowledgment to a specific task.
//
// Use this when you need to acknowledge receipt of metrics from a specific task.
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
//   - specifier: Target task's specifier (empty for broadcast)
//   - payload: Response payload (bytes)
func (c *MetricsBridgeClient) SendResponseToTask(workspace, implementation, specifier string, payload []byte) error {
	var topic string
	if specifier != "" {
		// Unique task
		topic = UniqueTaskTopic(workspace, implementation, specifier)
	} else {
		// Non-unique task broadcast
		topic = TaskBroadcastTopic(workspace, implementation)
	}
	return c.sendMessage(topic, payload, pb.MessageType_CONTROL)
}

// =============================================================================
// Generic Message Sending
// =============================================================================

// SendMessage sends a message to a specified topic.
//
// This is the low-level sending method. For most use cases, prefer the
// type-specific helpers (SendAcknowledgment, SendResponseToAgent, etc.).
//
// Note: Metrics bridges are primarily receive-only. Use this method sparingly
// and only when you need to respond to metric sources.
//
// Parameters:
//   - targetTopic: The destination topic string
//   - payload: Message payload (bytes)
//   - msgType: Message type (CHAT, CONTROL, TOOL_CALL, EVENT, METRIC)
func (c *MetricsBridgeClient) SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	return c.sendMessage(targetTopic, payload, msgType)
}

// SendControlMessage sends a CONTROL message to a specified topic.
func (c *MetricsBridgeClient) SendControlMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_CONTROL)
}

// SendChatMessage sends a CHAT message to a specified topic.
func (c *MetricsBridgeClient) SendChatMessage(targetTopic string, payload []byte) error {
	return c.sendMessage(targetTopic, payload, pb.MessageType_OPAQUE)
}

// =============================================================================
// Metric Parsing Helper
// =============================================================================

// ParseMetric unmarshals a raw METRIC message payload into a *pb.Metric.
//
// MetricsBridgeClient consumers can use this inside their OnMessage handler
// to decode incoming metric payloads received from agents or tasks.
//
// Example:
//
//	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
//	    m, err := aether.ParseMetric(msg.Payload)
//	    if err != nil {
//	        return fmt.Errorf("bad metric payload: %w", err)
//	    }
//	    for _, e := range m.Entries {
//	        fmt.Printf("%s/%s = %f\n", e.Name, e.Kind, e.Qty)
//	    }
//	    return nil
//	})
func ParseMetric(payload []byte) (*pb.Metric, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("metric payload is empty")
	}
	m := &pb.Metric{}
	if err := proto.Unmarshal(payload, m); err != nil {
		return nil, fmt.Errorf("unmarshal metric: %w", err)
	}
	return m, nil
}
