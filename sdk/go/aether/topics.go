// Package aether topic construction helpers.
//
// This file provides helper functions for constructing topic strings used
// for message routing in the Aether system. Topics follow a structured
// format that identifies the principal type and location.
//
// Topic Schema:
//   - ag.{workspace}.{impl}.{spec} - Specific agent instance
//   - tu.{workspace}.{impl}.{spec} - Unique task (named)
//   - ta.{workspace}.{impl}.{id} - Non-unique task instance (server-assigned ID)
//   - tb.{workspace}.{impl} - Task broadcast (load-balancing for non-unique tasks)
//   - us.{user_id}.{window_id} - User window-specific messages
//   - uw.{user_id}.{workspace} - User workspace-scoped messages
//   - ga.{workspace} - Global agent broadcast in workspace
//   - gu.{workspace} - Global user broadcast in workspace
//   - event.* - Workflow Engine only (broadcast events)
//   - metric.* - Metrics Bridge only (telemetry ingestion)

package aether

import "fmt"

// =============================================================================
// Topic Prefixes
// =============================================================================

// TopicPrefix constants define the prefix for each topic type.
const (
	// TopicPrefixAgent is the prefix for agent topics.
	TopicPrefixAgent = "ag"

	// TopicPrefixUniqueTask is the prefix for unique task topics.
	TopicPrefixUniqueTask = "tu"

	// TopicPrefixTask is the prefix for non-unique task topics.
	TopicPrefixTask = "ta"

	// TopicPrefixTaskBroadcast is the prefix for task broadcast topics.
	TopicPrefixTaskBroadcast = "tb"

	// TopicPrefixUser is the prefix for user session topics.
	TopicPrefixUser = "us"

	// TopicPrefixUserWorkspace is the prefix for user workspace topics.
	TopicPrefixUserWorkspace = "uw"

	// TopicPrefixGlobalAgents is the prefix for global agent broadcast topics.
	TopicPrefixGlobalAgents = "ga"

	// TopicPrefixGlobalUsers is the prefix for global user broadcast topics.
	TopicPrefixGlobalUsers = "gu"

	// TopicPrefixEvent is the prefix for event topics (workflow engine).
	TopicPrefixEvent = "event"

	// TopicPrefixMetric is the prefix for metric topics (metrics bridge).
	TopicPrefixMetric = "metric"

	// TopicPrefixProgress is the prefix for progress stream topics.
	TopicPrefixProgress = "pg"

	// TopicPrefixBridge is the prefix for bridge topics.
	TopicPrefixBridge = "br"
)

// =============================================================================
// Topic Construction Helpers - Bridges
// =============================================================================

// BridgeTopic creates a topic string for a specific bridge instance.
//
// Format: br.{implementation}.{specifier}
//
// Example:
//
//	topic := aether.BridgeTopic("aether-msgbridge", "instance-1")
//	// Returns: "br.aether-msgbridge.instance-1"
func BridgeTopic(implementation, specifier string) string {
	return fmt.Sprintf("%s.%s.%s", TopicPrefixBridge, implementation, specifier)
}

// =============================================================================
// Topic Construction Helpers - Agents
// =============================================================================

// AgentTopic creates a topic string for a specific agent.
//
// Format: ag.{workspace}.{implementation}.{specifier}
//
// Example:
//
//	topic := aether.AgentTopic("prod", "data-processor", "instance-1")
//	// Returns: "ag.prod.data-processor.instance-1"
func AgentTopic(workspace, implementation, specifier string) string {
	return fmt.Sprintf("%s.%s.%s.%s", TopicPrefixAgent, workspace, implementation, specifier)
}

// GlobalAgentsTopic creates a broadcast topic for all agents in a workspace.
//
// Format: ga.{workspace}
//
// Messages sent to this topic are delivered to all agents in the workspace.
//
// Example:
//
//	topic := aether.GlobalAgentsTopic("prod")
//	// Returns: "ga.prod"
func GlobalAgentsTopic(workspace string) string {
	return fmt.Sprintf("%s.%s", TopicPrefixGlobalAgents, workspace)
}

// =============================================================================
// Topic Construction Helpers - Tasks
// =============================================================================

// UniqueTaskTopic creates a topic string for a unique (named) task.
//
// Format: tu.{workspace}.{implementation}.{specifier}
//
// Unique tasks have a persistent identity like agents and can only have
// one active connection at a time.
//
// Example:
//
//	topic := aether.UniqueTaskTopic("prod", "report-generator", "daily-report")
//	// Returns: "tu.prod.report-generator.daily-report"
func UniqueTaskTopic(workspace, implementation, specifier string) string {
	return fmt.Sprintf("%s.%s.%s.%s", TopicPrefixUniqueTask, workspace, implementation, specifier)
}

// TaskTopic creates a topic string for a non-unique task instance.
//
// Format: ta.{workspace}.{implementation}.{id}
//
// Non-unique tasks receive a server-assigned ID. They also subscribe to the
// broadcast topic for work claiming via TaskBroadcastTopic.
//
// Example:
//
//	topic := aether.TaskTopic("prod", "data-processor", "abc123")
//	// Returns: "ta.prod.data-processor.abc123"
func TaskTopic(workspace, implementation, id string) string {
	return fmt.Sprintf("%s.%s.%s.%s", TopicPrefixTask, workspace, implementation, id)
}

// TaskBroadcastTopic creates a broadcast topic for task load balancing.
//
// Format: tb.{workspace}.{implementation}
//
// Non-unique tasks subscribe to this topic to receive work items that can be
// claimed by any available worker of that implementation type.
//
// Example:
//
//	topic := aether.TaskBroadcastTopic("prod", "data-processor")
//	// Returns: "tb.prod.data-processor"
func TaskBroadcastTopic(workspace, implementation string) string {
	return fmt.Sprintf("%s.%s.%s", TopicPrefixTaskBroadcast, workspace, implementation)
}

// =============================================================================
// Topic Construction Helpers - Users
// =============================================================================

// UserTopic creates a topic string for a user session.
//
// Format: us.{user_id}.{window_id}
//
// Users are identified by user_id and window_id, allowing multiple browser
// tabs or sessions per user.
//
// Example:
//
//	topic := aether.UserTopic("alice", "tab-1")
//	// Returns: "us.alice.tab-1"
func UserTopic(userID, windowID string) string {
	return fmt.Sprintf("%s.%s.%s", TopicPrefixUser, userID, windowID)
}

// UserWorkspaceTopic creates a topic string for user workspace messages.
//
// Format: uw.{user_id}.{workspace}
//
// Messages sent to this topic reach a specific user's workspace scope,
// regardless of which window/tab they're using.
//
// Example:
//
//	topic := aether.UserWorkspaceTopic("alice", "prod")
//	// Returns: "uw.alice.prod"
func UserWorkspaceTopic(userID, workspace string) string {
	return fmt.Sprintf("%s.%s.%s", TopicPrefixUserWorkspace, userID, workspace)
}

// GlobalUsersTopic creates a broadcast topic for all users in a workspace.
//
// Format: gu.{workspace}
//
// Messages sent to this topic are delivered to all users in the workspace.
//
// Example:
//
//	topic := aether.GlobalUsersTopic("prod")
//	// Returns: "gu.prod"
func GlobalUsersTopic(workspace string) string {
	return fmt.Sprintf("%s.%s", TopicPrefixGlobalUsers, workspace)
}

// =============================================================================
// Topic Construction Helpers - System Topics
// =============================================================================

// EventTopic creates a topic string for broadcast events.
//
// Format: event.{event_type}
//
// Event topics are used by the Workflow Engine to receive and process
// broadcast events. Only workflow engines can subscribe to event topics.
//
// Example:
//
//	topic := aether.EventTopic("task.completed")
//	// Returns: "event.task.completed"
func EventTopic(eventType string) string {
	return fmt.Sprintf("%s.%s", TopicPrefixEvent, eventType)
}

// EventWildcardTopic returns the wildcard pattern for all events.
//
// Format: event.*
//
// This is the topic that Workflow Engines subscribe to for receiving all events.
func EventWildcardTopic() string {
	return TopicPrefixEvent + ".*"
}

// MetricTopic creates a topic string for telemetry/metrics.
//
// Format: metric.{metric_type}
//
// Metric topics are used by the Metrics Bridge to receive telemetry data.
// Only metrics bridges can subscribe to metric topics.
//
// Example:
//
//	topic := aether.MetricTopic("performance")
//	// Returns: "metric.performance"
func MetricTopic(metricType string) string {
	return fmt.Sprintf("%s.%s", TopicPrefixMetric, metricType)
}

// MetricWildcardTopic returns the wildcard pattern for all metrics.
//
// Format: metric.*
//
// This is the topic that Metrics Bridges subscribe to for receiving all metrics.
func MetricWildcardTopic() string {
	return TopicPrefixMetric + ".*"
}

// ProgressTopic creates a topic string for workspace progress updates.
//
// Format: pg.{workspace}
//
// Progress updates from agents and tasks in a workspace are published to this
// topic. Users and agents subscribe to it with server-side recipient filtering.
func ProgressTopic(workspace string) string {
	return fmt.Sprintf("%s.%s", TopicPrefixProgress, workspace)
}

// =============================================================================
// Topic Aliases for Convenience
// =============================================================================

// The following are convenience aliases that match the Python client API.
// They call the primary topic functions above.

// CreateTopicAgent is an alias for AgentTopic.
// Provided for API compatibility with the Python client.
func CreateTopicAgent(workspace, implementation, specifier string) string {
	return AgentTopic(workspace, implementation, specifier)
}

// CreateTopicTask creates a topic string for a task.
// If specifier is provided, creates a unique task topic (tu.*).
// If specifier is empty, returns a partial topic ending in "." for server ID assignment.
//
// This matches the Python client behavior for create_topic_task.
func CreateTopicTask(workspace, implementation, specifier string) string {
	if specifier != "" {
		return UniqueTaskTopic(workspace, implementation, specifier)
	}
	// Non-unique task: return partial topic with trailing dot
	// The server will append the assigned ID
	return fmt.Sprintf("%s.%s.%s.", TopicPrefixTask, workspace, implementation)
}

// CreateTopicTaskBroadcast is an alias for TaskBroadcastTopic.
// Provided for API compatibility with the Python client.
func CreateTopicTaskBroadcast(workspace, implementation string) string {
	return TaskBroadcastTopic(workspace, implementation)
}

// CreateTopicUser is an alias for UserTopic.
// Provided for API compatibility with the Python client.
func CreateTopicUser(userID, windowID string) string {
	return UserTopic(userID, windowID)
}

// CreateTopicUserWorkspace is an alias for UserWorkspaceTopic.
// Provided for API compatibility with the Python client.
func CreateTopicUserWorkspace(userID, workspace string) string {
	return UserWorkspaceTopic(userID, workspace)
}

// CreateTopicGlobalAgents is an alias for GlobalAgentsTopic.
// Provided for API compatibility with the Python client.
func CreateTopicGlobalAgents(workspace string) string {
	return GlobalAgentsTopic(workspace)
}

// CreateTopicGlobalUsers is an alias for GlobalUsersTopic.
// Provided for API compatibility with the Python client.
func CreateTopicGlobalUsers(workspace string) string {
	return GlobalUsersTopic(workspace)
}
