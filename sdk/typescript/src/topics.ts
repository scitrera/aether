/**
 * Topic construction helpers for the Aether TypeScript SDK.
 *
 * This module provides helper functions for constructing topic strings used
 * for message routing in the Aether system. Topics follow a structured
 * format that identifies the principal type and location.
 *
 * Segment separator is "::" (not ".") so field values can legitimately
 * contain "." — e.g. Python FQN implementations
 * ("scitrera_ai_runtime.cowork.aether_bridge.CoworkAgent") or email-style
 * user_ids ("alice@example.com") — without the parser ambiguity that a
 * dotted separator would create.
 *
 * Topic Schema:
 * - ag::{workspace}::{impl}::{spec} - Specific agent instance
 * - tu::{workspace}::{impl}::{spec} - Unique task (named)
 * - ta::{workspace}::{impl}::{id}   - Non-unique task instance (server-assigned ID)
 * - tb::{workspace}::{impl}         - Task broadcast (load-balancing)
 * - us::{user_id}::{window_id}      - User window-specific messages
 * - uw::{user_id}::{workspace}      - User workspace-scoped messages
 * - ga::{workspace}                 - Global agent broadcast in workspace
 * - gu::{workspace}                 - Global user broadcast in workspace
 * - event::*                        - Workflow Engine only (broadcast events)
 * - metric::*                       - Metrics Bridge only (telemetry ingestion)
 *
 * @module topics
 */

// =============================================================================
// Identity Separator
// =============================================================================

/**
 * Segment separator for all identity / topic strings. Must match
 * server/pkg/models/topics.go::IdentitySep and the Python SDK's IDENTITY_SEP.
 */
export const IDENTITY_SEP = "::";

// =============================================================================
// Topic Prefixes
// =============================================================================

/** Prefix for agent topics. */
export const TOPIC_PREFIX_AGENT = "ag";

/** Prefix for unique task topics. */
export const TOPIC_PREFIX_UNIQUE_TASK = "tu";

/** Prefix for non-unique task topics. */
export const TOPIC_PREFIX_TASK = "ta";

/** Prefix for task broadcast topics. */
export const TOPIC_PREFIX_TASK_BROADCAST = "tb";

/** Prefix for user session topics. */
export const TOPIC_PREFIX_USER = "us";

/** Prefix for user workspace topics. */
export const TOPIC_PREFIX_USER_WORKSPACE = "uw";

/** Prefix for global agent broadcast topics. */
export const TOPIC_PREFIX_GLOBAL_AGENTS = "ga";

/** Prefix for global user broadcast topics. */
export const TOPIC_PREFIX_GLOBAL_USERS = "gu";

/** Prefix for event topics (workflow engine). */
export const TOPIC_PREFIX_EVENT = "event";

/** Prefix for metric topics (metrics bridge). */
export const TOPIC_PREFIX_METRIC = "metric";

/** Prefix for progress stream topics. */
export const TOPIC_PREFIX_PROGRESS = "pg";

/** Prefix for bridge topics. */
export const TOPIC_PREFIX_BRIDGE = "br";

// =============================================================================
// Topic Construction Helpers - Agents
// =============================================================================

/**
 * Creates a topic string for a specific agent.
 *
 * @param workspace - The agent's workspace
 * @param implementation - The agent's implementation type
 * @param specifier - The agent's unique specifier
 * @returns Topic string in format: ag::{workspace}::{implementation}::{specifier}
 *
 * @example
 * ```typescript
 * const topic = agentTopic("prod", "data-processor", "instance-1");
 * // Returns: "ag::prod::data-processor::instance-1"
 * ```
 */
export function agentTopic(workspace: string, implementation: string, specifier: string): string {
  return `${TOPIC_PREFIX_AGENT}${IDENTITY_SEP}${workspace}${IDENTITY_SEP}${implementation}${IDENTITY_SEP}${specifier}`;
}

/**
 * Creates a broadcast topic for all agents in a workspace.
 *
 * @param workspace - The workspace to broadcast to
 * @returns Topic string in format: ga::{workspace}
 */
export function globalAgentsTopic(workspace: string): string {
  return `${TOPIC_PREFIX_GLOBAL_AGENTS}${IDENTITY_SEP}${workspace}`;
}

// =============================================================================
// Topic Construction Helpers - Tasks
// =============================================================================

/**
 * Creates a topic string for a unique (named) task.
 *
 * @param workspace - The task's workspace
 * @param implementation - The task's implementation type
 * @param specifier - The task's unique specifier
 * @returns Topic string in format: tu::{workspace}::{implementation}::{specifier}
 */
export function uniqueTaskTopic(workspace: string, implementation: string, specifier: string): string {
  return `${TOPIC_PREFIX_UNIQUE_TASK}${IDENTITY_SEP}${workspace}${IDENTITY_SEP}${implementation}${IDENTITY_SEP}${specifier}`;
}

/**
 * Creates a topic string for a non-unique task instance.
 *
 * @param workspace - The task's workspace
 * @param implementation - The task's implementation type
 * @param id - The server-assigned task instance ID
 * @returns Topic string in format: ta::{workspace}::{implementation}::{id}
 */
export function taskTopic(workspace: string, implementation: string, id: string): string {
  return `${TOPIC_PREFIX_TASK}${IDENTITY_SEP}${workspace}${IDENTITY_SEP}${implementation}${IDENTITY_SEP}${id}`;
}

/**
 * Creates a broadcast topic for task load balancing.
 *
 * Non-unique tasks subscribe to this topic to receive work items that can
 * be claimed by any available worker of that implementation type.
 *
 * @param workspace - The task's workspace
 * @param implementation - The task's implementation type
 * @returns Topic string in format: tb::{workspace}::{implementation}
 */
export function taskBroadcastTopic(workspace: string, implementation: string): string {
  return `${TOPIC_PREFIX_TASK_BROADCAST}${IDENTITY_SEP}${workspace}${IDENTITY_SEP}${implementation}`;
}

// =============================================================================
// Topic Construction Helpers - Users
// =============================================================================

/**
 * Creates a topic string for a user session.
 *
 * @param userId - The user's unique identifier
 * @param windowId - The window/session identifier
 * @returns Topic string in format: us::{userId}::{windowId}
 */
export function userTopic(userId: string, windowId: string): string {
  return `${TOPIC_PREFIX_USER}${IDENTITY_SEP}${userId}${IDENTITY_SEP}${windowId}`;
}

/**
 * Creates a topic string for user workspace messages.
 *
 * Messages sent to this topic reach a specific user regardless of which
 * window/tab they are using.
 *
 * @param userId - The user's unique identifier
 * @param workspace - The workspace
 * @returns Topic string in format: uw::{userId}::{workspace}
 */
export function userWorkspaceTopic(userId: string, workspace: string): string {
  return `${TOPIC_PREFIX_USER_WORKSPACE}${IDENTITY_SEP}${userId}${IDENTITY_SEP}${workspace}`;
}

/**
 * Creates a broadcast topic for all users in a workspace.
 *
 * @param workspace - The workspace to broadcast to
 * @returns Topic string in format: gu::{workspace}
 */
export function globalUsersTopic(workspace: string): string {
  return `${TOPIC_PREFIX_GLOBAL_USERS}${IDENTITY_SEP}${workspace}`;
}

// =============================================================================
// Topic Construction Helpers - System Topics
// =============================================================================

/**
 * Creates a topic string for broadcast events.
 *
 * @param eventType - The event type identifier
 * @returns Topic string in format: event::{eventType}
 */
export function eventTopic(eventType: string): string {
  return `${TOPIC_PREFIX_EVENT}${IDENTITY_SEP}${eventType}`;
}

/**
 * Returns the wildcard pattern for all events.
 *
 * This is the topic that Workflow Engines subscribe to.
 *
 * @returns "event::*"
 */
export function eventWildcardTopic(): string {
  return `${TOPIC_PREFIX_EVENT}${IDENTITY_SEP}*`;
}

/**
 * Creates a topic string for telemetry/metrics.
 *
 * @param metricType - The metric type identifier
 * @returns Topic string in format: metric::{metricType}
 */
export function metricTopic(metricType: string): string {
  return `${TOPIC_PREFIX_METRIC}${IDENTITY_SEP}${metricType}`;
}

/**
 * Returns the wildcard pattern for all metrics.
 *
 * This is the topic that Metrics Bridges subscribe to.
 *
 * @returns "metric::*"
 */
export function metricWildcardTopic(): string {
  return `${TOPIC_PREFIX_METRIC}${IDENTITY_SEP}*`;
}

/**
 * Creates a topic string for workspace progress updates.
 *
 * Progress updates from agents and tasks in a workspace are published to this
 * topic. Users and agents subscribe to it with server-side recipient filtering.
 *
 * @param workspace - The workspace
 * @returns Topic string in format: pg::{workspace}
 */
export function progressTopic(workspace: string): string {
  return `${TOPIC_PREFIX_PROGRESS}${IDENTITY_SEP}${workspace}`;
}

// =============================================================================
// Topic Construction Helpers - Bridges
// =============================================================================

/**
 * Creates a topic string for a specific bridge instance.
 *
 * Bridges are cross-workspace principals identified only by implementation
 * and specifier (no workspace component).
 *
 * @param implementation - The bridge implementation type (e.g., "aether-msgbridge")
 * @param specifier - The bridge's unique specifier (e.g., "discord-1")
 * @returns Topic string in format: br::{implementation}::{specifier}
 *
 * @example
 * ```typescript
 * const topic = bridgeTopic("aether-msgbridge", "discord-1");
 * // Returns: "br::aether-msgbridge::discord-1"
 * ```
 */
export function bridgeTopic(implementation: string, specifier: string): string {
  return `${TOPIC_PREFIX_BRIDGE}${IDENTITY_SEP}${implementation}${IDENTITY_SEP}${specifier}`;
}