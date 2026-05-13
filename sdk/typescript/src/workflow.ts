/**
 * Workflow engine client implementation for the Aether TypeScript SDK.
 *
 * The workflow engine is the sole subscriber to event.* topics and processes
 * broadcast events to trigger downstream actions by sending commands to
 * agents/tasks. It is a singleton per gateway deployment.
 *
 * @module workflow
 */

import { AetherClient } from "./client.js";
import type { AetherClientOptions } from "./client.js";
import { MessageType } from "./types.js";
import {
  agentTopic,
  uniqueTaskTopic,
  taskBroadcastTopic,
  globalAgentsTopic,
  globalUsersTopic,
  userTopic,
  metricWildcardTopic,
} from "./topics.js";
import type { Metric } from "./metrics-builder.js";

// =============================================================================
// Workflow Engine Client Options
// =============================================================================

/**
 * Configuration options for the WorkflowEngineClient.
 *
 * No additional identity fields are needed — the workflow engine is
 * identified by its principal type alone (singleton per gateway).
 */
export type WorkflowEngineClientOptions = AetherClientOptions;

// =============================================================================
// WorkflowEngineClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as a workflow engine.
 *
 * The workflow engine receives all events (event.*) and can send commands
 * to any principal type. It is the central event processor for the system.
 *
 * @example
 * ```typescript
 * const engine = new WorkflowEngineClient({
 *   address: "localhost:50051",
 * });
 *
 * engine.onMessage((msg) => {
 *   // Process incoming events
 *   const event = JSON.parse(new TextDecoder().decode(msg.payload));
 *   console.log(`Event from ${msg.sourceTopic}:`, event);
 * });
 *
 * await engine.connect();
 * ```
 */
export class WorkflowEngineClient extends AetherClient {
  constructor(options: WorkflowEngineClientOptions) {
    super(options);
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      workflowEngine: {},
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Message Sending Helpers
  // ===========================================================================

  /**
   * Sends a command to a specific agent.
   */
  sendCommandToAgent(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Control,
  ): void {
    this._sendMessage(agentTopic(workspace, implementation, specifier), payload, messageType);
  }

  /**
   * Sends a command to a specific task.
   * For unique tasks (with specifier), uses tu.* topic.
   * For non-unique tasks (empty specifier), uses tb.* broadcast topic.
   */
  sendCommandToTask(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Control,
  ): void {
    const topic = specifier
      ? uniqueTaskTopic(workspace, implementation, specifier)
      : taskBroadcastTopic(workspace, implementation);
    this._sendMessage(topic, payload, messageType);
  }

  /**
   * Sends a broadcast to all agents in a workspace.
   */
  broadcastToAgents(
    workspace: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Control,
  ): void {
    this._sendMessage(globalAgentsTopic(workspace), payload, messageType);
  }

  /**
   * Sends a broadcast to all users in a workspace.
   */
  broadcastToUsers(
    workspace: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(globalUsersTopic(workspace), payload, messageType);
  }

  /**
   * Sends a message to a specific user session.
   */
  sendToUser(
    userId: string,
    windowId: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(userTopic(userId, windowId), payload, messageType);
  }

  /**
   * Publishes a metric to the metrics bridge.
   *
   * @param metric - Structured metric to publish. Use {@link newMetric} to construct one.
   */
  sendMetric(metric: Metric): void {
    const payload = this._encodeMetric(metric as Record<string, unknown>);
    this._sendMessage(metricWildcardTopic(), payload, MessageType.Metric);
  }
}
