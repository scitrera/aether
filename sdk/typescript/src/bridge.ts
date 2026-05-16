/**
 * Bridge client implementation for the Aether TypeScript SDK.
 *
 * This module provides the BridgeClient class for connecting to the Aether
 * gateway as a cross-workspace message bridge. Bridges relay messages across
 * workspace boundaries and can send to any principal type in any workspace.
 *
 * Unlike workspace-scoped principals (agents, tasks, users), bridges have no
 * workspace field in their identity — allowing them to send to targets in any
 * workspace. Each bridge identity can only have one active connection at a time
 * (Connection = Lock paradigm).
 *
 * @module bridge
 */

import { AetherClient } from "./client.js";
import type { AetherClientOptions } from "./client.js";
import { MessageType } from "./types.js";
import { InvalidArgumentError } from "./errors.js";
import {
  agentTopic,
  bridgeTopic,
  uniqueTaskTopic,
  taskBroadcastTopic,
  userTopic,
  userWorkspaceTopic,
  globalAgentsTopic,
  globalUsersTopic,
} from "./topics.js";

// =============================================================================
// Bridge Client Options
// =============================================================================

/**
 * Configuration options for the BridgeClient.
 */
export interface BridgeClientOptions extends AetherClientOptions {
  /** The bridge implementation type (e.g., "aether-msgbridge", "webhook-bridge"). Required. */
  implementation: string;
  /** The unique specifier for this bridge instance (e.g., "default", "discord-1"). Required. */
  specifier: string;
}

// =============================================================================
// BridgeClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as a cross-workspace message bridge.
 *
 * Bridges are identified by implementation and specifier with no workspace field,
 * allowing them to operate across workspace boundaries. Each bridge identity can
 * only have one active connection at a time (Connection = Lock paradigm).
 *
 * BridgeClient extends AetherClient and adds bridge-specific functionality:
 * - Identity management (implementation, specifier)
 * - Messaging helpers to any workspace (sendToAgent, sendToUser, sendToTask, broadcast)
 *
 * Topic format: br::{implementation}::{specifier}
 *
 * @example
 * ```typescript
 * import { BridgeClient } from "@scitrera/aether-client";
 *
 * const bridge = new BridgeClient({
 *   address: "localhost:50051",
 *   implementation: "aether-msgbridge",
 *   specifier: "discord-1",
 * });
 *
 * bridge.onMessage((msg) => {
 *   console.log(`Received from ${msg.sourceTopic}:`, msg.payload);
 * });
 *
 * await bridge.connect();
 *
 * // Send to any workspace — bridges are cross-workspace
 * const encoder = new TextEncoder();
 * bridge.sendToAgent("prod", "my-agent", "instance-1", encoder.encode("Hello!"));
 * bridge.sendToUser("alice", "tab-1", encoder.encode("Notification from Discord"));
 * ```
 */
export class BridgeClient extends AetherClient {
  private readonly _implementation: string;
  private readonly _specifier: string;

  /**
   * Creates a new BridgeClient.
   *
   * @param options - Bridge client configuration
   * @throws {@link InvalidArgumentError} if required fields are missing
   */
  constructor(options: BridgeClientOptions) {
    super(options);

    if (!options.implementation) {
      throw new InvalidArgumentError("Implementation is required", "implementation");
    }
    if (!options.specifier) {
      throw new InvalidArgumentError("Specifier is required", "specifier");
    }

    this._implementation = options.implementation;
    this._specifier = options.specifier;
  }

  // ===========================================================================
  // Identity Accessors
  // ===========================================================================

  /** Returns the bridge's implementation type. */
  get implementation(): string {
    return this._implementation;
  }

  /** Returns the bridge's specifier (instance identifier). */
  get specifier(): string {
    return this._specifier;
  }

  /**
   * Returns this bridge's topic address.
   *
   * Format: br::{implementation}::{specifier}
   */
  get topic(): string {
    return bridgeTopic(this._implementation, this._specifier);
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      bridge: {
        implementation: this._implementation,
        specifier: this._specifier,
      },
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Message Sending Helpers
  // ===========================================================================

  /**
   * Sends a message to a specific agent in any workspace.
   *
   * @param workspace - Target agent's workspace
   * @param implementation - Target agent's implementation type
   * @param specifier - Target agent's specifier
   * @param payload - Message payload (bytes)
   * @param messageType - Message type. Default: Chat
   */
  sendToAgent(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(agentTopic(workspace, implementation, specifier), payload, messageType);
  }

  /**
   * Sends a message to a specific task in any workspace.
   *
   * For unique tasks (with specifier), uses tu::{workspace}::{impl}::{spec} topic.
   * For non-unique tasks (empty specifier), uses tb::{workspace}::{impl} broadcast topic.
   *
   * @param workspace - Target task's workspace
   * @param implementation - Target task's implementation type
   * @param specifier - Target task's specifier (empty for broadcast)
   * @param payload - Message payload (bytes)
   * @param messageType - Message type. Default: Chat
   */
  sendToTask(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    const topic = specifier
      ? uniqueTaskTopic(workspace, implementation, specifier)
      : taskBroadcastTopic(workspace, implementation);
    this._sendMessage(topic, payload, messageType);
  }

  /**
   * Sends a message to a specific user session.
   *
   * @param userId - Target user's ID
   * @param windowId - Target user's window/session ID
   * @param payload - Message payload (bytes)
   * @param messageType - Message type. Default: Chat
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
   * Sends a message to a user's workspace scope.
   *
   * This reaches the user regardless of which window/tab they are using.
   *
   * @param userId - Target user's ID
   * @param workspace - Target workspace
   * @param payload - Message payload (bytes)
   * @param messageType - Message type. Default: Chat
   */
  sendToUserWorkspace(
    userId: string,
    workspace: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(userWorkspaceTopic(userId, workspace), payload, messageType);
  }

  // ===========================================================================
  // Broadcast Helpers
  // ===========================================================================

  /**
   * Sends a message to all agents in a workspace.
   *
   * @param workspace - Target workspace
   * @param payload - Message payload (bytes)
   * @param messageType - Message type. Default: Chat
   */
  broadcastToAgents(
    workspace: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(globalAgentsTopic(workspace), payload, messageType);
  }

  /**
   * Sends a message to all users in a workspace.
   *
   * @param workspace - Target workspace
   * @param payload - Message payload (bytes)
   * @param messageType - Message type. Default: Chat
   */
  broadcastToUsers(
    workspace: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(globalUsersTopic(workspace), payload, messageType);
  }
}
