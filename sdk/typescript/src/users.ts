/**
 * User client implementation for the Aether TypeScript SDK.
 *
 * This module provides the UserClient class for connecting to the Aether
 * gateway as a user. Users are identified by userId and windowId, allowing
 * multiple browser tabs or sessions per user.
 *
 * Per the Aether specification, users can only send direct messages.
 * They cannot publish events or metrics like agents and tasks can.
 *
 * @module users
 */

import { AetherClient } from "./client.js";
import type { AetherClientOptions } from "./client.js";
import { MessageType, TaskAssignmentMode } from "./types.js";
import type { MessageHandler } from "./types.js";
import { InvalidArgumentError } from "./errors.js";
import {
  agentTopic,
  uniqueTaskTopic,
  taskBroadcastTopic,
  userTopic,
  userWorkspaceTopic,
} from "./topics.js";
import type { CreateTaskOptions } from "./agents.js";

// =============================================================================
// User Client Options
// =============================================================================

/**
 * Configuration options for the UserClient.
 */
export interface UserClientOptions extends AetherClientOptions {
  /** The user's unique identifier. Required. */
  userId: string;
  /** The window/session identifier. Required. */
  windowId: string;
  /** Optional initial workspace association. */
  workspace?: string;
}

// =============================================================================
// UserClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as a user.
 *
 * Users are identified by userId and windowId, allowing multiple browser
 * tabs or sessions per user. Each user session (userId + windowId combination)
 * is unique and can only have one active connection at a time.
 *
 * UserClient extends AetherClient and adds user-specific functionality:
 * - Identity management (userId, windowId)
 * - Messaging helpers (sendToAgent, sendToUser, sendToTask)
 * - Optional workspace association
 *
 * Note: Per the Aether specification, users can only send direct messages.
 * They cannot publish events or metrics.
 *
 * @example
 * ```typescript
 * import { UserClient } from "@scitrera/aether-client";
 *
 * const user = new UserClient({
 *   address: "localhost:50051",
 *   userId: "alice",
 *   windowId: "tab-1",
 * });
 *
 * user.onIncomingMessage((msg) => {
 *   console.log(`Received from ${msg.sourceTopic}:`, msg.payload);
 * });
 *
 * await user.connect();
 *
 * // Send a message to an agent
 * const encoder = new TextEncoder();
 * user.sendToAgent("prod", "data-processor", "instance-1",
 *   encoder.encode(JSON.stringify({ action: "process" }))
 * );
 * ```
 */
export class UserClient extends AetherClient {
  private readonly _userId: string;
  private readonly _windowId: string;
  private _workspace: string;

  /**
   * Creates a new UserClient.
   *
   * @param options - User client configuration
   * @throws {@link InvalidArgumentError} if required fields are missing
   */
  constructor(options: UserClientOptions) {
    super(options);

    if (!options.userId) {
      throw new InvalidArgumentError("User ID is required", "userId");
    }
    if (!options.windowId) {
      throw new InvalidArgumentError("Window ID is required", "windowId");
    }

    this._userId = options.userId;
    this._windowId = options.windowId;
    this._workspace = options.workspace ?? "";
  }

  // ===========================================================================
  // Identity Accessors
  // ===========================================================================

  /** Returns the user's unique identifier. */
  get userId(): string {
    return this._userId;
  }

  /** Returns the user's window/session identifier. */
  get windowId(): string {
    return this._windowId;
  }

  /** Returns the user's current workspace (if set). */
  get workspace(): string {
    return this._workspace;
  }

  /**
   * Returns this user's topic address.
   *
   * Format: us::{userId}::{windowId}
   */
  get topic(): string {
    return userTopic(this._userId, this._windowId);
  }

  /**
   * Returns this user's workspace-scoped topic address.
   *
   * Format: uw::{userId}::{workspace}
   *
   * Returns empty string if no workspace is set.
   */
  get workspaceTopic(): string {
    if (!this._workspace) return "";
    return userWorkspaceTopic(this._userId, this._workspace);
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      user: {
        userId: this._userId,
        windowId: this._windowId,
      },
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Workspace Management
  // ===========================================================================

  /**
   * Sets the user's current workspace for workspace-scoped operations.
   *
   * This is a local operation that does not notify the gateway.
   *
   * @param workspace - The workspace to associate with
   */
  setWorkspace(workspace: string): void {
    this._workspace = workspace;
  }

  /**
   * Switches the user's workspace on the gateway.
   *
   * Sends a SwitchWorkspace message to the gateway, which will update
   * topic subscriptions (including progress) and send a new ConfigSnapshot.
   *
   * @param newWorkspace - The new workspace to switch to
   * @throws {@link InvalidArgumentError} if the workspace is empty
   */
  switchWorkspace(newWorkspace: string): void {
    if (!newWorkspace) {
      throw new InvalidArgumentError("Workspace cannot be empty", "newWorkspace");
    }
    this._sendUpstream({
      switchWorkspace: {
        newWorkspaceId: newWorkspace,
      },
    });
    this._workspace = newWorkspace;
  }

  // ===========================================================================
  // Message Sending Helpers
  // ===========================================================================

  /**
   * Sends a message to a specific agent.
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
    const topic = agentTopic(workspace, implementation, specifier);
    this._sendMessage(topic, payload, messageType);
  }

  /**
   * Sends a message to a specific task.
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
    const topic = userTopic(userId, windowId);
    this._sendMessage(topic, payload, messageType);
  }

  /**
   * Sends a message to a user's workspace scope.
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
    const topic = userWorkspaceTopic(userId, workspace);
    this._sendMessage(topic, payload, messageType);
  }

  // ===========================================================================
  // Task Creation
  // ===========================================================================

  /**
   * Creates a new task with the specified parameters.
   *
   * Users can create tasks targeting agents or task pools.
   *
   * @param opts - Task creation options
   * @throws {@link InvalidArgumentError} if task type is missing
   */
  createTask(opts: CreateTaskOptions): void {
    if (!opts.taskType) {
      throw new InvalidArgumentError("Task type is required", "taskType");
    }

    const workspace = opts.workspace || this._workspace;
    let assignmentMode = opts.assignmentMode ?? TaskAssignmentMode.SelfAssign;

    // Auto-set TARGETED mode if target is specified
    if (opts.targetAgentId && assignmentMode === TaskAssignmentMode.SelfAssign) {
      assignmentMode = TaskAssignmentMode.Targeted;
    }

    // Auto-set POOL mode if target implementation is specified
    if (opts.targetImplementation && assignmentMode === TaskAssignmentMode.SelfAssign) {
      assignmentMode = TaskAssignmentMode.Pool;
    }

    this._sendUpstream({
      createTask: {
        taskType: opts.taskType,
        workspace,
        assignmentMode,
        targetAgentId: opts.targetAgentId ?? "",
        targetImplementation: opts.targetImplementation ?? "",
        launchParamOverrides: opts.launchParamOverrides ?? {},
        metadata: opts.metadata ?? {},
      },
    });
  }

  // ===========================================================================
  // Convenience Handler Registration
  // ===========================================================================

  /**
   * Registers a handler for all incoming messages.
   *
   * This is an alias for onMessage, provided for API clarity in the
   * user context.
   *
   * @param handler - Function called when a message is received
   */
  onIncomingMessage(handler: MessageHandler): void {
    this.onMessage(handler);
  }
}
