/**
 * Agent client implementation for the Aether TypeScript SDK.
 *
 * This module provides the AgentClient class for connecting to the Aether
 * gateway as an agent. Agents are persistent entities with
 * workspace/implementation/specifier identity that can send and receive
 * messages, manage state, and participate in task orchestration.
 *
 * @module agents
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
  globalAgentsTopic,
  globalUsersTopic,
  eventWildcardTopic,
  metricWildcardTopic,
} from "./topics.js";
import type { Metric } from "./metrics-builder.js";

// =============================================================================
// Agent Client Options
// =============================================================================

/**
 * Configuration options for the AgentClient.
 */
export interface AgentClientOptions extends AetherClientOptions {
  /** The workspace to connect to. Required. */
  workspace: string;
  /** The agent implementation type. Required. */
  implementation: string;
  /** The unique specifier for this agent instance. Required. */
  specifier: string;
}

// =============================================================================
// Task Creation Options
// =============================================================================

/**
 * Options for creating a task.
 */
export interface CreateTaskOptions {
  /** The type of task to create. Required. */
  taskType: string;
  /** The workspace for the task. If empty, uses the agent's workspace. */
  workspace?: string;
  /** For TARGETED mode: the agent to assign to. */
  targetAgentId?: string;
  /** For POOL mode: the agent implementation type to match. */
  targetImplementation?: string;
  /** Optional parameter overrides for orchestration. */
  launchParamOverrides?: Record<string, string>;
  /** Optional task metadata. */
  metadata?: Record<string, string>;
  /** Assignment mode. Default: SelfAssign. */
  assignmentMode?: TaskAssignmentMode;
}

// =============================================================================
// AgentClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as an agent.
 *
 * Agents are persistent entities identified by workspace, implementation,
 * and specifier. Each agent identity can only have one active connection
 * at a time (Connection = Lock paradigm).
 *
 * AgentClient extends AetherClient and adds agent-specific functionality:
 * - Identity management (workspace, implementation, specifier)
 * - Messaging helpers (sendToAgent, sendToUser, sendToTask, broadcast)
 * - Event and metric publishing
 * - Workspace switching
 * - Task creation
 *
 * @example
 * ```typescript
 * import { AgentClient } from "@scitrera/aether-client";
 *
 * const agent = new AgentClient({
 *   address: "localhost:50051",
 *   workspace: "prod",
 *   implementation: "data-processor",
 *   specifier: "instance-1",
 * });
 *
 * agent.onMessage((msg) => {
 *   console.log(`Received from ${msg.sourceTopic}:`, msg.payload);
 * });
 *
 * await agent.connect();
 * ```
 */
export class AgentClient extends AetherClient {
  private _workspace: string;
  private readonly _implementation: string;
  private readonly _specifier: string;

  /**
   * Creates a new AgentClient.
   *
   * @param options - Agent client configuration
   * @throws {@link InvalidArgumentError} if required fields are missing
   */
  constructor(options: AgentClientOptions) {
    super(options);

    if (!options.workspace) {
      throw new InvalidArgumentError("Workspace is required", "workspace");
    }
    if (!options.implementation) {
      throw new InvalidArgumentError("Implementation is required", "implementation");
    }
    if (!options.specifier) {
      throw new InvalidArgumentError("Specifier is required", "specifier");
    }

    this._workspace = options.workspace;
    this._implementation = options.implementation;
    this._specifier = options.specifier;
  }

  // ===========================================================================
  // Identity Accessors
  // ===========================================================================

  /** Returns the agent's current workspace. */
  get workspace(): string {
    return this._workspace;
  }

  /** Returns the agent's implementation type. */
  get implementation(): string {
    return this._implementation;
  }

  /** Returns the agent's specifier (instance identifier). */
  get specifier(): string {
    return this._specifier;
  }

  /**
   * Returns this agent's topic address.
   *
   * Format: ag.{workspace}.{implementation}.{specifier}
   */
  get topic(): string {
    return agentTopic(this._workspace, this._implementation, this._specifier);
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      agent: {
        workspace: this._workspace,
        implementation: this._implementation,
        specifier: this._specifier,
      },
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Workspace Management
  // ===========================================================================

  /**
   * Switches the agent's workspace.
   *
   * Sends a SwitchWorkspace message to the gateway, which will update
   * the agent's topic subscription and optionally send a new ConfigSnapshot.
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
   * For unique tasks (with specifier), uses tu.{workspace}.{impl}.{spec} topic.
   * For non-unique tasks (empty specifier), uses tb.{workspace}.{impl} broadcast topic.
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
    const topic = userWorkspaceTopic(userId, workspace);
    this._sendMessage(topic, payload, messageType);
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
    const topic = globalAgentsTopic(workspace);
    this._sendMessage(topic, payload, messageType);
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
    const topic = globalUsersTopic(workspace);
    this._sendMessage(topic, payload, messageType);
  }

  // ===========================================================================
  // Event and Metric Publishing
  // ===========================================================================

  /**
   * Publishes an event to the workflow engine.
   *
   * @param payload - Event payload (bytes)
   */
  sendEvent(payload: Uint8Array): void {
    this._sendMessage(eventWildcardTopic(), payload, MessageType.Event);
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

  // ===========================================================================
  // Task Creation
  // ===========================================================================

  /**
   * Creates a new task with the specified parameters.
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
   * Registers a handler specifically for task-related messages.
   *
   * Filters incoming messages to only those from task topics (tu.* or ta.*).
   *
   * @param handler - Function called when a task message is received
   */
  onTaskMessage(handler: MessageHandler): void {
    const existingHandler = this["_onMessage" as keyof this] as MessageHandler;
    this.onMessage((msg) => {
      if (msg.sourceTopic.startsWith("tu.") || msg.sourceTopic.startsWith("ta.") || msg.sourceTopic.startsWith("tb.")) {
        handler(msg);
      } else if (existingHandler) {
        existingHandler(msg);
      }
    });
  }

  /**
   * Registers a handler specifically for control messages.
   *
   * Note: Since the base protocol delivers all messages through a single
   * stream, this filters based on source topic prefixes. For more granular
   * filtering, use onMessage directly and inspect the payload.
   *
   * @param handler - Function called when a control message is received
   */
  onControlMessage(handler: MessageHandler): void {
    const existingHandler = this["_onMessage" as keyof this] as MessageHandler;
    this.onMessage((msg) => {
      // Control messages can come from any source; delegate to user handler
      // for inspection. This provides a simple hook point.
      handler(msg);
      if (existingHandler) {
        existingHandler(msg);
      }
    });
  }
}
