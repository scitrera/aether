/**
 * Orchestrator client implementation for the Aether TypeScript SDK.
 *
 * Orchestrators manage agent/task lifecycle: receiving startup requests
 * when targeted agents are offline, launching compute resources, and
 * managing agent pools and scaling.
 *
 * @module orchestrator
 */

import { AetherClient } from "./client.js";
import type { AetherClientOptions } from "./client.js";
import { MessageType } from "./types.js";
import type { TaskAssignment, ConnectionAck } from "./types.js";
import { InvalidArgumentError } from "./errors.js";
import {
  agentTopic,
  uniqueTaskTopic,
  taskBroadcastTopic,
} from "./topics.js";

// =============================================================================
// Orchestrator Client Options
// =============================================================================

/**
 * Configuration options for the OrchestratorClient.
 */
export interface OrchestratorClientOptions extends AetherClientOptions {
  /** The orchestrator implementation type (e.g., "kubernetes-orchestrator"). Required. */
  implementation: string;
  /** Profiles this orchestrator supports (e.g., ["kubernetes", "docker"]). Required, at least one. */
  supportedProfiles: string[];
  /** Unique specifier for this instance. Auto-generated if not provided. */
  specifier?: string;
}

// =============================================================================
// OrchestratorClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as an orchestrator.
 *
 * Orchestrators receive task assignments via the onTaskAssignment handler
 * and are responsible for launching the appropriate compute resources.
 *
 * @example
 * ```typescript
 * const orchestrator = new OrchestratorClient({
 *   address: "localhost:50051",
 *   implementation: "k8s-orchestrator",
 *   supportedProfiles: ["kubernetes"],
 * });
 *
 * orchestrator.onTaskAssignment((assignment) => {
 *   console.log(`Starting ${assignment.targetImplementation} for task ${assignment.taskId}`);
 *   // Launch container based on assignment.launchParams
 * });
 *
 * await orchestrator.connect();
 * ```
 */
export class OrchestratorClient extends AetherClient {
  private readonly _implementation: string;
  private readonly _specifier: string;
  private readonly _supportedProfiles: string[];

  constructor(options: OrchestratorClientOptions) {
    super(options);

    if (!options.implementation) {
      throw new InvalidArgumentError("Implementation is required", "implementation");
    }
    if (!options.supportedProfiles || options.supportedProfiles.length === 0) {
      throw new InvalidArgumentError("At least one supported profile is required", "supportedProfiles");
    }

    this._implementation = options.implementation;
    this._specifier = options.specifier ?? crypto.randomUUID().slice(0, 8);
    this._supportedProfiles = options.supportedProfiles;
  }

  // ===========================================================================
  // Identity Accessors
  // ===========================================================================

  get implementation(): string {
    return this._implementation;
  }

  get specifier(): string {
    return this._specifier;
  }

  get supportedProfiles(): string[] {
    return [...this._supportedProfiles];
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      orchestrator: {
        implementation: this._implementation,
        specifier: this._specifier,
        supportedProfiles: this._supportedProfiles,
      },
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Message Sending Helpers
  // ===========================================================================

  /**
   * Sends a status/control message to an agent.
   */
  sendStatusToAgent(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Control,
  ): void {
    this._sendMessage(agentTopic(workspace, implementation, specifier), payload, messageType);
  }

  /**
   * Sends a status/control message to a task.
   * For unique tasks (with specifier), uses tu.* topic.
   * For non-unique tasks (empty specifier), uses tb.* broadcast topic.
   */
  sendStatusToTask(
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
}

// =============================================================================
// BaseOrchestrator Options
// =============================================================================

/**
 * Configuration options for BaseOrchestrator subclasses.
 */
export interface BaseOrchestratorOptions extends OrchestratorClientOptions {
  /**
   * Whether to log task assignment details to the console.
   * Default: false.
   */
  logAssignments?: boolean;
}

// =============================================================================
// BaseOrchestrator
// =============================================================================

/**
 * Abstract base class for building orchestrators.
 *
 * BaseOrchestrator wraps {@link OrchestratorClient} and provides a structured
 * framework for managing agent/task lifecycle. Subclasses implement the
 * abstract {@link launchTask} method to handle task assignments with their
 * specific compute backend (Docker, Kubernetes, subprocess, etc.).
 *
 * The class manages the full orchestrator lifecycle:
 * - Connecting to the Aether gateway as an Orchestrator principal
 * - Receiving {@link TaskAssignment} messages and dispatching to launchTask
 * - Error handling and logging hooks
 *
 * @example
 * ```typescript
 * import { BaseOrchestrator, TaskAssignment } from "@scitrera/aether-client";
 *
 * class MyOrchestrator extends BaseOrchestrator {
 *   async launchTask(assignment: TaskAssignment): Promise<void> {
 *     console.log(`Launching task ${assignment.taskId} (profile: ${assignment.profile})`);
 *     // Start a subprocess, container, etc. using assignment.launchParams
 *   }
 * }
 *
 * const orch = new MyOrchestrator({
 *   address: "localhost:50051",
 *   implementation: "my-orchestrator",
 *   supportedProfiles: ["my-profile"],
 * });
 *
 * orch.onConnect((ack) => {
 *   console.log(`Connected with session ${ack.sessionId}`);
 * });
 *
 * await orch.connect();
 * ```
 */
export abstract class BaseOrchestrator {
  /** The underlying OrchestratorClient for direct gateway access. */
  readonly client: OrchestratorClient;

  private readonly _logAssignments: boolean;

  /**
   * Creates a new BaseOrchestrator.
   *
   * @param options - Orchestrator configuration options
   */
  constructor(options: BaseOrchestratorOptions) {
    this._logAssignments = options.logAssignments ?? false;
    this.client = new OrchestratorClient(options);

    // Wire task assignment handler to launchTask
    this.client.onTaskAssignment((assignment) => {
      return this._handleAssignment(assignment);
    });
  }

  // ===========================================================================
  // Abstract Interface
  // ===========================================================================

  /**
   * Called when the gateway assigns a task to this orchestrator.
   *
   * Subclasses must implement this method to launch the appropriate compute
   * resource (container, process, VM, etc.) based on the assignment details.
   *
   * The assignment includes:
   * - `taskId`: Unique task identifier
   * - `profile`: The profile that matched (from supportedProfiles)
   * - `targetImplementation`: The agent/task implementation to launch
   * - `workspace`: The workspace this task belongs to
   * - `specifier`: Optional unique specifier for the agent/task
   * - `launchParams`: Key-value launch parameters (image, command, env vars, etc.)
   * - `metadata`: Arbitrary task metadata
   *
   * @param assignment - The task assignment received from the gateway
   */
  abstract launchTask(assignment: TaskAssignment): void | Promise<void>;

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Connects to the Aether gateway.
   *
   * Must be called before the orchestrator can receive task assignments.
   */
  async connect(): Promise<void> {
    await this.client.connect();
  }

  /**
   * Disconnects from the Aether gateway.
   */
  async disconnect(): Promise<void> {
    await this.client.disconnect();
  }

  /**
   * Returns whether the orchestrator is currently connected.
   */
  get connected(): boolean {
    return this.client.connected;
  }

  /**
   * Returns the current session ID assigned by the gateway.
   * Empty string if not connected.
   */
  get sessionId(): string {
    return this.client.sessionId;
  }

  /**
   * Returns the orchestrator's implementation type.
   */
  get implementation(): string {
    return this.client.implementation;
  }

  /**
   * Returns the orchestrator's specifier.
   */
  get specifier(): string {
    return this.client.specifier;
  }

  /**
   * Returns the list of profiles this orchestrator supports.
   */
  get supportedProfiles(): string[] {
    return this.client.supportedProfiles;
  }

  // ===========================================================================
  // Handler Registration
  // ===========================================================================

  /**
   * Registers a handler for successful connection events.
   *
   * @param handler - Called with the ConnectionAck when connected
   */
  onConnect(handler: (ack: ConnectionAck) => void | Promise<void>): void {
    this.client.onConnect(handler);
  }

  /**
   * Registers a handler for disconnection events.
   *
   * @param handler - Called with the reason string when disconnected
   */
  onDisconnect(handler: (reason: string) => void | Promise<void>): void {
    this.client.onDisconnect(handler);
  }

  /**
   * Registers a handler for reconnection attempts.
   *
   * @param handler - Called with the attempt number on each reconnect try
   */
  onReconnecting(handler: (attempt: number) => void | Promise<void>): void {
    this.client.onReconnecting(handler);
  }

  // ===========================================================================
  // Message Sending
  // ===========================================================================

  /**
   * Sends a status/control message to an agent.
   *
   * @param workspace - Target agent's workspace
   * @param implementation - Target agent's implementation type
   * @param specifier - Target agent's specifier
   * @param payload - Message payload
   * @param messageType - Message type. Default: Control
   */
  sendStatusToAgent(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Control,
  ): void {
    this.client.sendStatusToAgent(workspace, implementation, specifier, payload, messageType);
  }

  /**
   * Sends a status/control message to a task.
   *
   * @param workspace - Target task's workspace
   * @param implementation - Target task's implementation type
   * @param specifier - Target task's specifier (empty for broadcast)
   * @param payload - Message payload
   * @param messageType - Message type. Default: Control
   */
  sendStatusToTask(
    workspace: string,
    implementation: string,
    specifier: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Control,
  ): void {
    this.client.sendStatusToTask(workspace, implementation, specifier, payload, messageType);
  }

  // ===========================================================================
  // Internal
  // ===========================================================================

  /**
   * Internal handler that logs and dispatches to launchTask.
   * @internal
   */
  private async _handleAssignment(assignment: TaskAssignment): Promise<void> {
    if (this._logAssignments) {
      console.log(
        `[BaseOrchestrator] Task assignment received: taskId=${assignment.taskId}` +
          ` profile=${assignment.profile} impl=${assignment.targetImplementation}` +
          ` workspace=${assignment.workspace}`,
      );
    }
    await this.launchTask(assignment);
  }
}
