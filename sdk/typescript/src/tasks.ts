/**
 * Task client implementation for the Aether TypeScript SDK.
 *
 * This module provides the TaskClient class for connecting to the Aether
 * gateway as a task. Tasks can be unique (named, with a specifier) or
 * non-unique (server-assigned ID, load-balanced via broadcast topic).
 *
 * @module tasks
 */

import { AetherClient } from "./client.js";
import type { AetherClientOptions } from "./client.js";
import { MessageType, TaskAssignmentMode } from "./types.js";
import { InvalidArgumentError } from "./errors.js";
import {
  agentTopic,
  uniqueTaskTopic,
  taskBroadcastTopic,
  userTopic,
  eventWildcardTopic,
  metricWildcardTopic,
} from "./topics.js";
import type { CreateTaskOptions } from "./agents.js";
import type { Metric } from "./metrics-builder.js";

// =============================================================================
// Task Client Options
// =============================================================================

/**
 * Configuration options for the TaskClient.
 */
export interface TaskClientOptions extends AetherClientOptions {
  /** The workspace to connect to. Required. */
  workspace: string;
  /** The task implementation type. Required. */
  implementation: string;
  /** Unique specifier for this task. Empty for non-unique tasks. */
  uniqueSpecifier?: string;
}

// =============================================================================
// TaskClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as a task.
 *
 * Tasks come in two flavors:
 * - **Unique tasks** (with specifier): Persistent identity like agents,
 *   only one connection at a time. Topic: tu::{workspace}::{impl}::{spec}
 * - **Non-unique tasks** (empty specifier): Server-assigned ID, multiple
 *   instances allowed. Subscribe to both ta::{workspace}::{impl}::{id} and
 *   the shared tb::{workspace}::{impl} broadcast for work claiming.
 *
 * @example
 * ```typescript
 * // Unique task
 * const task = new TaskClient({
 *   address: "localhost:50051",
 *   workspace: "prod",
 *   implementation: "report-gen",
 *   uniqueSpecifier: "daily-report",
 * });
 *
 * // Non-unique task (worker pool)
 * const worker = new TaskClient({
 *   address: "localhost:50051",
 *   workspace: "prod",
 *   implementation: "data-processor",
 * });
 * ```
 */
export class TaskClient extends AetherClient {
  private _workspace: string;
  private readonly _implementation: string;
  private readonly _uniqueSpecifier: string;

  constructor(options: TaskClientOptions) {
    super(options);

    if (!options.workspace) {
      throw new InvalidArgumentError("Workspace is required", "workspace");
    }
    if (!options.implementation) {
      throw new InvalidArgumentError("Implementation is required", "implementation");
    }

    this._workspace = options.workspace;
    this._implementation = options.implementation;
    this._uniqueSpecifier = options.uniqueSpecifier ?? "";
  }

  // ===========================================================================
  // Identity Accessors
  // ===========================================================================

  get workspace(): string {
    return this._workspace;
  }

  get implementation(): string {
    return this._implementation;
  }

  get uniqueSpecifier(): string {
    return this._uniqueSpecifier;
  }

  /** Whether this is a unique (named) task. */
  get isUnique(): boolean {
    return this._uniqueSpecifier !== "";
  }

  /**
   * Returns this task's topic address.
   * For unique tasks: tu::{workspace}::{impl}::{spec}
   * For non-unique tasks: returns the broadcast topic tb::{workspace}::{impl}
   * (the actual instance topic is assigned by the server).
   */
  get topic(): string {
    if (this._uniqueSpecifier) {
      return uniqueTaskTopic(this._workspace, this._implementation, this._uniqueSpecifier);
    }
    return taskBroadcastTopic(this._workspace, this._implementation);
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      task: {
        workspace: this._workspace,
        implementation: this._implementation,
        uniqueSpecifier: this._uniqueSpecifier,
      },
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Workspace Management
  // ===========================================================================

  /**
   * Switches the task's workspace.
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
   * Sends a message to a specific task.
   * For unique tasks (with specifier), uses tu.* topic.
   * For non-unique tasks (empty specifier), uses tb.* broadcast topic.
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
   */
  sendToUser(
    userId: string,
    windowId: string,
    payload: Uint8Array,
    messageType: MessageType = MessageType.Opaque,
  ): void {
    this._sendMessage(userTopic(userId, windowId), payload, messageType);
  }

  // ===========================================================================
  // Event and Metric Publishing
  // ===========================================================================

  /**
   * Publishes an event to the workflow engine.
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
}
