// Original file: aether.proto

import type { TaskClass as _aether_v1_TaskClass, TaskClass__Output as _aether_v1_TaskClass__Output } from '../../aether/v1/TaskClass';
import type { Long } from '@grpc/proto-loader';

export interface TaskAssignment {
  'taskId'?: (string);
  'taskType'?: (string);
  /**
   * Agent identity receiving the task
   */
  'assignedTo'?: (string);
  'metadata'?: ({[key: string]: string});
  /**
   * Unix timestamp
   */
  'assignedAt'?: (number | string | Long);
  /**
   * For orchestrated tasks (agent startup)
   */
  'profile'?: (string);
  /**
   * Launch parameters for the agent
   */
  'launchParams'?: ({[key: string]: string});
  /**
   * Target agent implementation to start
   */
  'targetImplementation'?: (string);
  /**
   * Workspace for the new agent
   */
  'workspace'?: (string);
  /**
   * Agent specifier (instance identifier)
   */
  'specifier'?: (string);
  /**
   * Optional binary payload carried from CreateTaskRequest.
   */
  'payload'?: (Buffer | Uint8Array | string);
  /**
   * Optional UI hint; defaults to UNSPECIFIED ⇒ INTERACTIVE.
   */
  'taskClass'?: (_aether_v1_TaskClass);
}

export interface TaskAssignment__Output {
  'taskId': (string);
  'taskType': (string);
  /**
   * Agent identity receiving the task
   */
  'assignedTo': (string);
  'metadata': ({[key: string]: string});
  /**
   * Unix timestamp
   */
  'assignedAt': (string);
  /**
   * For orchestrated tasks (agent startup)
   */
  'profile': (string);
  /**
   * Launch parameters for the agent
   */
  'launchParams': ({[key: string]: string});
  /**
   * Target agent implementation to start
   */
  'targetImplementation': (string);
  /**
   * Workspace for the new agent
   */
  'workspace': (string);
  /**
   * Agent specifier (instance identifier)
   */
  'specifier': (string);
  /**
   * Optional binary payload carried from CreateTaskRequest.
   */
  'payload': (Buffer);
  /**
   * Optional UI hint; defaults to UNSPECIFIED ⇒ INTERACTIVE.
   */
  'taskClass': (_aether_v1_TaskClass__Output);
}
