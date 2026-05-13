// Original file: aether.proto

import type { TaskStatus as _aether_v1_TaskStatus, TaskStatus__Output as _aether_v1_TaskStatus__Output } from '../../aether/v1/TaskStatus';
import type { TaskClass as _aether_v1_TaskClass, TaskClass__Output as _aether_v1_TaskClass__Output } from '../../aether/v1/TaskClass';
import type { Long } from '@grpc/proto-loader';

/**
 * TaskInfo represents a task.
 * Matches the TaskInfo struct in state_provider.go.
 */
export interface TaskInfo {
  /**
   * Unique task identifier
   */
  'taskId'?: (string);
  /**
   * Task type (e.g., "data-processing", "report-generation")
   */
  'taskType'?: (string);
  /**
   * Task status
   */
  'status'?: (_aether_v1_TaskStatus);
  /**
   * Workspace the task belongs to
   */
  'workspace'?: (string);
  /**
   * Target topic for task messages
   */
  'targetTopic'?: (string);
  /**
   * Agent identity assigned to handle this task
   */
  'assignedTo'?: (string);
  /**
   * Unix timestamp when task was created
   */
  'createdAt'?: (number | string | Long);
  /**
   * Unix timestamp when task execution started (0 if not started)
   */
  'startedAt'?: (number | string | Long);
  /**
   * Unix timestamp when task completed (0 if not completed)
   */
  'completedAt'?: (number | string | Long);
  /**
   * Current attempt number
   */
  'attempt'?: (number);
  /**
   * Maximum retry attempts allowed
   */
  'maxAttempts'?: (number);
  /**
   * Error message if task failed
   */
  'error'?: (string);
  /**
   * Task metadata
   */
  'metadata'?: ({[key: string]: string});
  /**
   * First-class authority lineage (populated when the task was created under an
   * on-behalf-of authority grant or inherited from a parent task grant).
   */
  'authorityMode'?: (string);
  /**
   * Subject principal type
   */
  'subjectType'?: (string);
  /**
   * Subject canonical identity
   */
  'subjectId'?: (string);
  /**
   * Root subject principal type
   */
  'rootSubjectType'?: (string);
  /**
   * Root subject canonical identity
   */
  'rootSubjectId'?: (string);
  /**
   * Current authority grant id
   */
  'authorityGrantId'?: (string);
  /**
   * Root authority grant id
   */
  'rootAuthorityGrantId'?: (string);
  /**
   * Parent authority grant id
   */
  'parentAuthorityGrantId'?: (string);
  /**
   * Identity of the actor that created this task
   */
  'creatorActorId'?: (string);
  /**
   * Parent task id when created inside another task's execution
   */
  'parentTaskId'?: (string);
  /**
   * Optional UI hint; defaults to UNSPECIFIED ⇒ INTERACTIVE.
   */
  'taskClass'?: (_aether_v1_TaskClass);
  /**
   * Disconnect grace window: connection-as-heartbeat model. When the
   * assigned worker's gRPC stream closes (other than worker EOF, which is
   * explicit "I'm done"), disconnected_at is stamped. The disconnect reaper
   * fails the task when (now - disconnected_at) > grace_window_ms.
   * disconnected_at is unix seconds; 0 means worker is currently connected.
   */
  'disconnectedAt'?: (number | string | Long);
  'graceWindowMs'?: (number | string | Long);
}

/**
 * TaskInfo represents a task.
 * Matches the TaskInfo struct in state_provider.go.
 */
export interface TaskInfo__Output {
  /**
   * Unique task identifier
   */
  'taskId': (string);
  /**
   * Task type (e.g., "data-processing", "report-generation")
   */
  'taskType': (string);
  /**
   * Task status
   */
  'status': (_aether_v1_TaskStatus__Output);
  /**
   * Workspace the task belongs to
   */
  'workspace': (string);
  /**
   * Target topic for task messages
   */
  'targetTopic': (string);
  /**
   * Agent identity assigned to handle this task
   */
  'assignedTo': (string);
  /**
   * Unix timestamp when task was created
   */
  'createdAt': (string);
  /**
   * Unix timestamp when task execution started (0 if not started)
   */
  'startedAt': (string);
  /**
   * Unix timestamp when task completed (0 if not completed)
   */
  'completedAt': (string);
  /**
   * Current attempt number
   */
  'attempt': (number);
  /**
   * Maximum retry attempts allowed
   */
  'maxAttempts': (number);
  /**
   * Error message if task failed
   */
  'error': (string);
  /**
   * Task metadata
   */
  'metadata': ({[key: string]: string});
  /**
   * First-class authority lineage (populated when the task was created under an
   * on-behalf-of authority grant or inherited from a parent task grant).
   */
  'authorityMode': (string);
  /**
   * Subject principal type
   */
  'subjectType': (string);
  /**
   * Subject canonical identity
   */
  'subjectId': (string);
  /**
   * Root subject principal type
   */
  'rootSubjectType': (string);
  /**
   * Root subject canonical identity
   */
  'rootSubjectId': (string);
  /**
   * Current authority grant id
   */
  'authorityGrantId': (string);
  /**
   * Root authority grant id
   */
  'rootAuthorityGrantId': (string);
  /**
   * Parent authority grant id
   */
  'parentAuthorityGrantId': (string);
  /**
   * Identity of the actor that created this task
   */
  'creatorActorId': (string);
  /**
   * Parent task id when created inside another task's execution
   */
  'parentTaskId': (string);
  /**
   * Optional UI hint; defaults to UNSPECIFIED ⇒ INTERACTIVE.
   */
  'taskClass': (_aether_v1_TaskClass__Output);
  /**
   * Disconnect grace window: connection-as-heartbeat model. When the
   * assigned worker's gRPC stream closes (other than worker EOF, which is
   * explicit "I'm done"), disconnected_at is stamped. The disconnect reaper
   * fails the task when (now - disconnected_at) > grace_window_ms.
   * disconnected_at is unix seconds; 0 means worker is currently connected.
   */
  'disconnectedAt': (string);
  'graceWindowMs': (string);
}
