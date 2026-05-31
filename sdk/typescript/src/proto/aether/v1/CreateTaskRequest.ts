// Original file: aether.proto

import type { TaskAssignmentMode as _aether_v1_TaskAssignmentMode, TaskAssignmentMode__Output as _aether_v1_TaskAssignmentMode__Output } from '../../aether/v1/TaskAssignmentMode';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { TaskClass as _aether_v1_TaskClass, TaskClass__Output as _aether_v1_TaskClass__Output } from '../../aether/v1/TaskClass';
import type { RetryPolicy as _aether_v1_RetryPolicy, RetryPolicy__Output as _aether_v1_RetryPolicy__Output } from '../../aether/v1/RetryPolicy';
import type { TaskPriority as _aether_v1_TaskPriority, TaskPriority__Output as _aether_v1_TaskPriority__Output } from '../../aether/v1/TaskPriority';

export interface CreateTaskRequest {
  'taskType'?: (string);
  'workspace'?: (string);
  'assignmentMode'?: (_aether_v1_TaskAssignmentMode);
  /**
   * For TARGETED mode only
   */
  'targetAgentId'?: (string);
  /**
   * For targeted tasks that trigger orchestration
   */
  'launchParamOverrides'?: ({[key: string]: string});
  'metadata'?: ({[key: string]: string});
  /**
   * Optional binary payload for task input data (e.g., serialized configs, protobuf work items).
   * Subject to server-enforced size limit (default 512KB).
   */
  'payload'?: (Buffer | Uint8Array | string);
  /**
   * For POOL mode: target agent implementation type (e.g., "data-processor")
   */
  'targetImplementation'?: (string);
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  /**
   * Optional correlation ID. If non-empty, the server will send a
   * CreateTaskResponse with this request_id echoed so the creator can
   * learn the resulting task_id. Leave empty for fire-and-forget semantics.
   */
  'requestId'?: (string);
  /**
   * Optional. Identity string ("<workspace>::<implementation>::<specifier>")
   * that the task's spawned worker will register as. When non-empty AND the
   * server can mint a task token (creator has Manage on the workspace, the
   * identity's workspace matches `workspace`, and the workspace is not on
   * the platform-blocklist), the gateway returns a fresh single-use task
   * token in CreateTaskResponse.task_token. The worker presents that token
   * at connection init to authenticate AS this identity. Leave empty when
   * the worker uses some other auth mechanism (mTLS-only, pre-issued API
   * key, etc.) and no per-task token is wanted.
   */
  'targetIdentity'?: (string);
  /**
   * Optional UI hint; defaults to UNSPECIFIED ⇒ INTERACTIVE.
   */
  'taskClass'?: (_aether_v1_TaskClass);
  /**
   * Client-minted opaque session identifier (A2A contextId, Phase 1).
   * Persisted on Task.context_id and queryable via TaskFilter.context_id.
   * Empty = no session grouping.
   */
  'contextId'?: (string);
  /**
   * Optional. When set, the task store computes next_retry_at on FailTask
   * according to this policy and re-pends the task automatically (up to
   * max_attempts). Absent = legacy behavior (immediate re-pend, hardcoded
   * max_retries=3).
   */
  'retryPolicy'?: (_aether_v1_RetryPolicy | null);
  /**
   * Optional dispatch priority. Defaults to UNSPECIFIED ⇒ NORMAL. Higher
   * priority pending tasks are delivered before lower ones (ties break FIFO).
   */
  'priority'?: (_aether_v1_TaskPriority);
}

export interface CreateTaskRequest__Output {
  'taskType': (string);
  'workspace': (string);
  'assignmentMode': (_aether_v1_TaskAssignmentMode__Output);
  /**
   * For TARGETED mode only
   */
  'targetAgentId': (string);
  /**
   * For targeted tasks that trigger orchestration
   */
  'launchParamOverrides': ({[key: string]: string});
  'metadata': ({[key: string]: string});
  /**
   * Optional binary payload for task input data (e.g., serialized configs, protobuf work items).
   * Subject to server-enforced size limit (default 512KB).
   */
  'payload': (Buffer);
  /**
   * For POOL mode: target agent implementation type (e.g., "data-processor")
   */
  'targetImplementation': (string);
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  /**
   * Optional correlation ID. If non-empty, the server will send a
   * CreateTaskResponse with this request_id echoed so the creator can
   * learn the resulting task_id. Leave empty for fire-and-forget semantics.
   */
  'requestId': (string);
  /**
   * Optional. Identity string ("<workspace>::<implementation>::<specifier>")
   * that the task's spawned worker will register as. When non-empty AND the
   * server can mint a task token (creator has Manage on the workspace, the
   * identity's workspace matches `workspace`, and the workspace is not on
   * the platform-blocklist), the gateway returns a fresh single-use task
   * token in CreateTaskResponse.task_token. The worker presents that token
   * at connection init to authenticate AS this identity. Leave empty when
   * the worker uses some other auth mechanism (mTLS-only, pre-issued API
   * key, etc.) and no per-task token is wanted.
   */
  'targetIdentity': (string);
  /**
   * Optional UI hint; defaults to UNSPECIFIED ⇒ INTERACTIVE.
   */
  'taskClass': (_aether_v1_TaskClass__Output);
  /**
   * Client-minted opaque session identifier (A2A contextId, Phase 1).
   * Persisted on Task.context_id and queryable via TaskFilter.context_id.
   * Empty = no session grouping.
   */
  'contextId': (string);
  /**
   * Optional. When set, the task store computes next_retry_at on FailTask
   * according to this policy and re-pends the task automatically (up to
   * max_attempts). Absent = legacy behavior (immediate re-pend, hardcoded
   * max_retries=3).
   */
  'retryPolicy': (_aether_v1_RetryPolicy__Output | null);
  /**
   * Optional dispatch priority. Defaults to UNSPECIFIED ⇒ NORMAL. Higher
   * priority pending tasks are delivered before lower ones (ties break FIFO).
   */
  'priority': (_aether_v1_TaskPriority__Output);
}
