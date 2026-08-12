// Original file: aether.proto

import type { TaskAssignmentMode as _aether_v1_TaskAssignmentMode, TaskAssignmentMode__Output as _aether_v1_TaskAssignmentMode__Output } from '../../aether/v1/TaskAssignmentMode';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { TaskClass as _aether_v1_TaskClass, TaskClass__Output as _aether_v1_TaskClass__Output } from '../../aether/v1/TaskClass';
import type { RetryPolicy as _aether_v1_RetryPolicy, RetryPolicy__Output as _aether_v1_RetryPolicy__Output } from '../../aether/v1/RetryPolicy';
import type { TaskPriority as _aether_v1_TaskPriority, TaskPriority__Output as _aether_v1_TaskPriority__Output } from '../../aether/v1/TaskPriority';
import type { TaskCompletionEvent as _aether_v1_TaskCompletionEvent, TaskCompletionEvent__Output as _aether_v1_TaskCompletionEvent__Output } from '../../aether/v1/TaskCompletionEvent';
import type { TargetOfflinePolicy as _aether_v1_TargetOfflinePolicy, TargetOfflinePolicy__Output as _aether_v1_TargetOfflinePolicy__Output } from '../../aether/v1/TargetOfflinePolicy';

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
  /**
   * Optional idempotency key for exactly-once task creation. When non-empty the
   * gateway dedupes creation on this key: a duplicate request returns the
   * existing task identity instead of creating a second task. Workflow joins
   * set it to make an on_complete create_task fire exactly once under retries
   * or engine restart.
   */
  'idempotencyKey'?: (string);
  /**
   * Optional fan-out/fan-in correlation identity, distinct from task_id. When
   * non-empty it is persisted on the task and propagated to child spawns, and is
   * queryable via TaskFilter.correlation_id. The barrier/group id a join matches.
   */
  'correlationId'?: (string);
  /**
   * Optional top-of-fan-out-tree identity (the flow / run id). Defaults to the
   * task's own id when it is a root; inherited by descendants on spawn. Becomes
   * the DAG run id when the join engine grows into full fan-out/fan-in.
   */
  'rootTaskId'?: (string);
  /**
   * Optional "feed B" config: emit a domain event onto event::* when this task
   * reaches a (selected) terminal status. Absent/disabled = no emission.
   */
  'completionEvent'?: (_aether_v1_TaskCompletionEvent | null);
  /**
   * Optional native parent for a nested task created by a long-lived worker.
   * The gateway accepts an explicit value only when the caller is the active
   * parent task's assigned execution identity. This is a request-scoped binding:
   * it may select a different assigned task than the connection's startup/task-
   * token association. Empty preserves connection-associated parent inference.
   */
  'parentTaskId'?: (string);
  /**
   * TARGETED mode only. QUEUE persists the task for delivery when the exact
   * static worker reconnects, without requiring an orchestration registry
   * entry. REJECT fails task creation while the worker is absent.
   */
  'targetOfflinePolicy'?: (_aether_v1_TargetOfflinePolicy);
  /**
   * Minimum delegation capacity the task's final execution identity must
   * retain after task-authority setup. Currently 0 or 1. Set to 1 when the
   * worker must perform one explicit downstream authorization continuation
   * (for example, Sahara querying the tool catalog under the user's authority).
   * In POOL mode the gateway reserves the additional anchor-to-assignee hop.
   */
  'requiredDownstreamAuthorityHops'?: (number);
  /**
   * WorkflowEngine-only authority audience binding. The gateway accepts this
   * field only from the authenticated WorkflowEngine principal and requires it
   * to match a workflow_schedule audience on authorization. Ordinary task
   * creators must leave it empty.
   */
  'originatingScheduleId'?: (string);
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
  /**
   * Optional idempotency key for exactly-once task creation. When non-empty the
   * gateway dedupes creation on this key: a duplicate request returns the
   * existing task identity instead of creating a second task. Workflow joins
   * set it to make an on_complete create_task fire exactly once under retries
   * or engine restart.
   */
  'idempotencyKey': (string);
  /**
   * Optional fan-out/fan-in correlation identity, distinct from task_id. When
   * non-empty it is persisted on the task and propagated to child spawns, and is
   * queryable via TaskFilter.correlation_id. The barrier/group id a join matches.
   */
  'correlationId': (string);
  /**
   * Optional top-of-fan-out-tree identity (the flow / run id). Defaults to the
   * task's own id when it is a root; inherited by descendants on spawn. Becomes
   * the DAG run id when the join engine grows into full fan-out/fan-in.
   */
  'rootTaskId': (string);
  /**
   * Optional "feed B" config: emit a domain event onto event::* when this task
   * reaches a (selected) terminal status. Absent/disabled = no emission.
   */
  'completionEvent': (_aether_v1_TaskCompletionEvent__Output | null);
  /**
   * Optional native parent for a nested task created by a long-lived worker.
   * The gateway accepts an explicit value only when the caller is the active
   * parent task's assigned execution identity. This is a request-scoped binding:
   * it may select a different assigned task than the connection's startup/task-
   * token association. Empty preserves connection-associated parent inference.
   */
  'parentTaskId': (string);
  /**
   * TARGETED mode only. QUEUE persists the task for delivery when the exact
   * static worker reconnects, without requiring an orchestration registry
   * entry. REJECT fails task creation while the worker is absent.
   */
  'targetOfflinePolicy': (_aether_v1_TargetOfflinePolicy__Output);
  /**
   * Minimum delegation capacity the task's final execution identity must
   * retain after task-authority setup. Currently 0 or 1. Set to 1 when the
   * worker must perform one explicit downstream authorization continuation
   * (for example, Sahara querying the tool catalog under the user's authority).
   * In POOL mode the gateway reserves the additional anchor-to-assignee hop.
   */
  'requiredDownstreamAuthorityHops': (number);
  /**
   * WorkflowEngine-only authority audience binding. The gateway accepts this
   * field only from the authenticated WorkflowEngine principal and requires it
   * to match a workflow_schedule audience on authorization. Ordinary task
   * creators must leave it empty.
   */
  'originatingScheduleId': (string);
}
