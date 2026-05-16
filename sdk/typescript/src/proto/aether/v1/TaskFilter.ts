// Original file: aether.proto

import type { TaskStatus as _aether_v1_TaskStatus, TaskStatus__Output as _aether_v1_TaskStatus__Output } from '../../aether/v1/TaskStatus';
import type { TaskClass as _aether_v1_TaskClass, TaskClass__Output as _aether_v1_TaskClass__Output } from '../../aether/v1/TaskClass';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { Long } from '@grpc/proto-loader';

/**
 * TaskFilter specifies filtering parameters for listing tasks.
 * Matches REST API query parameters: status, workspace, type, limit, offset.
 */
export interface TaskFilter {
  /**
   * Filter by single task status (deprecated — use statuses)
   */
  'status'?: (_aether_v1_TaskStatus);
  /**
   * Filter by workspace
   */
  'workspace'?: (string);
  /**
   * Filter by task type
   */
  'taskType'?: (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit'?: (number);
  /**
   * Offset for pagination
   */
  'offset'?: (number);
  /**
   * Filter by multiple task statuses (takes priority over status)
   */
  'statuses'?: (_aether_v1_TaskStatus)[];
  /**
   * Authority lineage filters (first-class task authority fields).
   */
  'subjectType'?: (string);
  /**
   * Filter by subject canonical identity
   */
  'subjectId'?: (string);
  /**
   * "direct" or "on_behalf_of"
   */
  'authorityMode'?: (string);
  /**
   * Filter by current authority grant id
   */
  'authorityGrantId'?: (string);
  /**
   * Filter by root authority grant id
   */
  'rootAuthorityGrantId'?: (string);
  /**
   * Filter by parent task id (nested task trees)
   */
  'parentTaskId'?: (string);
  /**
   * Optional UI hint filter; 0 = no filter.
   */
  'taskClass'?: (_aether_v1_TaskClass);
  /**
   * Inverted UI hint filter: any task whose task_class appears in this list
   * is omitted. Forward-compatible: new TaskClass values pass through unless
   * explicitly excluded. Combinable with task_class above (positive single
   * include) but the typical use is one or the other.
   */
  'excludeTaskClasses'?: (_aether_v1_TaskClass)[];
  /**
   * Filter by client-minted session identifier (Phase 1). Mirrors the
   * context_id added to CreateTaskRequest / TaskInfo. Empty = no filter.
   */
  'contextId'?: (string);
  /**
   * Inverted status filter (Phase 1). Any task whose status is in this list
   * is omitted. Combine with status/statuses to express "all non-terminal"
   * or similar queries without enumerating the affirmative set.
   */
  'excludeStatuses'?: (_aether_v1_TaskStatus)[];
  /**
   * Phase 4: management-surface filter extensions. Mirrors A2A's ListTasks
   * semantics over Aether's bidi-stream surface.
   * 
   * Filter by the actor that created the task (lineage). The principal_id
   * field is matched against the task's stored creator identity column;
   * principal_type is informational only since the storage column is a single
   * canonical identity string. Empty = no filter.
   */
  'creatorActor'?: (_aether_v1_PrincipalRef | null);
  /**
   * Filter by status-changed time. Returns tasks whose most recent status
   * transition occurred at or after this unix-ms timestamp. 0 = no filter.
   */
  'statusTimestampAfterUnixMs'?: (number | string | Long);
  /**
   * Cursor-based pagination. Empty = start from the beginning. The server
   * returns a `next_page_token` on TaskQueryResponse when more results exist;
   * pass it back to fetch the next page. The cursor encodes the last
   * (updated_at, task_id) tuple seen and is opaque to clients. Cursor
   * pagination takes priority over Limit/Offset when both are supplied.
   */
  'pageToken'?: (string);
  /**
   * When true and parent_task_id is set, list ALL descendants (recursive),
   * not just direct children. Default false: direct children only, preserving
   * the existing parent_task_id behavior.
   */
  'includeDescendants'?: (boolean);
}

/**
 * TaskFilter specifies filtering parameters for listing tasks.
 * Matches REST API query parameters: status, workspace, type, limit, offset.
 */
export interface TaskFilter__Output {
  /**
   * Filter by single task status (deprecated — use statuses)
   */
  'status': (_aether_v1_TaskStatus__Output);
  /**
   * Filter by workspace
   */
  'workspace': (string);
  /**
   * Filter by task type
   */
  'taskType': (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
  /**
   * Filter by multiple task statuses (takes priority over status)
   */
  'statuses': (_aether_v1_TaskStatus__Output)[];
  /**
   * Authority lineage filters (first-class task authority fields).
   */
  'subjectType': (string);
  /**
   * Filter by subject canonical identity
   */
  'subjectId': (string);
  /**
   * "direct" or "on_behalf_of"
   */
  'authorityMode': (string);
  /**
   * Filter by current authority grant id
   */
  'authorityGrantId': (string);
  /**
   * Filter by root authority grant id
   */
  'rootAuthorityGrantId': (string);
  /**
   * Filter by parent task id (nested task trees)
   */
  'parentTaskId': (string);
  /**
   * Optional UI hint filter; 0 = no filter.
   */
  'taskClass': (_aether_v1_TaskClass__Output);
  /**
   * Inverted UI hint filter: any task whose task_class appears in this list
   * is omitted. Forward-compatible: new TaskClass values pass through unless
   * explicitly excluded. Combinable with task_class above (positive single
   * include) but the typical use is one or the other.
   */
  'excludeTaskClasses': (_aether_v1_TaskClass__Output)[];
  /**
   * Filter by client-minted session identifier (Phase 1). Mirrors the
   * context_id added to CreateTaskRequest / TaskInfo. Empty = no filter.
   */
  'contextId': (string);
  /**
   * Inverted status filter (Phase 1). Any task whose status is in this list
   * is omitted. Combine with status/statuses to express "all non-terminal"
   * or similar queries without enumerating the affirmative set.
   */
  'excludeStatuses': (_aether_v1_TaskStatus__Output)[];
  /**
   * Phase 4: management-surface filter extensions. Mirrors A2A's ListTasks
   * semantics over Aether's bidi-stream surface.
   * 
   * Filter by the actor that created the task (lineage). The principal_id
   * field is matched against the task's stored creator identity column;
   * principal_type is informational only since the storage column is a single
   * canonical identity string. Empty = no filter.
   */
  'creatorActor': (_aether_v1_PrincipalRef__Output | null);
  /**
   * Filter by status-changed time. Returns tasks whose most recent status
   * transition occurred at or after this unix-ms timestamp. 0 = no filter.
   */
  'statusTimestampAfterUnixMs': (string);
  /**
   * Cursor-based pagination. Empty = start from the beginning. The server
   * returns a `next_page_token` on TaskQueryResponse when more results exist;
   * pass it back to fetch the next page. The cursor encodes the last
   * (updated_at, task_id) tuple seen and is opaque to clients. Cursor
   * pagination takes priority over Limit/Offset when both are supplied.
   */
  'pageToken': (string);
  /**
   * When true and parent_task_id is set, list ALL descendants (recursive),
   * not just direct children. Default false: direct children only, preserving
   * the existing parent_task_id behavior.
   */
  'includeDescendants': (boolean);
}
