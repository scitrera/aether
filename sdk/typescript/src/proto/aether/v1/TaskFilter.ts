// Original file: aether.proto

import type { TaskStatus as _aether_v1_TaskStatus, TaskStatus__Output as _aether_v1_TaskStatus__Output } from '../../aether/v1/TaskStatus';
import type { TaskClass as _aether_v1_TaskClass, TaskClass__Output as _aether_v1_TaskClass__Output } from '../../aether/v1/TaskClass';

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
}
