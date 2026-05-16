// Original file: aether.proto

import type { TaskInfo as _aether_v1_TaskInfo, TaskInfo__Output as _aether_v1_TaskInfo__Output } from '../../aether/v1/TaskInfo';

/**
 * TaskQueryResponse is sent in response to TaskQuery operations.
 * Contains task data for either GET (single task) or LIST (multiple tasks).
 */
export interface TaskQueryResponse {
  'success'?: (boolean);
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * For GET operation (single task)
   */
  'task'?: (_aether_v1_TaskInfo | null);
  /**
   * For LIST operation (multiple tasks)
   */
  'tasks'?: (_aether_v1_TaskInfo)[];
  /**
   * For LIST: total count (may differ from returned if paginated)
   */
  'totalCount'?: (number);
  /**
   * Echoed from the originating TaskQuery for correlation
   */
  'requestId'?: (string);
  /**
   * Phase 4: cursor-based pagination. Populated when LIST results were
   * returned at the requested page size and more rows may exist. Empty when
   * the last page has been served. Pass back as TaskFilter.page_token.
   */
  'nextPageToken'?: (string);
}

/**
 * TaskQueryResponse is sent in response to TaskQuery operations.
 * Contains task data for either GET (single task) or LIST (multiple tasks).
 */
export interface TaskQueryResponse__Output {
  'success': (boolean);
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * For GET operation (single task)
   */
  'task': (_aether_v1_TaskInfo__Output | null);
  /**
   * For LIST operation (multiple tasks)
   */
  'tasks': (_aether_v1_TaskInfo__Output)[];
  /**
   * For LIST: total count (may differ from returned if paginated)
   */
  'totalCount': (number);
  /**
   * Echoed from the originating TaskQuery for correlation
   */
  'requestId': (string);
  /**
   * Phase 4: cursor-based pagination. Populated when LIST results were
   * returned at the requested page size and more rows may exist. Empty when
   * the last page has been served. Pass back as TaskFilter.page_token.
   */
  'nextPageToken': (string);
}
