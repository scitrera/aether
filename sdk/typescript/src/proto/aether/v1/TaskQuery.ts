// Original file: aether.proto

import type { TaskFilter as _aether_v1_TaskFilter, TaskFilter__Output as _aether_v1_TaskFilter__Output } from '../../aether/v1/TaskFilter';

// Original file: aether.proto

export const _aether_v1_TaskQuery_OpType = {
  /**
   * GET /api/tasks - List tasks with optional filters
   */
  LIST: 'LIST',
  /**
   * GET /api/tasks/{id} - Get a specific task by ID
   */
  GET: 'GET',
} as const;

export type _aether_v1_TaskQuery_OpType =
  /**
   * GET /api/tasks - List tasks with optional filters
   */
  | 'LIST'
  | 0
  /**
   * GET /api/tasks/{id} - Get a specific task by ID
   */
  | 'GET'
  | 1

export type _aether_v1_TaskQuery_OpType__Output = typeof _aether_v1_TaskQuery_OpType[keyof typeof _aether_v1_TaskQuery_OpType]

/**
 * TaskQuery allows clients to list and get tasks through the gRPC streaming
 * interface. This provides feature parity with the REST Admin API for task
 * queries.
 * REST equivalents: GET /api/tasks, GET /api/tasks/{id}
 */
export interface TaskQuery {
  'op'?: (_aether_v1_TaskQuery_OpType);
  /**
   * For GET: the task ID to retrieve
   */
  'taskId'?: (string);
  /**
   * For LIST: optional filter parameters
   */
  'filter'?: (_aether_v1_TaskFilter | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * TaskQuery allows clients to list and get tasks through the gRPC streaming
 * interface. This provides feature parity with the REST Admin API for task
 * queries.
 * REST equivalents: GET /api/tasks, GET /api/tasks/{id}
 */
export interface TaskQuery__Output {
  'op': (_aether_v1_TaskQuery_OpType__Output);
  /**
   * For GET: the task ID to retrieve
   */
  'taskId': (string);
  /**
   * For LIST: optional filter parameters
   */
  'filter': (_aether_v1_TaskFilter__Output | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
