// Original file: aether.proto


// Original file: aether.proto

export const _aether_v1_TaskOperation_OpType = {
  /**
   * POST /api/tasks/{id}/retry - Retry a failed task
   */
  RETRY: 'RETRY',
  /**
   * POST /api/tasks/{id}/cancel - Cancel a running or queued task
   */
  CANCEL: 'CANCEL',
  /**
   * POST /api/tasks/{id}/complete - Mark a task as completed (for POOL workers)
   */
  COMPLETE: 'COMPLETE',
  /**
   * POST /api/tasks/{id}/fail - Mark a task as failed (for POOL workers)
   */
  FAIL: 'FAIL',
} as const;

export type _aether_v1_TaskOperation_OpType =
  /**
   * POST /api/tasks/{id}/retry - Retry a failed task
   */
  | 'RETRY'
  | 0
  /**
   * POST /api/tasks/{id}/cancel - Cancel a running or queued task
   */
  | 'CANCEL'
  | 1
  /**
   * POST /api/tasks/{id}/complete - Mark a task as completed (for POOL workers)
   */
  | 'COMPLETE'
  | 2
  /**
   * POST /api/tasks/{id}/fail - Mark a task as failed (for POOL workers)
   */
  | 'FAIL'
  | 3

export type _aether_v1_TaskOperation_OpType__Output = typeof _aether_v1_TaskOperation_OpType[keyof typeof _aether_v1_TaskOperation_OpType]

/**
 * TaskOperation allows clients to perform lifecycle operations on tasks.
 * This includes retrying failed tasks, canceling running/queued tasks,
 * and reporting task completion or failure from POOL workers.
 * REST equivalents: POST /api/tasks/{id}/retry, POST /api/tasks/{id}/cancel,
 * POST /api/tasks/{id}/complete, POST /api/tasks/{id}/fail
 */
export interface TaskOperation {
  'op'?: (_aether_v1_TaskOperation_OpType);
  /**
   * The task ID to operate on (required for all operations)
   */
  'taskId'?: (string);
  /**
   * Optional reason for the operation (e.g., cancellation reason, failure message)
   */
  'reason'?: (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * TaskOperation allows clients to perform lifecycle operations on tasks.
 * This includes retrying failed tasks, canceling running/queued tasks,
 * and reporting task completion or failure from POOL workers.
 * REST equivalents: POST /api/tasks/{id}/retry, POST /api/tasks/{id}/cancel,
 * POST /api/tasks/{id}/complete, POST /api/tasks/{id}/fail
 */
export interface TaskOperation__Output {
  'op': (_aether_v1_TaskOperation_OpType__Output);
  /**
   * The task ID to operate on (required for all operations)
   */
  'taskId': (string);
  /**
   * Optional reason for the operation (e.g., cancellation reason, failure message)
   */
  'reason': (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
