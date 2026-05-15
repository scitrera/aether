// Original file: aether.proto

import type { WaitSpec as _aether_v1_WaitSpec, WaitSpec__Output as _aether_v1_WaitSpec__Output } from '../../aether/v1/WaitSpec';

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
  /**
   * Transition running -> WAITING_* with a typed reason. Requires wait_spec.
   */
  PAUSE: 'PAUSE',
  /**
   * Transition running -> WAITING_DEPENDENCY on specific task ids. Requires wait_spec.
   */
  WAIT_FOR: 'WAIT_FOR',
  /**
   * Force-resume a paused task (admin/manual; normally task_waker fires).
   */
  RESUME: 'RESUME',
  /**
   * Agent declines before processing -> REJECTED terminal state.
   */
  REJECT: 'REJECT',
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
  /**
   * Transition running -> WAITING_* with a typed reason. Requires wait_spec.
   */
  | 'PAUSE'
  | 4
  /**
   * Transition running -> WAITING_DEPENDENCY on specific task ids. Requires wait_spec.
   */
  | 'WAIT_FOR'
  | 5
  /**
   * Force-resume a paused task (admin/manual; normally task_waker fires).
   */
  | 'RESUME'
  | 6
  /**
   * Agent declines before processing -> REJECTED terminal state.
   */
  | 'REJECT'
  | 7

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
   * Optional reason for the operation (e.g., cancellation reason, failure message,
   * REJECT reason, or PAUSE reason narrative).
   */
  'reason'?: (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
  /**
   * Populated for PAUSE / WAIT_FOR ops (Phase 1). Describes why the task is
   * pausing and which wake conditions task_waker should monitor. Not used by
   * the other op types.
   */
  'waitSpec'?: (_aether_v1_WaitSpec | null);
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
   * Optional reason for the operation (e.g., cancellation reason, failure message,
   * REJECT reason, or PAUSE reason narrative).
   */
  'reason': (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
  /**
   * Populated for PAUSE / WAIT_FOR ops (Phase 1). Describes why the task is
   * pausing and which wake conditions task_waker should monitor. Not used by
   * the other op types.
   */
  'waitSpec': (_aether_v1_WaitSpec__Output | null);
}
