// Original file: aether.proto

/**
 * TaskStatus enumerates the lifecycle states of a task.
 */
export const TaskStatus = {
  TASK_STATUS_UNSPECIFIED: 'TASK_STATUS_UNSPECIFIED',
  TASK_STATUS_QUEUED: 'TASK_STATUS_QUEUED',
  TASK_STATUS_RUNNING: 'TASK_STATUS_RUNNING',
  TASK_STATUS_COMPLETED: 'TASK_STATUS_COMPLETED',
  TASK_STATUS_FAILED: 'TASK_STATUS_FAILED',
  TASK_STATUS_CANCELLED: 'TASK_STATUS_CANCELLED',
} as const;

/**
 * TaskStatus enumerates the lifecycle states of a task.
 */
export type TaskStatus =
  | 'TASK_STATUS_UNSPECIFIED'
  | 0
  | 'TASK_STATUS_QUEUED'
  | 1
  | 'TASK_STATUS_RUNNING'
  | 2
  | 'TASK_STATUS_COMPLETED'
  | 3
  | 'TASK_STATUS_FAILED'
  | 4
  | 'TASK_STATUS_CANCELLED'
  | 5

/**
 * TaskStatus enumerates the lifecycle states of a task.
 */
export type TaskStatus__Output = typeof TaskStatus[keyof typeof TaskStatus]
