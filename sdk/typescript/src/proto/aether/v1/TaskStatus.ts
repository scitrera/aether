// Original file: aether.proto

/**
 * TaskStatus enumerates the lifecycle states of a task.
 * Note: the proto-side enum is coarser than the Go-side TaskStatus
 * (pending/assigned/starting all collapse to TASK_STATUS_QUEUED on the wire).
 * The new WAITING_* /HIBERNATED/REJECTED states added in Phase 1 are distinct
 * on the wire because clients (e.g. the Python SDK) need to react to them
 * directly.
 */
export const TaskStatus = {
  TASK_STATUS_UNSPECIFIED: 'TASK_STATUS_UNSPECIFIED',
  TASK_STATUS_QUEUED: 'TASK_STATUS_QUEUED',
  TASK_STATUS_RUNNING: 'TASK_STATUS_RUNNING',
  TASK_STATUS_COMPLETED: 'TASK_STATUS_COMPLETED',
  TASK_STATUS_FAILED: 'TASK_STATUS_FAILED',
  TASK_STATUS_CANCELLED: 'TASK_STATUS_CANCELLED',
  /**
   * A2A: INPUT_REQUIRED — parked pending typed input
   */
  TASK_STATUS_WAITING_INPUT: 'TASK_STATUS_WAITING_INPUT',
  /**
   * A2A: AUTH_REQUIRED — parked pending authority grant
   */
  TASK_STATUS_WAITING_AUTHORITY: 'TASK_STATUS_WAITING_AUTHORITY',
  /**
   * parked pending child/sibling task transitions
   */
  TASK_STATUS_WAITING_DEPENDENCY: 'TASK_STATUS_WAITING_DEPENDENCY',
  /**
   * checkpointed and evicted from compute
   */
  TASK_STATUS_HIBERNATED: 'TASK_STATUS_HIBERNATED',
  /**
   * agent declined before processing (distinct from failed)
   */
  TASK_STATUS_REJECTED: 'TASK_STATUS_REJECTED',
} as const;

/**
 * TaskStatus enumerates the lifecycle states of a task.
 * Note: the proto-side enum is coarser than the Go-side TaskStatus
 * (pending/assigned/starting all collapse to TASK_STATUS_QUEUED on the wire).
 * The new WAITING_* /HIBERNATED/REJECTED states added in Phase 1 are distinct
 * on the wire because clients (e.g. the Python SDK) need to react to them
 * directly.
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
   * A2A: INPUT_REQUIRED — parked pending typed input
   */
  | 'TASK_STATUS_WAITING_INPUT'
  | 6
  /**
   * A2A: AUTH_REQUIRED — parked pending authority grant
   */
  | 'TASK_STATUS_WAITING_AUTHORITY'
  | 7
  /**
   * parked pending child/sibling task transitions
   */
  | 'TASK_STATUS_WAITING_DEPENDENCY'
  | 8
  /**
   * checkpointed and evicted from compute
   */
  | 'TASK_STATUS_HIBERNATED'
  | 9
  /**
   * agent declined before processing (distinct from failed)
   */
  | 'TASK_STATUS_REJECTED'
  | 10

/**
 * TaskStatus enumerates the lifecycle states of a task.
 * Note: the proto-side enum is coarser than the Go-side TaskStatus
 * (pending/assigned/starting all collapse to TASK_STATUS_QUEUED on the wire).
 * The new WAITING_* /HIBERNATED/REJECTED states added in Phase 1 are distinct
 * on the wire because clients (e.g. the Python SDK) need to react to them
 * directly.
 */
export type TaskStatus__Output = typeof TaskStatus[keyof typeof TaskStatus]
