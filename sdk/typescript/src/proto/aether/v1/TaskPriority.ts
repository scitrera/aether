// Original file: aether.proto

/**
 * TaskPriority orders task dispatch. Unlike TaskClass (a UI hint), the server
 * USES priority for scheduling: among pending tasks eligible for delivery,
 * higher priority is dispatched first; ties break FIFO (oldest created_at).
 * Underlying values are intentionally spaced so new levels can be inserted
 * later, and the numeric value is used directly as the descending sort key.
 */
export const TaskPriority = {
  /**
   * Treated as NORMAL for back-compat.
   */
  TASK_PRIORITY_UNSPECIFIED: 'TASK_PRIORITY_UNSPECIFIED',
  /**
   * Lowest; best-effort, yields to everything.
   */
  TASK_PRIORITY_XLOW: 'TASK_PRIORITY_XLOW',
  /**
   * Below normal.
   */
  TASK_PRIORITY_LOW: 'TASK_PRIORITY_LOW',
  /**
   * Default.
   */
  TASK_PRIORITY_NORMAL: 'TASK_PRIORITY_NORMAL',
  /**
   * Above normal; jumps ahead of normal/low.
   */
  TASK_PRIORITY_HIGH: 'TASK_PRIORITY_HIGH',
  /**
   * Highest; reserved for future true preemption.
   */
  TASK_PRIORITY_PREEMPT: 'TASK_PRIORITY_PREEMPT',
} as const;

/**
 * TaskPriority orders task dispatch. Unlike TaskClass (a UI hint), the server
 * USES priority for scheduling: among pending tasks eligible for delivery,
 * higher priority is dispatched first; ties break FIFO (oldest created_at).
 * Underlying values are intentionally spaced so new levels can be inserted
 * later, and the numeric value is used directly as the descending sort key.
 */
export type TaskPriority =
  /**
   * Treated as NORMAL for back-compat.
   */
  | 'TASK_PRIORITY_UNSPECIFIED'
  | 0
  /**
   * Lowest; best-effort, yields to everything.
   */
  | 'TASK_PRIORITY_XLOW'
  | 10
  /**
   * Below normal.
   */
  | 'TASK_PRIORITY_LOW'
  | 20
  /**
   * Default.
   */
  | 'TASK_PRIORITY_NORMAL'
  | 30
  /**
   * Above normal; jumps ahead of normal/low.
   */
  | 'TASK_PRIORITY_HIGH'
  | 40
  /**
   * Highest; reserved for future true preemption.
   */
  | 'TASK_PRIORITY_PREEMPT'
  | 50

/**
 * TaskPriority orders task dispatch. Unlike TaskClass (a UI hint), the server
 * USES priority for scheduling: among pending tasks eligible for delivery,
 * higher priority is dispatched first; ties break FIFO (oldest created_at).
 * Underlying values are intentionally spaced so new levels can be inserted
 * later, and the numeric value is used directly as the descending sort key.
 */
export type TaskPriority__Output = typeof TaskPriority[keyof typeof TaskPriority]
