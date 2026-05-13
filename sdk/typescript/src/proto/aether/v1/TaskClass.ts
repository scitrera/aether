// Original file: aether.proto

/**
 * TaskClass categorizes how UIs should surface a task's lifecycle.
 * Orthogonal to TaskStatus (lifecycle) and TaskAssignmentMode (assignment):
 * purely a hint for clients deciding whether/how to show progress and
 * notifications. Servers persist and propagate it but do NOT make
 * scheduling/dispatch/ACL decisions on it.
 */
export const TaskClass = {
  /**
   * Treated as INTERACTIVE for back-compat.
   */
  TASK_CLASS_UNSPECIFIED: 'TASK_CLASS_UNSPECIFIED',
  /**
   * Short-lived, user actively waiting.
   */
  TASK_CLASS_INTERACTIVE: 'TASK_CLASS_INTERACTIVE',
  /**
   * Surface in foreground bell with live progress.
   */
  TASK_CLASS_BACKGROUND: 'TASK_CLASS_BACKGROUND',
  /**
   * Hidden from user-facing notifications by default;
   * visible only in admin dashboards.
   */
  TASK_CLASS_BATCH: 'TASK_CLASS_BATCH',
} as const;

/**
 * TaskClass categorizes how UIs should surface a task's lifecycle.
 * Orthogonal to TaskStatus (lifecycle) and TaskAssignmentMode (assignment):
 * purely a hint for clients deciding whether/how to show progress and
 * notifications. Servers persist and propagate it but do NOT make
 * scheduling/dispatch/ACL decisions on it.
 */
export type TaskClass =
  /**
   * Treated as INTERACTIVE for back-compat.
   */
  | 'TASK_CLASS_UNSPECIFIED'
  | 0
  /**
   * Short-lived, user actively waiting.
   */
  | 'TASK_CLASS_INTERACTIVE'
  | 1
  /**
   * Surface in foreground bell with live progress.
   */
  | 'TASK_CLASS_BACKGROUND'
  | 2
  /**
   * Hidden from user-facing notifications by default;
   * visible only in admin dashboards.
   */
  | 'TASK_CLASS_BATCH'
  | 3

/**
 * TaskClass categorizes how UIs should surface a task's lifecycle.
 * Orthogonal to TaskStatus (lifecycle) and TaskAssignmentMode (assignment):
 * purely a hint for clients deciding whether/how to show progress and
 * notifications. Servers persist and propagate it but do NOT make
 * scheduling/dispatch/ACL decisions on it.
 */
export type TaskClass__Output = typeof TaskClass[keyof typeof TaskClass]
