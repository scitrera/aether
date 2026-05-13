// Original file: aether.proto

export const TaskAssignmentMode = {
  /**
   * Default: agent assigns to itself
   */
  SELF_ASSIGN: 'SELF_ASSIGN',
  /**
   * Assign to specific agent (may trigger orchestration)
   */
  TARGETED: 'TARGETED',
  /**
   * Any matching worker can claim the task
   */
  POOL: 'POOL',
} as const;

export type TaskAssignmentMode =
  /**
   * Default: agent assigns to itself
   */
  | 'SELF_ASSIGN'
  | 0
  /**
   * Assign to specific agent (may trigger orchestration)
   */
  | 'TARGETED'
  | 1
  /**
   * Any matching worker can claim the task
   */
  | 'POOL'
  | 2

export type TaskAssignmentMode__Output = typeof TaskAssignmentMode[keyof typeof TaskAssignmentMode]
