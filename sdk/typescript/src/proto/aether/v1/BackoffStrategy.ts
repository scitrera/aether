// Original file: aether.proto

/**
 * BackoffStrategy describes how the task store scales delays across retry
 * attempts when a RetryPolicy is attached to a task. Defaults to
 * EXPONENTIAL when unspecified (preserves current behavior).
 */
export const BackoffStrategy = {
  BACKOFF_STRATEGY_UNSPECIFIED: 'BACKOFF_STRATEGY_UNSPECIFIED',
  /**
   * Same delay every attempt.
   */
  BACKOFF_STRATEGY_FIXED: 'BACKOFF_STRATEGY_FIXED',
  /**
   * delay = initial * 2^(n-1), capped at max_delay_ms.
   */
  BACKOFF_STRATEGY_EXPONENTIAL: 'BACKOFF_STRATEGY_EXPONENTIAL',
  /**
   * Use schedule_ms[attempt-1]; clamp the last entry.
   */
  BACKOFF_STRATEGY_EXPLICIT_SCHEDULE: 'BACKOFF_STRATEGY_EXPLICIT_SCHEDULE',
} as const;

/**
 * BackoffStrategy describes how the task store scales delays across retry
 * attempts when a RetryPolicy is attached to a task. Defaults to
 * EXPONENTIAL when unspecified (preserves current behavior).
 */
export type BackoffStrategy =
  | 'BACKOFF_STRATEGY_UNSPECIFIED'
  | 0
  /**
   * Same delay every attempt.
   */
  | 'BACKOFF_STRATEGY_FIXED'
  | 1
  /**
   * delay = initial * 2^(n-1), capped at max_delay_ms.
   */
  | 'BACKOFF_STRATEGY_EXPONENTIAL'
  | 2
  /**
   * Use schedule_ms[attempt-1]; clamp the last entry.
   */
  | 'BACKOFF_STRATEGY_EXPLICIT_SCHEDULE'
  | 3

/**
 * BackoffStrategy describes how the task store scales delays across retry
 * attempts when a RetryPolicy is attached to a task. Defaults to
 * EXPONENTIAL when unspecified (preserves current behavior).
 */
export type BackoffStrategy__Output = typeof BackoffStrategy[keyof typeof BackoffStrategy]
