// Original file: aether.proto

import type { BackoffStrategy as _aether_v1_BackoffStrategy, BackoffStrategy__Output as _aether_v1_BackoffStrategy__Output } from '../../aether/v1/BackoffStrategy';
import type { Long } from '@grpc/proto-loader';

/**
 * RetryPolicy lifts retry scheduling out of individual workers and into the
 * task store. When a task carries a RetryPolicy and a worker calls FailTask
 * without an explicit reschedule, the store computes next_retry_at from
 * this policy and re-pends the task. The waker then picks it up at the
 * scheduled time. Workers can still call RescheduleTaskAt to override
 * (e.g., to honor a Retry-After header).
 */
export interface RetryPolicy {
  /**
   * Total attempts allowed (1 = no retries). 0 means use server default (3).
   */
  'maxAttempts'?: (number);
  'backoff'?: (_aether_v1_BackoffStrategy);
  'initialDelayMs'?: (number | string | Long);
  'maxDelayMs'?: (number | string | Long);
  /**
   * Multiplicative random jitter in [0,1]. Final delay = computed *
   * (1 + uniform(-jitter, +jitter)).
   */
  'jitterFactor'?: (number | string);
  /**
   * For BACKOFF_STRATEGY_EXPLICIT_SCHEDULE. Indexed by 0-based attempt
   * number; the final entry is used for any subsequent attempt.
   */
  'scheduleMs'?: (number | string | Long)[];
  /**
   * Optional worker convention: HTTP-style status codes that should
   * trigger a retry. The task store does not inspect failure context;
   * workers use this to decide whether to FailTask or CompleteTask with
   * permanent-failure semantics.
   */
  'retryableStatusCodes'?: (number)[];
  /**
   * Worker hint: prefer Retry-After (or analogous) over computed delay
   * when set on the failure response.
   */
  'honorRetryAfter'?: (boolean);
}

/**
 * RetryPolicy lifts retry scheduling out of individual workers and into the
 * task store. When a task carries a RetryPolicy and a worker calls FailTask
 * without an explicit reschedule, the store computes next_retry_at from
 * this policy and re-pends the task. The waker then picks it up at the
 * scheduled time. Workers can still call RescheduleTaskAt to override
 * (e.g., to honor a Retry-After header).
 */
export interface RetryPolicy__Output {
  /**
   * Total attempts allowed (1 = no retries). 0 means use server default (3).
   */
  'maxAttempts': (number);
  'backoff': (_aether_v1_BackoffStrategy__Output);
  'initialDelayMs': (string);
  'maxDelayMs': (string);
  /**
   * Multiplicative random jitter in [0,1]. Final delay = computed *
   * (1 + uniform(-jitter, +jitter)).
   */
  'jitterFactor': (number);
  /**
   * For BACKOFF_STRATEGY_EXPLICIT_SCHEDULE. Indexed by 0-based attempt
   * number; the final entry is used for any subsequent attempt.
   */
  'scheduleMs': (string)[];
  /**
   * Optional worker convention: HTTP-style status codes that should
   * trigger a retry. The task store does not inspect failure context;
   * workers use this to decide whether to FailTask or CompleteTask with
   * permanent-failure semantics.
   */
  'retryableStatusCodes': (number)[];
  /**
   * Worker hint: prefer Retry-After (or analogous) over computed delay
   * when set on the failure response.
   */
  'honorRetryAfter': (boolean);
}
