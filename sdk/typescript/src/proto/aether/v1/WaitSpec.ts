// Original file: aether.proto

import type { WaitReason as _aether_v1_WaitReason, WaitReason__Output as _aether_v1_WaitReason__Output } from '../../aether/v1/WaitReason';
import type { Long } from '@grpc/proto-loader';

/**
 * WaitSpec describes why a task is paused and how it should be woken.
 * Which fields are meaningful depends on the WaitReason:
 * - WAIT_REASON_INPUT       -> expected_principal, input_match
 * - WAIT_REASON_AUTHORITY   -> authority_request_id
 * - WAIT_REASON_DEPENDENCY  -> depends_on, wake_on_any
 * - WAIT_REASON_HIBERNATION -> (none required; checkpoint key carried out-of-band)
 * timeout_ms / scheduled_wake_unix_ms apply across all reasons.
 */
export interface WaitSpec {
  'reason'?: (_aether_v1_WaitReason);
  /**
   * For WAITING_INPUT: principal identity expected to send the input message.
   * Empty = any principal in the workspace.
   */
  'expectedPrincipal'?: (string);
  /**
   * For WAITING_INPUT: optional metadata key/value the inbound message must
   * match for task_waker to consider it a wake trigger.
   */
  'inputMatch'?: ({[key: string]: string});
  /**
   * For WAITING_AUTHORITY: the authority request id this task is waiting on.
   * Resolved (granted or denied) via the Phase 2 authority-request flow.
   */
  'authorityRequestId'?: (string);
  /**
   * For WAITING_DEPENDENCY: task ids whose terminal transition wakes this
   * task. Default semantics: wake when ALL listed tasks reach a terminal
   * state.
   */
  'dependsOn'?: (string)[];
  /**
   * If true, wake on the FIRST listed task reaching a terminal state.
   */
  'wakeOnAny'?: (boolean);
  /**
   * Optional timeout. 0 = no timeout. On expiry, task_waker transitions the
   * task to FAILED with reason "wait timeout".
   */
  'timeoutMs'?: (number | string | Long);
  /**
   * Optional scheduled-wake timestamp (unix ms). Independent of timeout_ms —
   * task wakes at this absolute time if still paused.
   */
  'scheduledWakeUnixMs'?: (number | string | Long);
}

/**
 * WaitSpec describes why a task is paused and how it should be woken.
 * Which fields are meaningful depends on the WaitReason:
 * - WAIT_REASON_INPUT       -> expected_principal, input_match
 * - WAIT_REASON_AUTHORITY   -> authority_request_id
 * - WAIT_REASON_DEPENDENCY  -> depends_on, wake_on_any
 * - WAIT_REASON_HIBERNATION -> (none required; checkpoint key carried out-of-band)
 * timeout_ms / scheduled_wake_unix_ms apply across all reasons.
 */
export interface WaitSpec__Output {
  'reason': (_aether_v1_WaitReason__Output);
  /**
   * For WAITING_INPUT: principal identity expected to send the input message.
   * Empty = any principal in the workspace.
   */
  'expectedPrincipal': (string);
  /**
   * For WAITING_INPUT: optional metadata key/value the inbound message must
   * match for task_waker to consider it a wake trigger.
   */
  'inputMatch': ({[key: string]: string});
  /**
   * For WAITING_AUTHORITY: the authority request id this task is waiting on.
   * Resolved (granted or denied) via the Phase 2 authority-request flow.
   */
  'authorityRequestId': (string);
  /**
   * For WAITING_DEPENDENCY: task ids whose terminal transition wakes this
   * task. Default semantics: wake when ALL listed tasks reach a terminal
   * state.
   */
  'dependsOn': (string)[];
  /**
   * If true, wake on the FIRST listed task reaching a terminal state.
   */
  'wakeOnAny': (boolean);
  /**
   * Optional timeout. 0 = no timeout. On expiry, task_waker transitions the
   * task to FAILED with reason "wait timeout".
   */
  'timeoutMs': (string);
  /**
   * Optional scheduled-wake timestamp (unix ms). Independent of timeout_ms —
   * task wakes at this absolute time if still paused.
   */
  'scheduledWakeUnixMs': (string);
}
