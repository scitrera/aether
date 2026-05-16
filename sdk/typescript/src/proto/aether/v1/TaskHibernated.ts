// Original file: aether.proto

import type { HibernationDescriptor as _aether_v1_HibernationDescriptor, HibernationDescriptor__Output as _aether_v1_HibernationDescriptor__Output } from '../../aether/v1/HibernationDescriptor';

/**
 * TaskHibernated is sent to the worker assigned to a task immediately after it
 * transitions to HIBERNATED. Workers SHOULD close their gRPC stream cleanly
 * after receiving this; the gateway will tolerate the disconnect and the
 * disconnect_reaper skips HIBERNATED tasks.
 */
export interface TaskHibernated {
  'taskId'?: (string);
  /**
   * Echo of the descriptor so the worker can verify it's hibernating with the
   * expected checkpoint key.
   */
  'descriptor'?: (_aether_v1_HibernationDescriptor | null);
}

/**
 * TaskHibernated is sent to the worker assigned to a task immediately after it
 * transitions to HIBERNATED. Workers SHOULD close their gRPC stream cleanly
 * after receiving this; the gateway will tolerate the disconnect and the
 * disconnect_reaper skips HIBERNATED tasks.
 */
export interface TaskHibernated__Output {
  'taskId': (string);
  /**
   * Echo of the descriptor so the worker can verify it's hibernating with the
   * expected checkpoint key.
   */
  'descriptor': (_aether_v1_HibernationDescriptor__Output | null);
}
