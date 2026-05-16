// Original file: aether.proto

import type { TaskStatus as _aether_v1_TaskStatus, TaskStatus__Output as _aether_v1_TaskStatus__Output } from '../../aether/v1/TaskStatus';

/**
 * TaskStatusChangedEvent fires on lifecycle transitions: pending->running,
 * running->completed, running->failed, paused->running, etc.
 */
export interface TaskStatusChangedEvent {
  'fromStatus'?: (_aether_v1_TaskStatus);
  'toStatus'?: (_aether_v1_TaskStatus);
  /**
   * optional narrative (e.g. cancel reason, fail message)
   */
  'reason'?: (string);
}

/**
 * TaskStatusChangedEvent fires on lifecycle transitions: pending->running,
 * running->completed, running->failed, paused->running, etc.
 */
export interface TaskStatusChangedEvent__Output {
  'fromStatus': (_aether_v1_TaskStatus__Output);
  'toStatus': (_aether_v1_TaskStatus__Output);
  /**
   * optional narrative (e.g. cancel reason, fail message)
   */
  'reason': (string);
}
