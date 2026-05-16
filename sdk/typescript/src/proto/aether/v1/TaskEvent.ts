// Original file: aether.proto

import type { TaskStatusChangedEvent as _aether_v1_TaskStatusChangedEvent, TaskStatusChangedEvent__Output as _aether_v1_TaskStatusChangedEvent__Output } from '../../aether/v1/TaskStatusChangedEvent';
import type { TaskProgressEvent as _aether_v1_TaskProgressEvent, TaskProgressEvent__Output as _aether_v1_TaskProgressEvent__Output } from '../../aether/v1/TaskProgressEvent';
import type { TaskChildLifecycleEvent as _aether_v1_TaskChildLifecycleEvent, TaskChildLifecycleEvent__Output as _aether_v1_TaskChildLifecycleEvent__Output } from '../../aether/v1/TaskChildLifecycleEvent';
import type { TaskAuthorityRequestEventRelay as _aether_v1_TaskAuthorityRequestEventRelay, TaskAuthorityRequestEventRelay__Output as _aether_v1_TaskAuthorityRequestEventRelay__Output } from '../../aether/v1/TaskAuthorityRequestEventRelay';
import type { Long } from '@grpc/proto-loader';

/**
 * TaskEvent is the typed payload published on the task-scoped topic
 * `tk.{workspace}.{task_id}.events` and delivered downstream to subscribers.
 */
export interface TaskEvent {
  'taskId'?: (string);
  'emittedAtUnixMs'?: (number | string | Long);
  /**
   * The workspace and (when applicable) parent task id for routing/auth.
   */
  'workspace'?: (string);
  'parentTaskId'?: (string);
  /**
   * Subscription id this event is being delivered to (gateway-stamped on send).
   */
  'subscriptionId'?: (string);
  'statusChanged'?: (_aether_v1_TaskStatusChangedEvent | null);
  'progress'?: (_aether_v1_TaskProgressEvent | null);
  'childLifecycle'?: (_aether_v1_TaskChildLifecycleEvent | null);
  'authorityRequest'?: (_aether_v1_TaskAuthorityRequestEventRelay | null);
  'event'?: "statusChanged"|"progress"|"childLifecycle"|"authorityRequest";
}

/**
 * TaskEvent is the typed payload published on the task-scoped topic
 * `tk.{workspace}.{task_id}.events` and delivered downstream to subscribers.
 */
export interface TaskEvent__Output {
  'taskId': (string);
  'emittedAtUnixMs': (string);
  /**
   * The workspace and (when applicable) parent task id for routing/auth.
   */
  'workspace': (string);
  'parentTaskId': (string);
  /**
   * Subscription id this event is being delivered to (gateway-stamped on send).
   */
  'subscriptionId': (string);
  'statusChanged'?: (_aether_v1_TaskStatusChangedEvent__Output | null);
  'progress'?: (_aether_v1_TaskProgressEvent__Output | null);
  'childLifecycle'?: (_aether_v1_TaskChildLifecycleEvent__Output | null);
  'authorityRequest'?: (_aether_v1_TaskAuthorityRequestEventRelay__Output | null);
  'event'?: "statusChanged"|"progress"|"childLifecycle"|"authorityRequest";
}
