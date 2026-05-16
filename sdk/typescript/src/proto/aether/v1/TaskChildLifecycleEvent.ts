// Original file: aether.proto

import type { TaskStatus as _aether_v1_TaskStatus, TaskStatus__Output as _aether_v1_TaskStatus__Output } from '../../aether/v1/TaskStatus';

/**
 * TaskChildLifecycleEvent reports a child task's lifecycle transition. The
 * `lifecycle` field is a coarse classifier: "spawned" | "transitioned" |
 * "completed".
 */
export interface TaskChildLifecycleEvent {
  'childTaskId'?: (string);
  'childStatus'?: (_aether_v1_TaskStatus);
  'lifecycle'?: (string);
}

/**
 * TaskChildLifecycleEvent reports a child task's lifecycle transition. The
 * `lifecycle` field is a coarse classifier: "spawned" | "transitioned" |
 * "completed".
 */
export interface TaskChildLifecycleEvent__Output {
  'childTaskId': (string);
  'childStatus': (_aether_v1_TaskStatus__Output);
  'lifecycle': (string);
}
