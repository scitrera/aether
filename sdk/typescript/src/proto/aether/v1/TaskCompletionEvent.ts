// Original file: aether.proto

import type { TaskStatus as _aether_v1_TaskStatus, TaskStatus__Output as _aether_v1_TaskStatus__Output } from '../../aether/v1/TaskStatus';

/**
 * TaskCompletionEvent opts a task into "feed B": when it reaches a terminal
 * status the server publishes a domain event onto the event plane (event::*)
 * so a workflow join (or any rule) can gather over task completions without the
 * worker emitting its own event. The emitted payload carries
 * {task_id, status, workspace, correlation_id, metadata}.
 */
export interface TaskCompletionEvent {
  /**
   * enabled turns the feature on. When false (default) no completion event is
   * emitted (today's behavior).
   */
  'enabled'?: (boolean);
  /**
   * event_name is the event name to publish. Empty ⇒ derived as
   * "task.completed" / "task.failed" / "task.cancelled" from the terminal status.
   */
  'eventName'?: (string);
  /**
   * on_statuses restricts emission to these terminal statuses. Empty ⇒ all
   * terminal statuses (completed, failed, cancelled).
   */
  'onStatuses'?: (_aether_v1_TaskStatus)[];
}

/**
 * TaskCompletionEvent opts a task into "feed B": when it reaches a terminal
 * status the server publishes a domain event onto the event plane (event::*)
 * so a workflow join (or any rule) can gather over task completions without the
 * worker emitting its own event. The emitted payload carries
 * {task_id, status, workspace, correlation_id, metadata}.
 */
export interface TaskCompletionEvent__Output {
  /**
   * enabled turns the feature on. When false (default) no completion event is
   * emitted (today's behavior).
   */
  'enabled': (boolean);
  /**
   * event_name is the event name to publish. Empty ⇒ derived as
   * "task.completed" / "task.failed" / "task.cancelled" from the terminal status.
   */
  'eventName': (string);
  /**
   * on_statuses restricts emission to these terminal statuses. Empty ⇒ all
   * terminal statuses (completed, failed, cancelled).
   */
  'onStatuses': (_aether_v1_TaskStatus__Output)[];
}
