// Original file: aether.proto

import type { TaskInfo as _aether_v1_TaskInfo, TaskInfo__Output as _aether_v1_TaskInfo__Output } from '../../aether/v1/TaskInfo';

/**
 * TaskOperationResponse is sent in response to TaskOperation.
 */
export interface TaskOperationResponse {
  'success'?: (boolean);
  /**
   * Human-readable result message
   */
  'message'?: (string);
  /**
   * Error details if success is false
   */
  'error'?: (string);
  /**
   * Updated task information after the operation (if applicable)
   */
  'task'?: (_aether_v1_TaskInfo | null);
  /**
   * Echoed from the originating TaskOperation for correlation
   */
  'requestId'?: (string);
}

/**
 * TaskOperationResponse is sent in response to TaskOperation.
 */
export interface TaskOperationResponse__Output {
  'success': (boolean);
  /**
   * Human-readable result message
   */
  'message': (string);
  /**
   * Error details if success is false
   */
  'error': (string);
  /**
   * Updated task information after the operation (if applicable)
   */
  'task': (_aether_v1_TaskInfo__Output | null);
  /**
   * Echoed from the originating TaskOperation for correlation
   */
  'requestId': (string);
}
