// Original file: aether.proto


/**
 * TaskProgressEvent is the task-scoped projection of a ProgressUpdate whose
 * task_id matches the subscribed task.
 */
export interface TaskProgressEvent {
  'state'?: (string);
  'progress'?: (number | string);
  'message'?: (string);
  'metadata'?: ({[key: string]: string});
}

/**
 * TaskProgressEvent is the task-scoped projection of a ProgressUpdate whose
 * task_id matches the subscribed task.
 */
export interface TaskProgressEvent__Output {
  'state': (string);
  'progress': (number);
  'message': (string);
  'metadata': ({[key: string]: string});
}
