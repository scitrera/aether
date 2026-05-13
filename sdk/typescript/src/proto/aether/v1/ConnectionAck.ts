// Original file: aether.proto


/**
 * ConnectionAck is sent immediately after successful connection.
 * Contains the session ID that the client should store for reconnection.
 */
export interface ConnectionAck {
  'sessionId'?: (string);
  /**
   * Indicates if this was a resumed session (client provided matching resume_session_id)
   */
  'resumed'?: (boolean);
  /**
   * For non-unique tasks, the server-assigned task instance ID used to construct
   * the task's topic address (ta.{workspace}.{impl}.{assigned_id}).
   * Empty for all other principal types.
   */
  'assignedId'?: (string);
}

/**
 * ConnectionAck is sent immediately after successful connection.
 * Contains the session ID that the client should store for reconnection.
 */
export interface ConnectionAck__Output {
  'sessionId': (string);
  /**
   * Indicates if this was a resumed session (client provided matching resume_session_id)
   */
  'resumed': (boolean);
  /**
   * For non-unique tasks, the server-assigned task instance ID used to construct
   * the task's topic address (ta.{workspace}.{impl}.{assigned_id}).
   * Empty for all other principal types.
   */
  'assignedId': (string);
}
