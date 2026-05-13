// Original file: aether.proto


/**
 * WorkflowResponse is sent in response to WorkflowOperation.
 * Uses JSON-encoded bytes for response payloads to avoid duplicating
 * the workflow server's internal types in the proto definition.
 */
export interface WorkflowResponse {
  'success'?: (boolean);
  'error'?: (string);
  'message'?: (string);
  /**
   * JSON response payload (entity or list)
   */
  'data'?: (Buffer | Uint8Array | string);
  /**
   * For LIST pagination
   */
  'totalCount'?: (number);
  /**
   * Echoed correlation ID
   */
  'requestId'?: (string);
}

/**
 * WorkflowResponse is sent in response to WorkflowOperation.
 * Uses JSON-encoded bytes for response payloads to avoid duplicating
 * the workflow server's internal types in the proto definition.
 */
export interface WorkflowResponse__Output {
  'success': (boolean);
  'error': (string);
  'message': (string);
  /**
   * JSON response payload (entity or list)
   */
  'data': (Buffer);
  /**
   * For LIST pagination
   */
  'totalCount': (number);
  /**
   * Echoed correlation ID
   */
  'requestId': (string);
}
