// Original file: aether.proto

import type { ConnectionInfo as _aether_v1_ConnectionInfo, ConnectionInfo__Output as _aether_v1_ConnectionInfo__Output } from '../../aether/v1/ConnectionInfo';

/**
 * SessionOperationResponse is sent in response to SessionOperation.
 */
export interface SessionOperationResponse {
  'success'?: (boolean);
  /**
   * Human-readable result message (DISCONNECT)
   */
  'message'?: (string);
  /**
   * Error details if success is false
   */
  'error'?: (string);
  /**
   * Echoed from the originating SessionOperation for correlation
   */
  'requestId'?: (string);
  /**
   * For GET: the requested session
   */
  'connection'?: (_aether_v1_ConnectionInfo | null);
  /**
   * For LIST: matching sessions (already paginated per filter.limit/offset)
   */
  'connections'?: (_aether_v1_ConnectionInfo)[];
  /**
   * For LIST: total count of matching sessions before limit/offset
   */
  'totalCount'?: (number);
}

/**
 * SessionOperationResponse is sent in response to SessionOperation.
 */
export interface SessionOperationResponse__Output {
  'success': (boolean);
  /**
   * Human-readable result message (DISCONNECT)
   */
  'message': (string);
  /**
   * Error details if success is false
   */
  'error': (string);
  /**
   * Echoed from the originating SessionOperation for correlation
   */
  'requestId': (string);
  /**
   * For GET: the requested session
   */
  'connection': (_aether_v1_ConnectionInfo__Output | null);
  /**
   * For LIST: matching sessions (already paginated per filter.limit/offset)
   */
  'connections': (_aether_v1_ConnectionInfo__Output)[];
  /**
   * For LIST: total count of matching sessions before limit/offset
   */
  'totalCount': (number);
}
