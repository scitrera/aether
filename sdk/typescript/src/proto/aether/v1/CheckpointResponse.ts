// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * CheckpointResponse is sent in response to CheckpointOperation.
 */
export interface CheckpointResponse {
  'success'?: (boolean);
  /**
   * For LOAD operation: the checkpoint data
   */
  'data'?: (Buffer | Uint8Array | string);
  /**
   * For LIST operation: available checkpoint keys
   */
  'keys'?: (string)[];
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * Checkpoint metadata
   */
  'savedAt'?: (number | string | Long);
  /**
   * Echoed from the originating CheckpointOperation for correlation
   */
  'requestId'?: (string);
}

/**
 * CheckpointResponse is sent in response to CheckpointOperation.
 */
export interface CheckpointResponse__Output {
  'success': (boolean);
  /**
   * For LOAD operation: the checkpoint data
   */
  'data': (Buffer);
  /**
   * For LIST operation: available checkpoint keys
   */
  'keys': (string)[];
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * Checkpoint metadata
   */
  'savedAt': (string);
  /**
   * Echoed from the originating CheckpointOperation for correlation
   */
  'requestId': (string);
}
