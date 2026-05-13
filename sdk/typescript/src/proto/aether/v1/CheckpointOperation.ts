// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

// Original file: aether.proto

export const _aether_v1_CheckpointOperation_OpType = {
  /**
   * Save checkpoint data
   */
  SAVE: 'SAVE',
  /**
   * Load checkpoint data
   */
  LOAD: 'LOAD',
  /**
   * Delete checkpoint
   */
  DELETE: 'DELETE',
  /**
   * List available checkpoints
   */
  LIST: 'LIST',
} as const;

export type _aether_v1_CheckpointOperation_OpType =
  /**
   * Save checkpoint data
   */
  | 'SAVE'
  | 0
  /**
   * Load checkpoint data
   */
  | 'LOAD'
  | 1
  /**
   * Delete checkpoint
   */
  | 'DELETE'
  | 2
  /**
   * List available checkpoints
   */
  | 'LIST'
  | 3

export type _aether_v1_CheckpointOperation_OpType__Output = typeof _aether_v1_CheckpointOperation_OpType[keyof typeof _aether_v1_CheckpointOperation_OpType]

/**
 * CheckpointOperation allows agents/tasks to save and load custom state.
 * This is separate from message offset tracking (handled automatically by RabbitMQ).
 * Use checkpoints to persist application-specific state that needs to survive restarts.
 */
export interface CheckpointOperation {
  'op'?: (_aether_v1_CheckpointOperation_OpType);
  /**
   * Checkpoint key - allows multiple named checkpoints per identity.
   * If empty, uses "default" as the key.
   */
  'key'?: (string);
  /**
   * Checkpoint data (for SAVE operation)
   */
  'data'?: (Buffer | Uint8Array | string);
  /**
   * TTL in seconds for the checkpoint:
   * -1 = use server default TTL (recommended for most use cases)
   * 0 = no expiration (checkpoint persists until explicitly deleted)
   * >0 = specific TTL in seconds
   */
  'ttl'?: (number | string | Long);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * CheckpointOperation allows agents/tasks to save and load custom state.
 * This is separate from message offset tracking (handled automatically by RabbitMQ).
 * Use checkpoints to persist application-specific state that needs to survive restarts.
 */
export interface CheckpointOperation__Output {
  'op': (_aether_v1_CheckpointOperation_OpType__Output);
  /**
   * Checkpoint key - allows multiple named checkpoints per identity.
   * If empty, uses "default" as the key.
   */
  'key': (string);
  /**
   * Checkpoint data (for SAVE operation)
   */
  'data': (Buffer);
  /**
   * TTL in seconds for the checkpoint:
   * -1 = use server default TTL (recommended for most use cases)
   * 0 = no expiration (checkpoint persists until explicitly deleted)
   * >0 = specific TTL in seconds
   */
  'ttl': (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
