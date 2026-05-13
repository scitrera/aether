// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLCleanupResult represents the result of a cleanup operation.
 * Used for CLEANUP_EXPIRED and CLEANUP_AUDIT_LOGS responses.
 */
export interface ACLCleanupResult {
  /**
   * Number of items deleted
   */
  'deletedCount'?: (number | string | Long);
  /**
   * Human-readable result message
   */
  'message'?: (string);
}

/**
 * ACLCleanupResult represents the result of a cleanup operation.
 * Used for CLEANUP_EXPIRED and CLEANUP_AUDIT_LOGS responses.
 */
export interface ACLCleanupResult__Output {
  /**
   * Number of items deleted
   */
  'deletedCount': (string);
  /**
   * Human-readable result message
   */
  'message': (string);
}
