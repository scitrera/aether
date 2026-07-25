// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLRoleAssignmentRequest contains data for assigning a role.
 */
export interface ACLRoleAssignmentRequest {
  /**
   * principal type or "group"
   */
  'assigneeType'?: (string);
  'assigneeId'?: (string);
  'grantedBy'?: (string);
  /**
   * Unix timestamp, 0 = no expiration
   */
  'expiresAt'?: (number | string | Long);
}

/**
 * ACLRoleAssignmentRequest contains data for assigning a role.
 */
export interface ACLRoleAssignmentRequest__Output {
  /**
   * principal type or "group"
   */
  'assigneeType': (string);
  'assigneeId': (string);
  'grantedBy': (string);
  /**
   * Unix timestamp, 0 = no expiration
   */
  'expiresAt': (string);
}
