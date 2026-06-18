// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLRoleAssignmentInfo represents a single role-assignment edge.
 */
export interface ACLRoleAssignmentInfo {
  'roleName'?: (string);
  'assigneeType'?: (string);
  'assigneeId'?: (string);
  'grantedBy'?: (string);
  'grantedAt'?: (number | string | Long);
  'expiresAt'?: (number | string | Long);
}

/**
 * ACLRoleAssignmentInfo represents a single role-assignment edge.
 */
export interface ACLRoleAssignmentInfo__Output {
  'roleName': (string);
  'assigneeType': (string);
  'assigneeId': (string);
  'grantedBy': (string);
  'grantedAt': (string);
  'expiresAt': (string);
}
