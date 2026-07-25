// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLGroupMemberInfo represents a single group-membership edge.
 */
export interface ACLGroupMemberInfo {
  'groupName'?: (string);
  'memberType'?: (string);
  'memberId'?: (string);
  'grantedBy'?: (string);
  'grantedAt'?: (number | string | Long);
  'expiresAt'?: (number | string | Long);
}

/**
 * ACLGroupMemberInfo represents a single group-membership edge.
 */
export interface ACLGroupMemberInfo__Output {
  'groupName': (string);
  'memberType': (string);
  'memberId': (string);
  'grantedBy': (string);
  'grantedAt': (string);
  'expiresAt': (string);
}
