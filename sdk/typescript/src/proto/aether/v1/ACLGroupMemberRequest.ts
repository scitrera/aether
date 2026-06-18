// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLGroupMemberRequest contains data for adding a member to a group.
 */
export interface ACLGroupMemberRequest {
  /**
   * principal type or "group" (nesting)
   */
  'memberType'?: (string);
  'memberId'?: (string);
  'grantedBy'?: (string);
  /**
   * Unix timestamp, 0 = no expiration
   */
  'expiresAt'?: (number | string | Long);
}

/**
 * ACLGroupMemberRequest contains data for adding a member to a group.
 */
export interface ACLGroupMemberRequest__Output {
  /**
   * principal type or "group" (nesting)
   */
  'memberType': (string);
  'memberId': (string);
  'grantedBy': (string);
  /**
   * Unix timestamp, 0 = no expiration
   */
  'expiresAt': (string);
}
