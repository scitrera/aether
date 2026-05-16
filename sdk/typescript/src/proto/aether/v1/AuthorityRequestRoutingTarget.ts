// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';

/**
 * AuthorityRequestRoutingTarget tells the gateway which approvers to notify
 * for a given request. Exactly one of (principal, capability) is populated.
 */
export interface AuthorityRequestRoutingTarget {
  /**
   * Specific approver principal/role/group identity.
   * Examples: "user::alice", "role::ops-on-call", "ag::tenant::policy::v1".
   */
  'principal'?: (_aether_v1_PrincipalRef | null);
  /**
   * Or a capability-gate string: "capability/approve/<action>".
   * Any actor holding this gate via existing ACL CheckAccess may resolve.
   */
  'capability'?: (string);
}

/**
 * AuthorityRequestRoutingTarget tells the gateway which approvers to notify
 * for a given request. Exactly one of (principal, capability) is populated.
 */
export interface AuthorityRequestRoutingTarget__Output {
  /**
   * Specific approver principal/role/group identity.
   * Examples: "user::alice", "role::ops-on-call", "ag::tenant::policy::v1".
   */
  'principal': (_aether_v1_PrincipalRef__Output | null);
  /**
   * Or a capability-gate string: "capability/approve/<action>".
   * Any actor holding this gate via existing ACL CheckAccess may resolve.
   */
  'capability': (string);
}
