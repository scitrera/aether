// Original file: aether.proto

import type { ACLAccessContributionInfo as _aether_v1_ACLAccessContributionInfo, ACLAccessContributionInfo__Output as _aether_v1_ACLAccessContributionInfo__Output } from '../../aether/v1/ACLAccessContributionInfo';

/**
 * ACLAccessExplanationInfo explains how a principal's effective access to a
 * resource is decided (EXPLAIN_ACCESS). The gateway records an "explain_access"
 * audit event attributing the call to the connected principal.
 */
export interface ACLAccessExplanationInfo {
  /**
   * self subject "type:id"
   */
  'principal'?: (string);
  /**
   * self + transitive groups/roles
   */
  'subjects'?: (string)[];
  'contributions'?: (_aether_v1_ACLAccessContributionInfo)[];
  'allowed'?: (boolean);
  /**
   * "ALLOW" / "DENY"
   */
  'decision'?: (string);
  'effectiveAccessLevel'?: (number);
  'fallbackApplied'?: (boolean);
  'reason'?: (string);
}

/**
 * ACLAccessExplanationInfo explains how a principal's effective access to a
 * resource is decided (EXPLAIN_ACCESS). The gateway records an "explain_access"
 * audit event attributing the call to the connected principal.
 */
export interface ACLAccessExplanationInfo__Output {
  /**
   * self subject "type:id"
   */
  'principal': (string);
  /**
   * self + transitive groups/roles
   */
  'subjects': (string)[];
  'contributions': (_aether_v1_ACLAccessContributionInfo__Output)[];
  'allowed': (boolean);
  /**
   * "ALLOW" / "DENY"
   */
  'decision': (string);
  'effectiveAccessLevel': (number);
  'fallbackApplied': (boolean);
  'reason': (string);
}
