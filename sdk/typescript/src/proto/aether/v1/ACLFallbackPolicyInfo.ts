// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLFallbackPolicyInfo represents a fallback policy for default access behavior.
 * Matches the FallbackPolicy struct in internal/acl/types.go.
 * When no explicit ACL rule matches, the fallback policy determines the default behavior.
 */
export interface ACLFallbackPolicyInfo {
  /**
   * Unique policy identifier
   */
  'policyId'?: (string);
  /**
   * Category (e.g., "user_workspace", "agent_workspace")
   */
  'ruleCategory'?: (string);
  /**
   * Default access level when no rule matches
   */
  'fallbackAccessLevel'?: (number);
  /**
   * Human-readable access level name
   */
  'fallbackAccessLevelName'?: (string);
  /**
   * Identity of who last updated this policy
   */
  'updatedBy'?: (string);
  /**
   * Unix timestamp when policy was last updated
   */
  'updatedAt'?: (number | string | Long);
}

/**
 * ACLFallbackPolicyInfo represents a fallback policy for default access behavior.
 * Matches the FallbackPolicy struct in internal/acl/types.go.
 * When no explicit ACL rule matches, the fallback policy determines the default behavior.
 */
export interface ACLFallbackPolicyInfo__Output {
  /**
   * Unique policy identifier
   */
  'policyId': (string);
  /**
   * Category (e.g., "user_workspace", "agent_workspace")
   */
  'ruleCategory': (string);
  /**
   * Default access level when no rule matches
   */
  'fallbackAccessLevel': (number);
  /**
   * Human-readable access level name
   */
  'fallbackAccessLevelName': (string);
  /**
   * Identity of who last updated this policy
   */
  'updatedBy': (string);
  /**
   * Unix timestamp when policy was last updated
   */
  'updatedAt': (string);
}
