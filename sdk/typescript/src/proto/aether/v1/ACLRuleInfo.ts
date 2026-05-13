// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLRuleInfo represents an access control rule.
 * Matches the ACLRule struct in internal/acl/types.go.
 */
export interface ACLRuleInfo {
  /**
   * Unique rule identifier
   */
  'ruleId'?: (string);
  /**
   * Type of principal (agent, task, user, wildcard)
   */
  'principalType'?: (string);
  /**
   * Identity of principal or wildcard pattern
   */
  'principalId'?: (string);
  /**
   * Type of resource (workspace, agent, permission)
   */
  'resourceType'?: (string);
  /**
   * ID of resource
   */
  'resourceId'?: (string);
  /**
   * Permission level granted (0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN)
   */
  'accessLevel'?: (number);
  /**
   * Human-readable access level name
   */
  'accessLevelName'?: (string);
  /**
   * Who created this rule
   */
  'grantedBy'?: (string);
  /**
   * Unix timestamp when rule was created
   */
  'grantedAt'?: (number | string | Long);
  /**
   * Optional expiration time (Unix timestamp, 0 = no expiration)
   */
  'expiresAt'?: (number | string | Long);
  /**
   * Human-readable reason for this grant
   */
  'reason'?: (string);
}

/**
 * ACLRuleInfo represents an access control rule.
 * Matches the ACLRule struct in internal/acl/types.go.
 */
export interface ACLRuleInfo__Output {
  /**
   * Unique rule identifier
   */
  'ruleId': (string);
  /**
   * Type of principal (agent, task, user, wildcard)
   */
  'principalType': (string);
  /**
   * Identity of principal or wildcard pattern
   */
  'principalId': (string);
  /**
   * Type of resource (workspace, agent, permission)
   */
  'resourceType': (string);
  /**
   * ID of resource
   */
  'resourceId': (string);
  /**
   * Permission level granted (0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN)
   */
  'accessLevel': (number);
  /**
   * Human-readable access level name
   */
  'accessLevelName': (string);
  /**
   * Who created this rule
   */
  'grantedBy': (string);
  /**
   * Unix timestamp when rule was created
   */
  'grantedAt': (string);
  /**
   * Optional expiration time (Unix timestamp, 0 = no expiration)
   */
  'expiresAt': (string);
  /**
   * Human-readable reason for this grant
   */
  'reason': (string);
}
