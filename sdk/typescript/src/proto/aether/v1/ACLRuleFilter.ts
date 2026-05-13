// Original file: aether.proto


/**
 * ACLRuleFilter specifies filtering parameters for listing ACL rules.
 * Matches RuleFilter struct in internal/acl/service.go.
 */
export interface ACLRuleFilter {
  /**
   * Filter by principal type (e.g., "agent", "task", "user")
   */
  'principalType'?: (string);
  /**
   * Filter by principal ID
   */
  'principalId'?: (string);
  /**
   * Filter by resource type (e.g., "workspace", "agent")
   */
  'resourceType'?: (string);
  /**
   * Filter by resource ID
   */
  'resourceId'?: (string);
  /**
   * Maximum number of results (0 = no limit)
   */
  'limit'?: (number);
  /**
   * Offset for pagination
   */
  'offset'?: (number);
}

/**
 * ACLRuleFilter specifies filtering parameters for listing ACL rules.
 * Matches RuleFilter struct in internal/acl/service.go.
 */
export interface ACLRuleFilter__Output {
  /**
   * Filter by principal type (e.g., "agent", "task", "user")
   */
  'principalType': (string);
  /**
   * Filter by principal ID
   */
  'principalId': (string);
  /**
   * Filter by resource type (e.g., "workspace", "agent")
   */
  'resourceType': (string);
  /**
   * Filter by resource ID
   */
  'resourceId': (string);
  /**
   * Maximum number of results (0 = no limit)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
}
