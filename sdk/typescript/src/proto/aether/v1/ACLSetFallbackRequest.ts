// Original file: aether.proto


/**
 * ACLSetFallbackRequest contains data for setting a fallback policy.
 * Matches SetFallbackPolicy parameters in internal/acl/service.go.
 */
export interface ACLSetFallbackRequest {
  /**
   * Rule category (e.g., "user_workspace", "agent_workspace")
   */
  'ruleCategory'?: (string);
  /**
   * Default access level when no rule matches
   */
  'fallbackAccessLevel'?: (number);
  /**
   * Identity of who is updating the policy
   */
  'updatedBy'?: (string);
}

/**
 * ACLSetFallbackRequest contains data for setting a fallback policy.
 * Matches SetFallbackPolicy parameters in internal/acl/service.go.
 */
export interface ACLSetFallbackRequest__Output {
  /**
   * Rule category (e.g., "user_workspace", "agent_workspace")
   */
  'ruleCategory': (string);
  /**
   * Default access level when no rule matches
   */
  'fallbackAccessLevel': (number);
  /**
   * Identity of who is updating the policy
   */
  'updatedBy': (string);
}
