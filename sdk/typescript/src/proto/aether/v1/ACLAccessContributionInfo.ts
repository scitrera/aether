// Original file: aether.proto


/**
 * ACLAccessContributionInfo is one rule that matched a principal or one of its
 * groups/roles for an explained resource.
 */
export interface ACLAccessContributionInfo {
  /**
   * granting subject, e.g. "role:wsadmin"
   */
  'subject'?: (string);
  'ruleId'?: (string);
  'accessLevel'?: (number);
  /**
   * matched object pattern, e.g. "workspace:prod" or "workspace:*"
   */
  'resource'?: (string);
  'expired'?: (boolean);
}

/**
 * ACLAccessContributionInfo is one rule that matched a principal or one of its
 * groups/roles for an explained resource.
 */
export interface ACLAccessContributionInfo__Output {
  /**
   * granting subject, e.g. "role:wsadmin"
   */
  'subject': (string);
  'ruleId': (string);
  'accessLevel': (number);
  /**
   * matched object pattern, e.g. "workspace:prod" or "workspace:*"
   */
  'resource': (string);
  'expired': (boolean);
}
