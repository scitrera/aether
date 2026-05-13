// Original file: aether.proto


/**
 * AuthorityGrantListRequest filters grants visible to the actor.
 */
export interface AuthorityGrantListRequest {
  /**
   * Optional filter
   */
  'audienceType'?: (string);
  /**
   * Optional filter
   */
  'audienceId'?: (string);
  'includeRevoked'?: (boolean);
  /**
   * 0 = default 100
   */
  'limit'?: (number);
  'offset'?: (number);
}

/**
 * AuthorityGrantListRequest filters grants visible to the actor.
 */
export interface AuthorityGrantListRequest__Output {
  /**
   * Optional filter
   */
  'audienceType': (string);
  /**
   * Optional filter
   */
  'audienceId': (string);
  'includeRevoked': (boolean);
  /**
   * 0 = default 100
   */
  'limit': (number);
  'offset': (number);
}
