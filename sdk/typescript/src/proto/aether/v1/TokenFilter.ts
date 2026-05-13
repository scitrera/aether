// Original file: aether.proto


/**
 * TokenFilter specifies filtering parameters for listing tokens.
 */
export interface TokenFilter {
  /**
   * Maximum number of results (0 = default 100)
   */
  'limit'?: (number);
  /**
   * Offset for pagination
   */
  'offset'?: (number);
  /**
   * If false (default), excludes revoked tokens
   */
  'includeRevoked'?: (boolean);
}

/**
 * TokenFilter specifies filtering parameters for listing tokens.
 */
export interface TokenFilter__Output {
  /**
   * Maximum number of results (0 = default 100)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
  /**
   * If false (default), excludes revoked tokens
   */
  'includeRevoked': (boolean);
}
