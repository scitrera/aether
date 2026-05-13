// Original file: aether.proto


/**
 * TokenCreateRequest contains data for creating a new API token.
 */
export interface TokenCreateRequest {
  /**
   * Human-readable token name (required)
   */
  'name'?: (string);
  /**
   * Principal type this token authenticates as (required: "agent", "task", "user")
   */
  'principalType'?: (string);
  /**
   * Glob patterns for workspace access (default: ["*"])
   */
  'workspacePatterns'?: (string)[];
  /**
   * Token scopes (default: ["connect"])
   */
  'scopes'?: (string)[];
  /**
   * Token expiration in hours (0 = no expiration)
   */
  'expiresInHours'?: (number);
  /**
   * Identity of who is creating the token (default: "admin")
   */
  'createdBy'?: (string);
}

/**
 * TokenCreateRequest contains data for creating a new API token.
 */
export interface TokenCreateRequest__Output {
  /**
   * Human-readable token name (required)
   */
  'name': (string);
  /**
   * Principal type this token authenticates as (required: "agent", "task", "user")
   */
  'principalType': (string);
  /**
   * Glob patterns for workspace access (default: ["*"])
   */
  'workspacePatterns': (string)[];
  /**
   * Token scopes (default: ["connect"])
   */
  'scopes': (string)[];
  /**
   * Token expiration in hours (0 = no expiration)
   */
  'expiresInHours': (number);
  /**
   * Identity of who is creating the token (default: "admin")
   */
  'createdBy': (string);
}
