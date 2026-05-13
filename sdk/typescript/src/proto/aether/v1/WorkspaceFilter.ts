// Original file: aether.proto


/**
 * WorkspaceFilter specifies filtering parameters for listing workspaces.
 * Matches REST API query parameters.
 */
export interface WorkspaceFilter {
  /**
   * Filter by tenant ID
   */
  'tenantId'?: (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit'?: (number);
  /**
   * Offset for pagination
   */
  'offset'?: (number);
}

/**
 * WorkspaceFilter specifies filtering parameters for listing workspaces.
 * Matches REST API query parameters.
 */
export interface WorkspaceFilter__Output {
  /**
   * Filter by tenant ID
   */
  'tenantId': (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
}
