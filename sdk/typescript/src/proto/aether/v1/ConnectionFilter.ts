// Original file: aether.proto

import type { PrincipalType as _aether_v1_PrincipalType, PrincipalType__Output as _aether_v1_PrincipalType__Output } from '../../aether/v1/PrincipalType';

/**
 * ConnectionFilter specifies filtering parameters for listing connections.
 * Matches REST API query parameters: type, workspace, limit, offset.
 */
export interface ConnectionFilter {
  /**
   * Filter by connection type
   */
  'type'?: (_aether_v1_PrincipalType);
  /**
   * Filter by workspace
   */
  'workspace'?: (string);
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
 * ConnectionFilter specifies filtering parameters for listing connections.
 * Matches REST API query parameters: type, workspace, limit, offset.
 */
export interface ConnectionFilter__Output {
  /**
   * Filter by connection type
   */
  'type': (_aether_v1_PrincipalType__Output);
  /**
   * Filter by workspace
   */
  'workspace': (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
}
