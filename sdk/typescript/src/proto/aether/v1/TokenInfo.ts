// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * TokenInfo represents an API token (excludes the token hash for security).
 */
export interface TokenInfo {
  /**
   * Unique token identifier
   */
  'id'?: (string);
  /**
   * Human-readable token name
   */
  'name'?: (string);
  /**
   * Principal type this token authenticates as
   */
  'principalType'?: (string);
  /**
   * Glob patterns for workspace access
   */
  'workspacePatterns'?: (string)[];
  /**
   * Token scopes
   */
  'scopes'?: (string)[];
  /**
   * Identity of who created the token
   */
  'createdBy'?: (string);
  /**
   * Expiration time (Unix timestamp, 0 = no expiration)
   */
  'expiresAt'?: (number | string | Long);
  /**
   * Last usage time (Unix timestamp, 0 = never used)
   */
  'lastUsedAt'?: (number | string | Long);
  /**
   * Whether the token has been revoked
   */
  'revoked'?: (boolean);
  /**
   * Revocation time (Unix timestamp, 0 = not revoked)
   */
  'revokedAt'?: (number | string | Long);
  /**
   * Creation time (Unix timestamp)
   */
  'createdAt'?: (number | string | Long);
  /**
   * Last update time (Unix timestamp)
   */
  'updatedAt'?: (number | string | Long);
}

/**
 * TokenInfo represents an API token (excludes the token hash for security).
 */
export interface TokenInfo__Output {
  /**
   * Unique token identifier
   */
  'id': (string);
  /**
   * Human-readable token name
   */
  'name': (string);
  /**
   * Principal type this token authenticates as
   */
  'principalType': (string);
  /**
   * Glob patterns for workspace access
   */
  'workspacePatterns': (string)[];
  /**
   * Token scopes
   */
  'scopes': (string)[];
  /**
   * Identity of who created the token
   */
  'createdBy': (string);
  /**
   * Expiration time (Unix timestamp, 0 = no expiration)
   */
  'expiresAt': (string);
  /**
   * Last usage time (Unix timestamp, 0 = never used)
   */
  'lastUsedAt': (string);
  /**
   * Whether the token has been revoked
   */
  'revoked': (boolean);
  /**
   * Revocation time (Unix timestamp, 0 = not revoked)
   */
  'revokedAt': (string);
  /**
   * Creation time (Unix timestamp)
   */
  'createdAt': (string);
  /**
   * Last update time (Unix timestamp)
   */
  'updatedAt': (string);
}
