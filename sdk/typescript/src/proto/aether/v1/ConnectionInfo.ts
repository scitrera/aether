// Original file: aether.proto

import type { PrincipalType as _aether_v1_PrincipalType, PrincipalType__Output as _aether_v1_PrincipalType__Output } from '../../aether/v1/PrincipalType';
import type { Long } from '@grpc/proto-loader';

/**
 * ConnectionInfo represents an active gateway connection.
 * Matches the ConnectionInfo struct in state_provider.go.
 */
export interface ConnectionInfo {
  /**
   * Unique session identifier
   */
  'sessionId'?: (string);
  /**
   * Connection type
   */
  'type'?: (_aether_v1_PrincipalType);
  /**
   * Full identity string (topic format)
   */
  'identity'?: (string);
  /**
   * Workspace the connection is associated with
   */
  'workspace'?: (string);
  /**
   * Implementation identifier (for agents/tasks)
   */
  'implementation'?: (string);
  /**
   * Specifier/instance identifier
   */
  'specifier'?: (string);
  /**
   * Unix timestamp when connected
   */
  'connectedAt'?: (number | string | Long);
  /**
   * Human-readable duration string
   */
  'duration'?: (string);
  /**
   * Remote address of the client
   */
  'remoteAddr'?: (string);
  /**
   * Unix timestamp of last activity
   */
  'lastActivity'?: (number | string | Long);
}

/**
 * ConnectionInfo represents an active gateway connection.
 * Matches the ConnectionInfo struct in state_provider.go.
 */
export interface ConnectionInfo__Output {
  /**
   * Unique session identifier
   */
  'sessionId': (string);
  /**
   * Connection type
   */
  'type': (_aether_v1_PrincipalType__Output);
  /**
   * Full identity string (topic format)
   */
  'identity': (string);
  /**
   * Workspace the connection is associated with
   */
  'workspace': (string);
  /**
   * Implementation identifier (for agents/tasks)
   */
  'implementation': (string);
  /**
   * Specifier/instance identifier
   */
  'specifier': (string);
  /**
   * Unix timestamp when connected
   */
  'connectedAt': (string);
  /**
   * Human-readable duration string
   */
  'duration': (string);
  /**
   * Remote address of the client
   */
  'remoteAddr': (string);
  /**
   * Unix timestamp of last activity
   */
  'lastActivity': (string);
}
