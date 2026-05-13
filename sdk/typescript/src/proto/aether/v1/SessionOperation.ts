// Original file: aether.proto

import type { ConnectionFilter as _aether_v1_ConnectionFilter, ConnectionFilter__Output as _aether_v1_ConnectionFilter__Output } from '../../aether/v1/ConnectionFilter';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';

// Original file: aether.proto

export const _aether_v1_SessionOperation_OpType = {
  /**
   * DELETE /api/connections/{id} - Forcibly disconnect a session
   */
  DISCONNECT: 'DISCONNECT',
  /**
   * GET /api/connections - List active sessions
   */
  LIST: 'LIST',
  /**
   * GET /api/connections/{id} - Get a single session
   */
  GET: 'GET',
} as const;

export type _aether_v1_SessionOperation_OpType =
  /**
   * DELETE /api/connections/{id} - Forcibly disconnect a session
   */
  | 'DISCONNECT'
  | 0
  /**
   * GET /api/connections - List active sessions
   */
  | 'LIST'
  | 1
  /**
   * GET /api/connections/{id} - Get a single session
   */
  | 'GET'
  | 2

export type _aether_v1_SessionOperation_OpType__Output = typeof _aether_v1_SessionOperation_OpType[keyof typeof _aether_v1_SessionOperation_OpType]

/**
 * SessionOperation covers CRUD/management operations on active gateway
 * sessions (live streaming connections). Mirrors AgentOperation in shape:
 * the same op enum carries reads (LIST/GET) and mutations (DISCONNECT).
 * REST equivalents:
 * - GET /api/connections           → LIST
 * - GET /api/connections/{id}      → GET
 * - DELETE /api/connections/{id}   → DISCONNECT
 */
export interface SessionOperation {
  'op'?: (_aether_v1_SessionOperation_OpType);
  /**
   * For DISCONNECT and GET: the session ID to operate on
   */
  'sessionId'?: (string);
  /**
   * For DISCONNECT: optional reason
   */
  'reason'?: (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
  /**
   * For LIST: optional filter parameters. Reuses ConnectionFilter so REST
   * and gRPC paths share the same shape.
   */
  'filter'?: (_aether_v1_ConnectionFilter | null);
  /**
   * Optional on-behalf-of authority context. When set, the gateway runs the
   * admin ACL check against the subject (the user) rather than the actor
   * (the platform-server). Mirrors AuditQuery.authorization.
   */
  'authorization'?: (_aether_v1_AuthorizationContext | null);
}

/**
 * SessionOperation covers CRUD/management operations on active gateway
 * sessions (live streaming connections). Mirrors AgentOperation in shape:
 * the same op enum carries reads (LIST/GET) and mutations (DISCONNECT).
 * REST equivalents:
 * - GET /api/connections           → LIST
 * - GET /api/connections/{id}      → GET
 * - DELETE /api/connections/{id}   → DISCONNECT
 */
export interface SessionOperation__Output {
  'op': (_aether_v1_SessionOperation_OpType__Output);
  /**
   * For DISCONNECT and GET: the session ID to operate on
   */
  'sessionId': (string);
  /**
   * For DISCONNECT: optional reason
   */
  'reason': (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
  /**
   * For LIST: optional filter parameters. Reuses ConnectionFilter so REST
   * and gRPC paths share the same shape.
   */
  'filter': (_aether_v1_ConnectionFilter__Output | null);
  /**
   * Optional on-behalf-of authority context. When set, the gateway runs the
   * admin ACL check against the subject (the user) rather than the actor
   * (the platform-server). Mirrors AuditQuery.authorization.
   */
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
}
