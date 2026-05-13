// Original file: aether.proto

import type { ConnectionFilter as _aether_v1_ConnectionFilter, ConnectionFilter__Output as _aether_v1_ConnectionFilter__Output } from '../../aether/v1/ConnectionFilter';

// Original file: aether.proto

export const _aether_v1_AdminQuery_OpType = {
  /**
   * GET /api/health - Health check with component status
   */
  GET_HEALTH: 'GET_HEALTH',
  /**
   * GET /api/info - Gateway information
   */
  GET_INFO: 'GET_INFO',
  /**
   * GET /api/stats - Gateway-wide statistics
   */
  GET_STATS: 'GET_STATS',
  /**
   * GET /api/connections - List active connections
   */
  LIST_CONNECTIONS: 'LIST_CONNECTIONS',
  /**
   * GET /api/connections/{id} - Get specific connection
   */
  GET_CONNECTION: 'GET_CONNECTION',
} as const;

export type _aether_v1_AdminQuery_OpType =
  /**
   * GET /api/health - Health check with component status
   */
  | 'GET_HEALTH'
  | 0
  /**
   * GET /api/info - Gateway information
   */
  | 'GET_INFO'
  | 1
  /**
   * GET /api/stats - Gateway-wide statistics
   */
  | 'GET_STATS'
  | 2
  /**
   * GET /api/connections - List active connections
   */
  | 'LIST_CONNECTIONS'
  | 3
  /**
   * GET /api/connections/{id} - Get specific connection
   */
  | 'GET_CONNECTION'
  | 4

export type _aether_v1_AdminQuery_OpType__Output = typeof _aether_v1_AdminQuery_OpType[keyof typeof _aether_v1_AdminQuery_OpType]

/**
 * AdminQuery allows clients to query gateway health, info, stats, and connections
 * through the gRPC streaming interface. This provides feature parity with the
 * REST Admin API for read-only administrative queries.
 */
export interface AdminQuery {
  'op'?: (_aether_v1_AdminQuery_OpType);
  /**
   * For GET_CONNECTION: the session ID to retrieve
   */
  'sessionId'?: (string);
  /**
   * For LIST_CONNECTIONS: optional filter parameters
   */
  'filter'?: (_aether_v1_ConnectionFilter | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * AdminQuery allows clients to query gateway health, info, stats, and connections
 * through the gRPC streaming interface. This provides feature parity with the
 * REST Admin API for read-only administrative queries.
 */
export interface AdminQuery__Output {
  'op': (_aether_v1_AdminQuery_OpType__Output);
  /**
   * For GET_CONNECTION: the session ID to retrieve
   */
  'sessionId': (string);
  /**
   * For LIST_CONNECTIONS: optional filter parameters
   */
  'filter': (_aether_v1_ConnectionFilter__Output | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
