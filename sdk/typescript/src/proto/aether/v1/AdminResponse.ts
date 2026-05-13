// Original file: aether.proto

import type { HealthInfo as _aether_v1_HealthInfo, HealthInfo__Output as _aether_v1_HealthInfo__Output } from '../../aether/v1/HealthInfo';
import type { GatewayInfo as _aether_v1_GatewayInfo, GatewayInfo__Output as _aether_v1_GatewayInfo__Output } from '../../aether/v1/GatewayInfo';
import type { GatewayStats as _aether_v1_GatewayStats, GatewayStats__Output as _aether_v1_GatewayStats__Output } from '../../aether/v1/GatewayStats';
import type { ConnectionInfo as _aether_v1_ConnectionInfo, ConnectionInfo__Output as _aether_v1_ConnectionInfo__Output } from '../../aether/v1/ConnectionInfo';

/**
 * AdminResponse is sent in response to AdminQuery operations.
 * Contains data for health, info, stats, or connection queries based on the
 * operation type that was requested.
 */
export interface AdminResponse {
  'success'?: (boolean);
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * For GET_HEALTH operation
   */
  'health'?: (_aether_v1_HealthInfo | null);
  /**
   * For GET_INFO operation
   */
  'info'?: (_aether_v1_GatewayInfo | null);
  /**
   * For GET_STATS operation
   */
  'stats'?: (_aether_v1_GatewayStats | null);
  /**
   * For GET_CONNECTION operation (single connection)
   */
  'connection'?: (_aether_v1_ConnectionInfo | null);
  /**
   * For LIST_CONNECTIONS operation (multiple connections)
   */
  'connections'?: (_aether_v1_ConnectionInfo)[];
  /**
   * For LIST_CONNECTIONS: total count (may differ from returned if paginated)
   */
  'totalCount'?: (number);
  /**
   * Echoed from the originating AdminQuery for correlation
   */
  'requestId'?: (string);
}

/**
 * AdminResponse is sent in response to AdminQuery operations.
 * Contains data for health, info, stats, or connection queries based on the
 * operation type that was requested.
 */
export interface AdminResponse__Output {
  'success': (boolean);
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * For GET_HEALTH operation
   */
  'health': (_aether_v1_HealthInfo__Output | null);
  /**
   * For GET_INFO operation
   */
  'info': (_aether_v1_GatewayInfo__Output | null);
  /**
   * For GET_STATS operation
   */
  'stats': (_aether_v1_GatewayStats__Output | null);
  /**
   * For GET_CONNECTION operation (single connection)
   */
  'connection': (_aether_v1_ConnectionInfo__Output | null);
  /**
   * For LIST_CONNECTIONS operation (multiple connections)
   */
  'connections': (_aether_v1_ConnectionInfo__Output)[];
  /**
   * For LIST_CONNECTIONS: total count (may differ from returned if paginated)
   */
  'totalCount': (number);
  /**
   * Echoed from the originating AdminQuery for correlation
   */
  'requestId': (string);
}
