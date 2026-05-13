// Original file: aether.proto

import type { HealthStatus as _aether_v1_HealthStatus, HealthStatus__Output as _aether_v1_HealthStatus__Output } from '../../aether/v1/HealthStatus';
import type { HealthCheck as _aether_v1_HealthCheck, HealthCheck__Output as _aether_v1_HealthCheck__Output } from '../../aether/v1/HealthCheck';
import type { GatewayStats as _aether_v1_GatewayStats, GatewayStats__Output as _aether_v1_GatewayStats__Output } from '../../aether/v1/GatewayStats';
import type { Long } from '@grpc/proto-loader';

/**
 * HealthInfo represents the overall health status of the gateway.
 * Matches the HealthStatus struct in state_provider.go.
 */
export interface HealthInfo {
  /**
   * Overall health status
   */
  'status'?: (_aether_v1_HealthStatus);
  /**
   * Unix timestamp of health check
   */
  'timestamp'?: (number | string | Long);
  /**
   * Individual component health checks
   */
  'checks'?: ({[key: string]: _aether_v1_HealthCheck});
  /**
   * Optional gateway statistics
   */
  'stats'?: (_aether_v1_GatewayStats | null);
}

/**
 * HealthInfo represents the overall health status of the gateway.
 * Matches the HealthStatus struct in state_provider.go.
 */
export interface HealthInfo__Output {
  /**
   * Overall health status
   */
  'status': (_aether_v1_HealthStatus__Output);
  /**
   * Unix timestamp of health check
   */
  'timestamp': (string);
  /**
   * Individual component health checks
   */
  'checks': ({[key: string]: _aether_v1_HealthCheck__Output});
  /**
   * Optional gateway statistics
   */
  'stats': (_aether_v1_GatewayStats__Output | null);
}
