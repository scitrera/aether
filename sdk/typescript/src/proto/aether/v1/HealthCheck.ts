// Original file: aether.proto

import type { HealthCheckStatus as _aether_v1_HealthCheckStatus, HealthCheckStatus__Output as _aether_v1_HealthCheckStatus__Output } from '../../aether/v1/HealthCheckStatus';

/**
 * HealthCheck represents a single component health check.
 * Matches the HealthCheck struct in state_provider.go.
 */
export interface HealthCheck {
  /**
   * Component health check status
   */
  'status'?: (_aether_v1_HealthCheckStatus);
  /**
   * Response latency (e.g., "1.5ms")
   */
  'latency'?: (string);
  /**
   * Error message if status is "error"
   */
  'error'?: (string);
}

/**
 * HealthCheck represents a single component health check.
 * Matches the HealthCheck struct in state_provider.go.
 */
export interface HealthCheck__Output {
  /**
   * Component health check status
   */
  'status': (_aether_v1_HealthCheckStatus__Output);
  /**
   * Response latency (e.g., "1.5ms")
   */
  'latency': (string);
  /**
   * Error message if status is "error"
   */
  'error': (string);
}
