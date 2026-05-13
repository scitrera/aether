// Original file: aether.proto

/**
 * HealthStatus enumerates the overall health states of the gateway.
 */
export const HealthStatus = {
  HEALTH_STATUS_UNSPECIFIED: 'HEALTH_STATUS_UNSPECIFIED',
  HEALTH_STATUS_HEALTHY: 'HEALTH_STATUS_HEALTHY',
  HEALTH_STATUS_DEGRADED: 'HEALTH_STATUS_DEGRADED',
  HEALTH_STATUS_UNHEALTHY: 'HEALTH_STATUS_UNHEALTHY',
} as const;

/**
 * HealthStatus enumerates the overall health states of the gateway.
 */
export type HealthStatus =
  | 'HEALTH_STATUS_UNSPECIFIED'
  | 0
  | 'HEALTH_STATUS_HEALTHY'
  | 1
  | 'HEALTH_STATUS_DEGRADED'
  | 2
  | 'HEALTH_STATUS_UNHEALTHY'
  | 3

/**
 * HealthStatus enumerates the overall health states of the gateway.
 */
export type HealthStatus__Output = typeof HealthStatus[keyof typeof HealthStatus]
