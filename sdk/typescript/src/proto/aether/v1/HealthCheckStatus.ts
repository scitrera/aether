// Original file: aether.proto

/**
 * HealthCheckStatus enumerates the result of individual component health checks.
 */
export const HealthCheckStatus = {
  HEALTH_CHECK_STATUS_UNSPECIFIED: 'HEALTH_CHECK_STATUS_UNSPECIFIED',
  HEALTH_CHECK_STATUS_OK: 'HEALTH_CHECK_STATUS_OK',
  HEALTH_CHECK_STATUS_ERROR: 'HEALTH_CHECK_STATUS_ERROR',
} as const;

/**
 * HealthCheckStatus enumerates the result of individual component health checks.
 */
export type HealthCheckStatus =
  | 'HEALTH_CHECK_STATUS_UNSPECIFIED'
  | 0
  | 'HEALTH_CHECK_STATUS_OK'
  | 1
  | 'HEALTH_CHECK_STATUS_ERROR'
  | 2

/**
 * HealthCheckStatus enumerates the result of individual component health checks.
 */
export type HealthCheckStatus__Output = typeof HealthCheckStatus[keyof typeof HealthCheckStatus]
