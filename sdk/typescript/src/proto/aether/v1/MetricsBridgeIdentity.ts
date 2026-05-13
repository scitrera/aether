// Original file: aether.proto


/**
 * MetricsBridgeIdentity identifies a metrics bridge principal.
 * Replaces the previous bool sentinel to allow future extensibility.
 */
export interface MetricsBridgeIdentity {
  /**
   * Optional instance identifier for multiple metrics bridges
   */
  'instanceId'?: (string);
}

/**
 * MetricsBridgeIdentity identifies a metrics bridge principal.
 * Replaces the previous bool sentinel to allow future extensibility.
 */
export interface MetricsBridgeIdentity__Output {
  /**
   * Optional instance identifier for multiple metrics bridges
   */
  'instanceId': (string);
}
