// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * GatewayInfo provides basic gateway information.
 * Matches the GatewayInfo struct in state_provider.go.
 */
export interface GatewayInfo {
  /**
   * Unique gateway identifier
   */
  'gatewayId'?: (string);
  /**
   * Gateway version
   */
  'version'?: (string);
  /**
   * Unix timestamp when gateway started
   */
  'startedAt'?: (number | string | Long);
  /**
   * Human-readable uptime string
   */
  'uptime'?: (string);
  /**
   * Go runtime version
   */
  'goVersion'?: (string);
  /**
   * Current number of goroutines
   */
  'numGoroutines'?: (number);
  /**
   * Allocated memory in MB
   */
  'memoryAllocMb'?: (number | string);
  /**
   * Number of active connections
   */
  'numConnections'?: (number);
}

/**
 * GatewayInfo provides basic gateway information.
 * Matches the GatewayInfo struct in state_provider.go.
 */
export interface GatewayInfo__Output {
  /**
   * Unique gateway identifier
   */
  'gatewayId': (string);
  /**
   * Gateway version
   */
  'version': (string);
  /**
   * Unix timestamp when gateway started
   */
  'startedAt': (string);
  /**
   * Human-readable uptime string
   */
  'uptime': (string);
  /**
   * Go runtime version
   */
  'goVersion': (string);
  /**
   * Current number of goroutines
   */
  'numGoroutines': (number);
  /**
   * Allocated memory in MB
   */
  'memoryAllocMb': (number);
  /**
   * Number of active connections
   */
  'numConnections': (number);
}
