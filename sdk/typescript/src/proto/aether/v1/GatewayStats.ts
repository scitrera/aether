// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * GatewayStats provides gateway-wide statistics.
 * Matches the GatewayStats struct in state_provider.go.
 */
export interface GatewayStats {
  /**
   * Connection counts by type
   */
  'agentConnections'?: (number);
  'taskConnections'?: (number);
  'userConnections'?: (number);
  'orchestratorConnections'?: (number);
  'workflowEngineConnected'?: (boolean);
  'metricsBridgeConnected'?: (boolean);
  /**
   * Task statistics
   */
  'totalTasks'?: (number);
  'pendingTasks'?: (number);
  'runningTasks'?: (number);
  'completedTasks'?: (number);
  'failedTasks'?: (number);
  /**
   * Message statistics
   */
  'messagesPerSecond'?: (number | string);
  'totalMessages'?: (number | string | Long);
  /**
   * Timer statistics
   */
  'activeTimers'?: (number);
  'pendingTimers'?: (number);
}

/**
 * GatewayStats provides gateway-wide statistics.
 * Matches the GatewayStats struct in state_provider.go.
 */
export interface GatewayStats__Output {
  /**
   * Connection counts by type
   */
  'agentConnections': (number);
  'taskConnections': (number);
  'userConnections': (number);
  'orchestratorConnections': (number);
  'workflowEngineConnected': (boolean);
  'metricsBridgeConnected': (boolean);
  /**
   * Task statistics
   */
  'totalTasks': (number);
  'pendingTasks': (number);
  'runningTasks': (number);
  'completedTasks': (number);
  'failedTasks': (number);
  /**
   * Message statistics
   */
  'messagesPerSecond': (number);
  'totalMessages': (string);
  /**
   * Timer statistics
   */
  'activeTimers': (number);
  'pendingTimers': (number);
}
