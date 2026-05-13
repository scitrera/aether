// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * OrchestratorInfo represents a connected orchestrator and its supported profiles.
 * Matches the OrchestratorProfileInfo struct in state_provider.go.
 */
export interface OrchestratorInfo {
  /**
   * Unique orchestrator identifier (topic format)
   */
  'orchestratorId'?: (string);
  /**
   * List of profiles this orchestrator supports
   */
  'profiles'?: (string)[];
  /**
   * Unix timestamp when orchestrator connected
   */
  'connectedAt'?: (number | string | Long);
}

/**
 * OrchestratorInfo represents a connected orchestrator and its supported profiles.
 * Matches the OrchestratorProfileInfo struct in state_provider.go.
 */
export interface OrchestratorInfo__Output {
  /**
   * Unique orchestrator identifier (topic format)
   */
  'orchestratorId': (string);
  /**
   * List of profiles this orchestrator supports
   */
  'profiles': (string)[];
  /**
   * Unix timestamp when orchestrator connected
   */
  'connectedAt': (string);
}
