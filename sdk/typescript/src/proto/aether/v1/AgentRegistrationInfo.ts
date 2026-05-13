// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * AgentRegistrationInfo represents an agent registration.
 * Matches the AgentRegistrationInfo struct in state_provider.go.
 * Agent registrations define how to launch agent implementations via orchestrators.
 */
export interface AgentRegistrationInfo {
  /**
   * Unique implementation name (e.g., "data-processor", "code-reviewer")
   */
  'implementation'?: (string);
  /**
   * Profile name for orchestration (e.g., "kubernetes", "docker")
   */
  'orchestratorProfile'?: (string);
  /**
   * Human-readable description of the agent type
   */
  'description'?: (string);
  /**
   * Default launch parameters for the agent
   */
  'launchParams'?: ({[key: string]: string});
  /**
   * Unix timestamp when registration was created
   */
  'registeredAt'?: (number | string | Long);
  /**
   * Unix timestamp when registration was last updated
   */
  'updatedAt'?: (number | string | Long);
}

/**
 * AgentRegistrationInfo represents an agent registration.
 * Matches the AgentRegistrationInfo struct in state_provider.go.
 * Agent registrations define how to launch agent implementations via orchestrators.
 */
export interface AgentRegistrationInfo__Output {
  /**
   * Unique implementation name (e.g., "data-processor", "code-reviewer")
   */
  'implementation': (string);
  /**
   * Profile name for orchestration (e.g., "kubernetes", "docker")
   */
  'orchestratorProfile': (string);
  /**
   * Human-readable description of the agent type
   */
  'description': (string);
  /**
   * Default launch parameters for the agent
   */
  'launchParams': ({[key: string]: string});
  /**
   * Unix timestamp when registration was created
   */
  'registeredAt': (string);
  /**
   * Unix timestamp when registration was last updated
   */
  'updatedAt': (string);
}
