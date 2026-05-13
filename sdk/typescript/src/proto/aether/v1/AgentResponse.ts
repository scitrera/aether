// Original file: aether.proto

import type { AgentRegistrationInfo as _aether_v1_AgentRegistrationInfo, AgentRegistrationInfo__Output as _aether_v1_AgentRegistrationInfo__Output } from '../../aether/v1/AgentRegistrationInfo';
import type { OrchestratorInfo as _aether_v1_OrchestratorInfo, OrchestratorInfo__Output as _aether_v1_OrchestratorInfo__Output } from '../../aether/v1/OrchestratorInfo';
import type { AgentLaunchResult as _aether_v1_AgentLaunchResult, AgentLaunchResult__Output as _aether_v1_AgentLaunchResult__Output } from '../../aether/v1/AgentLaunchResult';

/**
 * AgentResponse is sent in response to AgentOperation.
 * Contains agent data based on the operation type that was requested.
 */
export interface AgentResponse {
  'success'?: (boolean);
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * Human-readable result message (for REGISTER, UPDATE, DELETE operations)
   */
  'message'?: (string);
  /**
   * For GET operation (single agent registration)
   */
  'agent'?: (_aether_v1_AgentRegistrationInfo | null);
  /**
   * For LIST operation (multiple agent registrations)
   */
  'agents'?: (_aether_v1_AgentRegistrationInfo)[];
  /**
   * For LIST: total count (may differ from returned if paginated)
   */
  'totalCount'?: (number);
  /**
   * For LIST_ORCHESTRATORS operation
   */
  'orchestrators'?: (_aether_v1_OrchestratorInfo)[];
  /**
   * For LAUNCH operation: the result of launching an agent
   */
  'launchResult'?: (_aether_v1_AgentLaunchResult | null);
  /**
   * Echoed from the originating AgentOperation for correlation
   */
  'requestId'?: (string);
}

/**
 * AgentResponse is sent in response to AgentOperation.
 * Contains agent data based on the operation type that was requested.
 */
export interface AgentResponse__Output {
  'success': (boolean);
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * Human-readable result message (for REGISTER, UPDATE, DELETE operations)
   */
  'message': (string);
  /**
   * For GET operation (single agent registration)
   */
  'agent': (_aether_v1_AgentRegistrationInfo__Output | null);
  /**
   * For LIST operation (multiple agent registrations)
   */
  'agents': (_aether_v1_AgentRegistrationInfo__Output)[];
  /**
   * For LIST: total count (may differ from returned if paginated)
   */
  'totalCount': (number);
  /**
   * For LIST_ORCHESTRATORS operation
   */
  'orchestrators': (_aether_v1_OrchestratorInfo__Output)[];
  /**
   * For LAUNCH operation: the result of launching an agent
   */
  'launchResult': (_aether_v1_AgentLaunchResult__Output | null);
  /**
   * Echoed from the originating AgentOperation for correlation
   */
  'requestId': (string);
}
