// Original file: aether.proto

import type { AgentFilter as _aether_v1_AgentFilter, AgentFilter__Output as _aether_v1_AgentFilter__Output } from '../../aether/v1/AgentFilter';
import type { AgentRegistrationInfo as _aether_v1_AgentRegistrationInfo, AgentRegistrationInfo__Output as _aether_v1_AgentRegistrationInfo__Output } from '../../aether/v1/AgentRegistrationInfo';
import type { AgentLaunchParams as _aether_v1_AgentLaunchParams, AgentLaunchParams__Output as _aether_v1_AgentLaunchParams__Output } from '../../aether/v1/AgentLaunchParams';

// Original file: aether.proto

export const _aether_v1_AgentOperation_OpType = {
  /**
   * GET /api/agents - List all agent registrations
   */
  LIST: 'LIST',
  /**
   * GET /api/agents/{implementation} - Get a specific agent registration
   */
  GET: 'GET',
  /**
   * POST /api/agents - Register a new agent type
   */
  REGISTER: 'REGISTER',
  /**
   * PUT /api/agents/{implementation} - Update an existing agent registration
   */
  UPDATE: 'UPDATE',
  /**
   * DELETE /api/agents/{implementation} - Delete an agent registration
   */
  DELETE: 'DELETE',
  /**
   * POST /api/agents/{implementation}/launch - Launch an agent via orchestration
   */
  LAUNCH: 'LAUNCH',
  /**
   * GET /api/orchestrators - List connected orchestrators with their profiles
   */
  LIST_ORCHESTRATORS: 'LIST_ORCHESTRATORS',
} as const;

export type _aether_v1_AgentOperation_OpType =
  /**
   * GET /api/agents - List all agent registrations
   */
  | 'LIST'
  | 0
  /**
   * GET /api/agents/{implementation} - Get a specific agent registration
   */
  | 'GET'
  | 1
  /**
   * POST /api/agents - Register a new agent type
   */
  | 'REGISTER'
  | 2
  /**
   * PUT /api/agents/{implementation} - Update an existing agent registration
   */
  | 'UPDATE'
  | 3
  /**
   * DELETE /api/agents/{implementation} - Delete an agent registration
   */
  | 'DELETE'
  | 4
  /**
   * POST /api/agents/{implementation}/launch - Launch an agent via orchestration
   */
  | 'LAUNCH'
  | 5
  /**
   * GET /api/orchestrators - List connected orchestrators with their profiles
   */
  | 'LIST_ORCHESTRATORS'
  | 6

export type _aether_v1_AgentOperation_OpType__Output = typeof _aether_v1_AgentOperation_OpType[keyof typeof _aether_v1_AgentOperation_OpType]

/**
 * AgentOperation allows clients to manage agent registrations and orchestration
 * through the gRPC streaming interface. This provides feature parity with the
 * REST Admin API for agent management.
 * REST equivalents:
 * - GET /api/agents → LIST
 * - POST /api/agents → REGISTER
 * - GET /api/agents/{implementation} → GET
 * - PUT /api/agents/{implementation} → UPDATE
 * - DELETE /api/agents/{implementation} → DELETE
 * - POST /api/agents/{implementation}/launch → LAUNCH
 * - GET /api/orchestrators → LIST_ORCHESTRATORS
 */
export interface AgentOperation {
  'op'?: (_aether_v1_AgentOperation_OpType);
  /**
   * For GET, UPDATE, DELETE, LAUNCH: the implementation name to operate on
   */
  'implementation'?: (string);
  /**
   * For LIST: optional filter parameters
   */
  'filter'?: (_aether_v1_AgentFilter | null);
  /**
   * For REGISTER and UPDATE: agent registration data
   */
  'agent'?: (_aether_v1_AgentRegistrationInfo | null);
  /**
   * For LAUNCH: launch request details
   */
  'launchParams'?: (_aether_v1_AgentLaunchParams | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * AgentOperation allows clients to manage agent registrations and orchestration
 * through the gRPC streaming interface. This provides feature parity with the
 * REST Admin API for agent management.
 * REST equivalents:
 * - GET /api/agents → LIST
 * - POST /api/agents → REGISTER
 * - GET /api/agents/{implementation} → GET
 * - PUT /api/agents/{implementation} → UPDATE
 * - DELETE /api/agents/{implementation} → DELETE
 * - POST /api/agents/{implementation}/launch → LAUNCH
 * - GET /api/orchestrators → LIST_ORCHESTRATORS
 */
export interface AgentOperation__Output {
  'op': (_aether_v1_AgentOperation_OpType__Output);
  /**
   * For GET, UPDATE, DELETE, LAUNCH: the implementation name to operate on
   */
  'implementation': (string);
  /**
   * For LIST: optional filter parameters
   */
  'filter': (_aether_v1_AgentFilter__Output | null);
  /**
   * For REGISTER and UPDATE: agent registration data
   */
  'agent': (_aether_v1_AgentRegistrationInfo__Output | null);
  /**
   * For LAUNCH: launch request details
   */
  'launchParams': (_aether_v1_AgentLaunchParams__Output | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
