// Original file: aether.proto

import type { PrincipalType as _aether_v1_PrincipalType, PrincipalType__Output as _aether_v1_PrincipalType__Output } from '../../aether/v1/PrincipalType';

/**
 * FlowNode represents an entity in the message flow graph.
 * Can be an agent, task, user, orchestrator, workflow engine, or metrics bridge.
 */
export interface FlowNode {
  /**
   * Unique node identifier (session_id or entity id)
   */
  'id'?: (string);
  /**
   * Human-readable display name
   */
  'label'?: (string);
  /**
   * Entity type
   */
  'type'?: (_aether_v1_PrincipalType);
  /**
   * Connection status: "connected", "disconnected"
   */
  'status'?: (string);
  /**
   * Implementation identifier (for agents/tasks)
   */
  'implementation'?: (string);
  /**
   * Specifier/instance identifier
   */
  'specifier'?: (string);
  /**
   * Topic this entity subscribes to
   */
  'topic'?: (string);
}

/**
 * FlowNode represents an entity in the message flow graph.
 * Can be an agent, task, user, orchestrator, workflow engine, or metrics bridge.
 */
export interface FlowNode__Output {
  /**
   * Unique node identifier (session_id or entity id)
   */
  'id': (string);
  /**
   * Human-readable display name
   */
  'label': (string);
  /**
   * Entity type
   */
  'type': (_aether_v1_PrincipalType__Output);
  /**
   * Connection status: "connected", "disconnected"
   */
  'status': (string);
  /**
   * Implementation identifier (for agents/tasks)
   */
  'implementation': (string);
  /**
   * Specifier/instance identifier
   */
  'specifier': (string);
  /**
   * Topic this entity subscribes to
   */
  'topic': (string);
}
