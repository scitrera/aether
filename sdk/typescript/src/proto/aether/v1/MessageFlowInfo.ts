// Original file: aether.proto

import type { FlowNode as _aether_v1_FlowNode, FlowNode__Output as _aether_v1_FlowNode__Output } from '../../aether/v1/FlowNode';
import type { FlowEdge as _aether_v1_FlowEdge, FlowEdge__Output as _aether_v1_FlowEdge__Output } from '../../aether/v1/FlowEdge';
import type { Long } from '@grpc/proto-loader';

/**
 * MessageFlowInfo represents the message flow graph for a workspace.
 * Shows connected entities (nodes) and message routing paths (edges).
 * Used by GET_MESSAGE_FLOW operation to visualize workspace topology.
 */
export interface MessageFlowInfo {
  /**
   * Workspace this flow belongs to
   */
  'workspaceId'?: (string);
  /**
   * All entities in the workspace
   */
  'nodes'?: (_aether_v1_FlowNode)[];
  /**
   * Message routing paths between entities
   */
  'edges'?: (_aether_v1_FlowEdge)[];
  /**
   * Unix timestamp when flow was last updated
   */
  'updatedAt'?: (number | string | Long);
}

/**
 * MessageFlowInfo represents the message flow graph for a workspace.
 * Shows connected entities (nodes) and message routing paths (edges).
 * Used by GET_MESSAGE_FLOW operation to visualize workspace topology.
 */
export interface MessageFlowInfo__Output {
  /**
   * Workspace this flow belongs to
   */
  'workspaceId': (string);
  /**
   * All entities in the workspace
   */
  'nodes': (_aether_v1_FlowNode__Output)[];
  /**
   * Message routing paths between entities
   */
  'edges': (_aether_v1_FlowEdge__Output)[];
  /**
   * Unix timestamp when flow was last updated
   */
  'updatedAt': (string);
}
