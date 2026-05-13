// Original file: aether.proto

import type { WorkspaceInfo as _aether_v1_WorkspaceInfo, WorkspaceInfo__Output as _aether_v1_WorkspaceInfo__Output } from '../../aether/v1/WorkspaceInfo';
import type { MessageFlowInfo as _aether_v1_MessageFlowInfo, MessageFlowInfo__Output as _aether_v1_MessageFlowInfo__Output } from '../../aether/v1/MessageFlowInfo';

/**
 * WorkspaceResponse is sent in response to WorkspaceOperation.
 * Contains workspace data based on the operation type that was requested.
 */
export interface WorkspaceResponse {
  'success'?: (boolean);
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * Human-readable result message (for CREATE, UPDATE, DELETE operations)
   */
  'message'?: (string);
  /**
   * For GET operation (single workspace)
   */
  'workspace'?: (_aether_v1_WorkspaceInfo | null);
  /**
   * For LIST operation (multiple workspaces)
   */
  'workspaces'?: (_aether_v1_WorkspaceInfo)[];
  /**
   * For LIST: total count (may differ from returned if paginated)
   */
  'totalCount'?: (number);
  /**
   * For GET_MESSAGE_FLOW operation
   */
  'messageFlow'?: (_aether_v1_MessageFlowInfo | null);
  /**
   * Echoed from the originating WorkspaceOperation for correlation
   */
  'requestId'?: (string);
}

/**
 * WorkspaceResponse is sent in response to WorkspaceOperation.
 * Contains workspace data based on the operation type that was requested.
 */
export interface WorkspaceResponse__Output {
  'success': (boolean);
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * Human-readable result message (for CREATE, UPDATE, DELETE operations)
   */
  'message': (string);
  /**
   * For GET operation (single workspace)
   */
  'workspace': (_aether_v1_WorkspaceInfo__Output | null);
  /**
   * For LIST operation (multiple workspaces)
   */
  'workspaces': (_aether_v1_WorkspaceInfo__Output)[];
  /**
   * For LIST: total count (may differ from returned if paginated)
   */
  'totalCount': (number);
  /**
   * For GET_MESSAGE_FLOW operation
   */
  'messageFlow': (_aether_v1_MessageFlowInfo__Output | null);
  /**
   * Echoed from the originating WorkspaceOperation for correlation
   */
  'requestId': (string);
}
