// Original file: aether.proto

import type { WorkspaceFilter as _aether_v1_WorkspaceFilter, WorkspaceFilter__Output as _aether_v1_WorkspaceFilter__Output } from '../../aether/v1/WorkspaceFilter';
import type { WorkspaceInfo as _aether_v1_WorkspaceInfo, WorkspaceInfo__Output as _aether_v1_WorkspaceInfo__Output } from '../../aether/v1/WorkspaceInfo';

// Original file: aether.proto

export const _aether_v1_WorkspaceOperation_OpType = {
  /**
   * GET /api/workspaces - List all workspaces
   */
  LIST: 'LIST',
  /**
   * GET /api/workspaces/{workspace_id} - Get a specific workspace
   */
  GET: 'GET',
  /**
   * POST /api/workspaces - Create a new workspace
   */
  CREATE: 'CREATE',
  /**
   * PUT /api/workspaces/{workspace_id} - Update an existing workspace
   */
  UPDATE: 'UPDATE',
  /**
   * DELETE /api/workspaces/{workspace_id} - Delete a workspace
   */
  DELETE: 'DELETE',
  /**
   * GET /api/workspaces/{workspace_id}/message-flow - Get message flow graph
   */
  GET_MESSAGE_FLOW: 'GET_MESSAGE_FLOW',
} as const;

export type _aether_v1_WorkspaceOperation_OpType =
  /**
   * GET /api/workspaces - List all workspaces
   */
  | 'LIST'
  | 0
  /**
   * GET /api/workspaces/{workspace_id} - Get a specific workspace
   */
  | 'GET'
  | 1
  /**
   * POST /api/workspaces - Create a new workspace
   */
  | 'CREATE'
  | 2
  /**
   * PUT /api/workspaces/{workspace_id} - Update an existing workspace
   */
  | 'UPDATE'
  | 3
  /**
   * DELETE /api/workspaces/{workspace_id} - Delete a workspace
   */
  | 'DELETE'
  | 4
  /**
   * GET /api/workspaces/{workspace_id}/message-flow - Get message flow graph
   */
  | 'GET_MESSAGE_FLOW'
  | 5

export type _aether_v1_WorkspaceOperation_OpType__Output = typeof _aether_v1_WorkspaceOperation_OpType[keyof typeof _aether_v1_WorkspaceOperation_OpType]

/**
 * WorkspaceOperation allows clients to perform CRUD operations on workspaces
 * and query message flow information through the gRPC streaming interface.
 * This provides feature parity with the REST Admin API for workspace management.
 * REST equivalents:
 * - GET /api/workspaces → LIST
 * - POST /api/workspaces → CREATE
 * - GET /api/workspaces/{workspace_id} → GET
 * - PUT /api/workspaces/{workspace_id} → UPDATE
 * - DELETE /api/workspaces/{workspace_id} → DELETE
 * - GET /api/workspaces/{workspace_id}/message-flow → GET_MESSAGE_FLOW
 */
export interface WorkspaceOperation {
  'op'?: (_aether_v1_WorkspaceOperation_OpType);
  /**
   * For GET, UPDATE, DELETE, GET_MESSAGE_FLOW: the workspace ID to operate on
   */
  'workspaceId'?: (string);
  /**
   * For LIST: optional filter parameters
   */
  'filter'?: (_aether_v1_WorkspaceFilter | null);
  /**
   * For CREATE and UPDATE: workspace data
   */
  'workspace'?: (_aether_v1_WorkspaceInfo | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * WorkspaceOperation allows clients to perform CRUD operations on workspaces
 * and query message flow information through the gRPC streaming interface.
 * This provides feature parity with the REST Admin API for workspace management.
 * REST equivalents:
 * - GET /api/workspaces → LIST
 * - POST /api/workspaces → CREATE
 * - GET /api/workspaces/{workspace_id} → GET
 * - PUT /api/workspaces/{workspace_id} → UPDATE
 * - DELETE /api/workspaces/{workspace_id} → DELETE
 * - GET /api/workspaces/{workspace_id}/message-flow → GET_MESSAGE_FLOW
 */
export interface WorkspaceOperation__Output {
  'op': (_aether_v1_WorkspaceOperation_OpType__Output);
  /**
   * For GET, UPDATE, DELETE, GET_MESSAGE_FLOW: the workspace ID to operate on
   */
  'workspaceId': (string);
  /**
   * For LIST: optional filter parameters
   */
  'filter': (_aether_v1_WorkspaceFilter__Output | null);
  /**
   * For CREATE and UPDATE: workspace data
   */
  'workspace': (_aether_v1_WorkspaceInfo__Output | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
