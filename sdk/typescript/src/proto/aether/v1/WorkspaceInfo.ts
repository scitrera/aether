// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * WorkspaceInfo represents a workspace.
 * Contains workspace metadata and optional runtime statistics.
 */
export interface WorkspaceInfo {
  /**
   * Unique workspace identifier
   */
  'workspaceId'?: (string);
  /**
   * Human-readable display name
   */
  'displayName'?: (string);
  /**
   * Workspace description
   */
  'description'?: (string);
  /**
   * Tenant/organization ID
   */
  'tenantId'?: (string);
  /**
   * Unix timestamp when workspace was created
   */
  'createdAt'?: (number | string | Long);
  /**
   * Unix timestamp when workspace was last updated
   */
  'updatedAt'?: (number | string | Long);
  /**
   * Custom workspace metadata
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Runtime statistics (populated by GET and LIST operations)
   */
  'activeAgents'?: (number);
  /**
   * Number of running tasks
   */
  'activeTasks'?: (number);
  /**
   * Number of currently connected users
   */
  'activeUsers'?: (number);
  /**
   * Total messages processed in this workspace
   */
  'totalMessages'?: (number | string | Long);
}

/**
 * WorkspaceInfo represents a workspace.
 * Contains workspace metadata and optional runtime statistics.
 */
export interface WorkspaceInfo__Output {
  /**
   * Unique workspace identifier
   */
  'workspaceId': (string);
  /**
   * Human-readable display name
   */
  'displayName': (string);
  /**
   * Workspace description
   */
  'description': (string);
  /**
   * Tenant/organization ID
   */
  'tenantId': (string);
  /**
   * Unix timestamp when workspace was created
   */
  'createdAt': (string);
  /**
   * Unix timestamp when workspace was last updated
   */
  'updatedAt': (string);
  /**
   * Custom workspace metadata
   */
  'metadata': ({[key: string]: string});
  /**
   * Runtime statistics (populated by GET and LIST operations)
   */
  'activeAgents': (number);
  /**
   * Number of running tasks
   */
  'activeTasks': (number);
  /**
   * Number of currently connected users
   */
  'activeUsers': (number);
  /**
   * Total messages processed in this workspace
   */
  'totalMessages': (string);
}
