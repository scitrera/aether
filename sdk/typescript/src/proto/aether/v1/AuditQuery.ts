// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { Long } from '@grpc/proto-loader';

/**
 * AuditQuery requests entries from the comprehensive audit log.
 * Requires system-level admin access or workspace-scoped read access.
 */
export interface AuditQuery {
  /**
   * Correlation ID for response matching
   */
  'requestId'?: (string);
  /**
   * Filter criteria (all optional — empty = no filter)
   */
  'startTime'?: (number | string | Long);
  /**
   * Unix timestamp (seconds) upper bound
   */
  'endTime'?: (number | string | Long);
  /**
   * Filter: connection, auth, message, kv, task, admin, acl
   */
  'eventType'?: (string);
  /**
   * Filter: agent, task, user, system
   */
  'actorType'?: (string);
  /**
   * Filter by specific actor identity
   */
  'actorId'?: (string);
  /**
   * Filter by resource type
   */
  'resourceType'?: (string);
  /**
   * Filter by resource ID
   */
  'resourceId'?: (string);
  /**
   * Filter by operation name
   */
  'operation'?: (string);
  /**
   * Filter by workspace
   */
  'workspace'?: (string);
  /**
   * If true, only return failed operations
   */
  'onlyFailures'?: (boolean);
  /**
   * Max results (default: 100, max: 500)
   */
  'limit'?: (number);
  /**
   * Pagination offset
   */
  'offset'?: (number);
  /**
   * Filter by subject principal type
   */
  'subjectType'?: (string);
  /**
   * Filter by subject identity
   */
  'subjectId'?: (string);
  /**
   * Filter: direct, on_behalf_of
   */
  'authorityMode'?: (string);
  /**
   * Filter by authority grant
   */
  'authorityGrantId'?: (string);
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  /**
   * Exclude rows whose actor_type is in this list (e.g. ["WorkflowEngine","Orchestrator"])
   */
  'excludeActorTypes'?: (string)[];
  /**
   * Exclude rows whose workspace is in this list (e.g. ["_system"])
   */
  'excludeWorkspaces'?: (string)[];
  /**
   * Exclude rows where actor_type=service AND authority_mode=direct (platform-server internal plumbing)
   */
  'excludeServiceDirect'?: (boolean);
}

/**
 * AuditQuery requests entries from the comprehensive audit log.
 * Requires system-level admin access or workspace-scoped read access.
 */
export interface AuditQuery__Output {
  /**
   * Correlation ID for response matching
   */
  'requestId': (string);
  /**
   * Filter criteria (all optional — empty = no filter)
   */
  'startTime': (string);
  /**
   * Unix timestamp (seconds) upper bound
   */
  'endTime': (string);
  /**
   * Filter: connection, auth, message, kv, task, admin, acl
   */
  'eventType': (string);
  /**
   * Filter: agent, task, user, system
   */
  'actorType': (string);
  /**
   * Filter by specific actor identity
   */
  'actorId': (string);
  /**
   * Filter by resource type
   */
  'resourceType': (string);
  /**
   * Filter by resource ID
   */
  'resourceId': (string);
  /**
   * Filter by operation name
   */
  'operation': (string);
  /**
   * Filter by workspace
   */
  'workspace': (string);
  /**
   * If true, only return failed operations
   */
  'onlyFailures': (boolean);
  /**
   * Max results (default: 100, max: 500)
   */
  'limit': (number);
  /**
   * Pagination offset
   */
  'offset': (number);
  /**
   * Filter by subject principal type
   */
  'subjectType': (string);
  /**
   * Filter by subject identity
   */
  'subjectId': (string);
  /**
   * Filter: direct, on_behalf_of
   */
  'authorityMode': (string);
  /**
   * Filter by authority grant
   */
  'authorityGrantId': (string);
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  /**
   * Exclude rows whose actor_type is in this list (e.g. ["WorkflowEngine","Orchestrator"])
   */
  'excludeActorTypes': (string)[];
  /**
   * Exclude rows whose workspace is in this list (e.g. ["_system"])
   */
  'excludeWorkspaces': (string)[];
  /**
   * Exclude rows where actor_type=service AND authority_mode=direct (platform-server internal plumbing)
   */
  'excludeServiceDirect': (boolean);
}
