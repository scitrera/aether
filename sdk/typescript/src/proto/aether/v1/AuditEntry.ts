// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * AuditEntry represents a single comprehensive audit log event.
 */
export interface AuditEntry {
  'auditId'?: (number | string | Long);
  /**
   * Unix timestamp (seconds)
   */
  'timestamp'?: (number | string | Long);
  /**
   * connection, auth, message, kv, task, admin, acl
   */
  'eventType'?: (string);
  /**
   * agent, task, user, system
   */
  'actorType'?: (string);
  'actorId'?: (string);
  'resourceType'?: (string);
  'resourceId'?: (string);
  'operation'?: (string);
  'workspace'?: (string);
  'sessionId'?: (string);
  'gatewayId'?: (string);
  'success'?: (boolean);
  'errorMessage'?: (string);
  /**
   * JSON-encoded metadata map
   */
  'metadataJson'?: (string);
  'subjectType'?: (string);
  'subjectId'?: (string);
  'rootSubjectType'?: (string);
  'rootSubjectId'?: (string);
  'authorityMode'?: (string);
  'rootAuthorityGrantId'?: (string);
  'authorityGrantId'?: (string);
  'parentAuthorityGrantId'?: (string);
  /**
   * "gateway" (default) or "principal" — distinguishes gateway-observed events from principal-submitted ones
   */
  'source'?: (string);
}

/**
 * AuditEntry represents a single comprehensive audit log event.
 */
export interface AuditEntry__Output {
  'auditId': (string);
  /**
   * Unix timestamp (seconds)
   */
  'timestamp': (string);
  /**
   * connection, auth, message, kv, task, admin, acl
   */
  'eventType': (string);
  /**
   * agent, task, user, system
   */
  'actorType': (string);
  'actorId': (string);
  'resourceType': (string);
  'resourceId': (string);
  'operation': (string);
  'workspace': (string);
  'sessionId': (string);
  'gatewayId': (string);
  'success': (boolean);
  'errorMessage': (string);
  /**
   * JSON-encoded metadata map
   */
  'metadataJson': (string);
  'subjectType': (string);
  'subjectId': (string);
  'rootSubjectType': (string);
  'rootSubjectId': (string);
  'authorityMode': (string);
  'rootAuthorityGrantId': (string);
  'authorityGrantId': (string);
  'parentAuthorityGrantId': (string);
  /**
   * "gateway" (default) or "principal" — distinguishes gateway-observed events from principal-submitted ones
   */
  'source': (string);
}
