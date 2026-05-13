// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLAuditEntryInfo represents an ACL decision audit log entry.
 * Matches the AuditLogEntry struct in internal/acl/types.go.
 */
export interface ACLAuditEntryInfo {
  /**
   * Unique audit entry identifier
   */
  'auditId'?: (number | string | Long);
  /**
   * Unix timestamp when decision was made
   */
  'timestamp'?: (number | string | Long);
  /**
   * Decision: "ALLOW" or "DENY"
   */
  'decision'?: (string);
  /**
   * Effective access level granted (or 0 if denied)
   */
  'accessLevel'?: (number);
  /**
   * Human-readable access level name
   */
  'accessLevelName'?: (string);
  /**
   * Type of principal that requested access
   */
  'principalType'?: (string);
  /**
   * ID of principal that requested access
   */
  'principalId'?: (string);
  /**
   * Type of resource being accessed
   */
  'resourceType'?: (string);
  /**
   * ID of resource being accessed
   */
  'resourceId'?: (string);
  /**
   * Operation that was attempted
   */
  'operation'?: (string);
  /**
   * Workspace context
   */
  'workspace'?: (string);
  /**
   * Rule ID that granted access (empty if fallback or denied)
   */
  'ruleId'?: (string);
  /**
   * Whether fallback policy was used
   */
  'fallbackApplied'?: (boolean);
  /**
   * Gateway that processed this request
   */
  'gatewayId'?: (string);
  /**
   * Session ID of the connection
   */
  'sessionId'?: (string);
  /**
   * Additional metadata
   */
  'metadata'?: ({[key: string]: string});
}

/**
 * ACLAuditEntryInfo represents an ACL decision audit log entry.
 * Matches the AuditLogEntry struct in internal/acl/types.go.
 */
export interface ACLAuditEntryInfo__Output {
  /**
   * Unique audit entry identifier
   */
  'auditId': (string);
  /**
   * Unix timestamp when decision was made
   */
  'timestamp': (string);
  /**
   * Decision: "ALLOW" or "DENY"
   */
  'decision': (string);
  /**
   * Effective access level granted (or 0 if denied)
   */
  'accessLevel': (number);
  /**
   * Human-readable access level name
   */
  'accessLevelName': (string);
  /**
   * Type of principal that requested access
   */
  'principalType': (string);
  /**
   * ID of principal that requested access
   */
  'principalId': (string);
  /**
   * Type of resource being accessed
   */
  'resourceType': (string);
  /**
   * ID of resource being accessed
   */
  'resourceId': (string);
  /**
   * Operation that was attempted
   */
  'operation': (string);
  /**
   * Workspace context
   */
  'workspace': (string);
  /**
   * Rule ID that granted access (empty if fallback or denied)
   */
  'ruleId': (string);
  /**
   * Whether fallback policy was used
   */
  'fallbackApplied': (boolean);
  /**
   * Gateway that processed this request
   */
  'gatewayId': (string);
  /**
   * Session ID of the connection
   */
  'sessionId': (string);
  /**
   * Additional metadata
   */
  'metadata': ({[key: string]: string});
}
