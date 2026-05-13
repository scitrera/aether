// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLAuditFilter specifies filtering parameters for querying the ACL audit log.
 * Matches AuditLogFilter struct in internal/acl/audit.go.
 */
export interface ACLAuditFilter {
  /**
   * Start time filter (Unix timestamp, 0 = no start filter)
   */
  'startTime'?: (number | string | Long);
  /**
   * End time filter (Unix timestamp, 0 = no end filter)
   */
  'endTime'?: (number | string | Long);
  /**
   * Filter by principal type
   */
  'principalType'?: (string);
  /**
   * Filter by principal ID
   */
  'principalId'?: (string);
  /**
   * Filter by resource type
   */
  'resourceType'?: (string);
  /**
   * Filter by resource ID
   */
  'resourceId'?: (string);
  /**
   * Filter by decision: "ALLOW" or "DENY"
   */
  'decision'?: (string);
  /**
   * Filter by workspace
   */
  'workspace'?: (string);
  /**
   * Maximum number of results (0 = no limit)
   */
  'limit'?: (number);
  /**
   * Offset for pagination
   */
  'offset'?: (number);
}

/**
 * ACLAuditFilter specifies filtering parameters for querying the ACL audit log.
 * Matches AuditLogFilter struct in internal/acl/audit.go.
 */
export interface ACLAuditFilter__Output {
  /**
   * Start time filter (Unix timestamp, 0 = no start filter)
   */
  'startTime': (string);
  /**
   * End time filter (Unix timestamp, 0 = no end filter)
   */
  'endTime': (string);
  /**
   * Filter by principal type
   */
  'principalType': (string);
  /**
   * Filter by principal ID
   */
  'principalId': (string);
  /**
   * Filter by resource type
   */
  'resourceType': (string);
  /**
   * Filter by resource ID
   */
  'resourceId': (string);
  /**
   * Filter by decision: "ALLOW" or "DENY"
   */
  'decision': (string);
  /**
   * Filter by workspace
   */
  'workspace': (string);
  /**
   * Maximum number of results (0 = no limit)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
}
