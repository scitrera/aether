// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLGrantRequest contains data for granting access (creating an ACL rule).
 * Matches GrantAccess parameters in internal/acl/service.go.
 */
export interface ACLGrantRequest {
  /**
   * Type of principal receiving access
   */
  'principalType'?: (string);
  /**
   * ID of principal receiving access
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
   * Access level to grant (0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN)
   */
  'accessLevel'?: (number);
  /**
   * Identity of who is granting access
   */
  'grantedBy'?: (string);
  /**
   * Human-readable reason for this grant
   */
  'reason'?: (string);
  /**
   * Optional expiration time (Unix timestamp, 0 = no expiration)
   */
  'expiresAt'?: (number | string | Long);
}

/**
 * ACLGrantRequest contains data for granting access (creating an ACL rule).
 * Matches GrantAccess parameters in internal/acl/service.go.
 */
export interface ACLGrantRequest__Output {
  /**
   * Type of principal receiving access
   */
  'principalType': (string);
  /**
   * ID of principal receiving access
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
   * Access level to grant (0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN)
   */
  'accessLevel': (number);
  /**
   * Identity of who is granting access
   */
  'grantedBy': (string);
  /**
   * Human-readable reason for this grant
   */
  'reason': (string);
  /**
   * Optional expiration time (Unix timestamp, 0 = no expiration)
   */
  'expiresAt': (string);
}
