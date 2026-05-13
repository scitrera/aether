// Original file: aether.proto

import type { AuditEntry as _aether_v1_AuditEntry, AuditEntry__Output as _aether_v1_AuditEntry__Output } from '../../aether/v1/AuditEntry';

/**
 * AuditQueryResponse returns comprehensive audit log entries.
 */
export interface AuditQueryResponse {
  /**
   * Correlation ID matching the request
   */
  'requestId'?: (string);
  'success'?: (boolean);
  /**
   * Error message if success=false
   */
  'error'?: (string);
  'entries'?: (_aether_v1_AuditEntry)[];
  /**
   * Total matching entries (for pagination)
   */
  'totalCount'?: (number);
}

/**
 * AuditQueryResponse returns comprehensive audit log entries.
 */
export interface AuditQueryResponse__Output {
  /**
   * Correlation ID matching the request
   */
  'requestId': (string);
  'success': (boolean);
  /**
   * Error message if success=false
   */
  'error': (string);
  'entries': (_aether_v1_AuditEntry__Output)[];
  /**
   * Total matching entries (for pagination)
   */
  'totalCount': (number);
}
