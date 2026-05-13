// Original file: aether.proto


/**
 * SubmitAuditEventResponse acknowledges a SubmitAuditEventRequest. success=false
 * indicates the event was rejected before persistence (event type forbidden,
 * rate-limited, ACL denial, audit disabled, etc.).
 */
export interface SubmitAuditEventResponse {
  /**
   * Echoes the request's correlation ID
   */
  'clientRequestId'?: (string);
  /**
   * True iff the event was enqueued for persistence
   */
  'success'?: (boolean);
  /**
   * ERR_AUDIT_TYPE_FORBIDDEN / ERR_PERMISSION_DENIED / ERR_AUDIT_RATE_LIMITED / ERR_AUDIT_DISABLED
   */
  'errorCode'?: (string);
  /**
   * Human-readable rejection reason
   */
  'errorMessage'?: (string);
}

/**
 * SubmitAuditEventResponse acknowledges a SubmitAuditEventRequest. success=false
 * indicates the event was rejected before persistence (event type forbidden,
 * rate-limited, ACL denial, audit disabled, etc.).
 */
export interface SubmitAuditEventResponse__Output {
  /**
   * Echoes the request's correlation ID
   */
  'clientRequestId': (string);
  /**
   * True iff the event was enqueued for persistence
   */
  'success': (boolean);
  /**
   * ERR_AUDIT_TYPE_FORBIDDEN / ERR_PERMISSION_DENIED / ERR_AUDIT_RATE_LIMITED / ERR_AUDIT_DISABLED
   */
  'errorCode': (string);
  /**
   * Human-readable rejection reason
   */
  'errorMessage': (string);
}
