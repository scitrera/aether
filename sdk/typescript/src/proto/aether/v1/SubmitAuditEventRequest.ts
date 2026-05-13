// Original file: aether.proto


/**
 * SubmitAuditEventRequest lets a connected principal submit a structured audit
 * event for forensic recording. The gateway stamps actor identity from the
 * authenticated session and ignores any client-supplied actor fields. Allowed
 * event_type values: "message", "kv", "task", "custom". Gateway-truth
 * categories (connection, auth, admin, acl) are reserved for server emission
 * only and will be rejected.
 */
export interface SubmitAuditEventRequest {
  /**
   * Required: one of "message", "kv", "task", "custom"
   */
  'eventType'?: (string);
  /**
   * Free-form operation name describing what occurred
   */
  'operation'?: (string);
  /**
   * Resource type (topic, kv_key, task, etc.)
   */
  'resourceType'?: (string);
  /**
   * Resource identifier
   */
  'resourceId'?: (string);
  /**
   * Optional; defaults to caller's workspace. Cross-workspace submissions require capability/audit_submit.
   */
  'workspace'?: (string);
  /**
   * Whether the underlying operation succeeded
   */
  'success'?: (boolean);
  /**
   * Optional error context for failures
   */
  'errorMessage'?: (string);
  /**
   * Additional context — credential-shaped keys are auto-redacted
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Correlation ID echoed in the response
   */
  'clientRequestId'?: (string);
}

/**
 * SubmitAuditEventRequest lets a connected principal submit a structured audit
 * event for forensic recording. The gateway stamps actor identity from the
 * authenticated session and ignores any client-supplied actor fields. Allowed
 * event_type values: "message", "kv", "task", "custom". Gateway-truth
 * categories (connection, auth, admin, acl) are reserved for server emission
 * only and will be rejected.
 */
export interface SubmitAuditEventRequest__Output {
  /**
   * Required: one of "message", "kv", "task", "custom"
   */
  'eventType': (string);
  /**
   * Free-form operation name describing what occurred
   */
  'operation': (string);
  /**
   * Resource type (topic, kv_key, task, etc.)
   */
  'resourceType': (string);
  /**
   * Resource identifier
   */
  'resourceId': (string);
  /**
   * Optional; defaults to caller's workspace. Cross-workspace submissions require capability/audit_submit.
   */
  'workspace': (string);
  /**
   * Whether the underlying operation succeeded
   */
  'success': (boolean);
  /**
   * Optional error context for failures
   */
  'errorMessage': (string);
  /**
   * Additional context — credential-shaped keys are auto-redacted
   */
  'metadata': ({[key: string]: string});
  /**
   * Correlation ID echoed in the response
   */
  'clientRequestId': (string);
}
