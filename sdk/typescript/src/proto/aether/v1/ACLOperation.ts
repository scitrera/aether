// Original file: aether.proto

import type { ACLRuleFilter as _aether_v1_ACLRuleFilter, ACLRuleFilter__Output as _aether_v1_ACLRuleFilter__Output } from '../../aether/v1/ACLRuleFilter';
import type { ACLAuditFilter as _aether_v1_ACLAuditFilter, ACLAuditFilter__Output as _aether_v1_ACLAuditFilter__Output } from '../../aether/v1/ACLAuditFilter';
import type { ACLGrantRequest as _aether_v1_ACLGrantRequest, ACLGrantRequest__Output as _aether_v1_ACLGrantRequest__Output } from '../../aether/v1/ACLGrantRequest';
import type { ACLSetFallbackRequest as _aether_v1_ACLSetFallbackRequest, ACLSetFallbackRequest__Output as _aether_v1_ACLSetFallbackRequest__Output } from '../../aether/v1/ACLSetFallbackRequest';

// Original file: aether.proto

export const _aether_v1_ACLOperation_OpType = {
  /**
   * GET /api/acl/rules - List ACL rules with optional filters
   */
  LIST_RULES: 'LIST_RULES',
  /**
   * GET /api/acl/rules/{rule_id} - Get a specific ACL rule
   */
  GET_RULE: 'GET_RULE',
  /**
   * POST /api/acl/rules - Grant access by creating a new ACL rule
   */
  GRANT: 'GRANT',
  /**
   * DELETE /api/acl/rules/{rule_id} - Revoke access by deleting an ACL rule
   */
  REVOKE: 'REVOKE',
  /**
   * GET /api/acl/audit - Query the ACL audit log
   */
  QUERY_AUDIT: 'QUERY_AUDIT',
  /**
   * GET /api/acl/fallback-policy - Get a fallback policy by category
   */
  GET_FALLBACK_POLICY: 'GET_FALLBACK_POLICY',
  /**
   * PUT /api/acl/fallback-policy - Set/update a fallback policy
   */
  SET_FALLBACK_POLICY: 'SET_FALLBACK_POLICY',
  /**
   * POST /api/acl/cleanup/expired-rules - Remove expired ACL rules
   */
  CLEANUP_EXPIRED: 'CLEANUP_EXPIRED',
  /**
   * POST /api/acl/cleanup/audit-logs - Remove old audit log entries
   */
  CLEANUP_AUDIT_LOGS: 'CLEANUP_AUDIT_LOGS',
} as const;

export type _aether_v1_ACLOperation_OpType =
  /**
   * GET /api/acl/rules - List ACL rules with optional filters
   */
  | 'LIST_RULES'
  | 0
  /**
   * GET /api/acl/rules/{rule_id} - Get a specific ACL rule
   */
  | 'GET_RULE'
  | 1
  /**
   * POST /api/acl/rules - Grant access by creating a new ACL rule
   */
  | 'GRANT'
  | 2
  /**
   * DELETE /api/acl/rules/{rule_id} - Revoke access by deleting an ACL rule
   */
  | 'REVOKE'
  | 3
  /**
   * GET /api/acl/audit - Query the ACL audit log
   */
  | 'QUERY_AUDIT'
  | 4
  /**
   * GET /api/acl/fallback-policy - Get a fallback policy by category
   */
  | 'GET_FALLBACK_POLICY'
  | 8
  /**
   * PUT /api/acl/fallback-policy - Set/update a fallback policy
   */
  | 'SET_FALLBACK_POLICY'
  | 9
  /**
   * POST /api/acl/cleanup/expired-rules - Remove expired ACL rules
   */
  | 'CLEANUP_EXPIRED'
  | 10
  /**
   * POST /api/acl/cleanup/audit-logs - Remove old audit log entries
   */
  | 'CLEANUP_AUDIT_LOGS'
  | 11

export type _aether_v1_ACLOperation_OpType__Output = typeof _aether_v1_ACLOperation_OpType[keyof typeof _aether_v1_ACLOperation_OpType]

/**
 * ACLOperation allows clients to manage access control rules, authority grants,
 * fallback policies, and audit logs through the gRPC streaming interface.
 * This provides feature parity with the REST Admin API for ACL management
 * (rules, fallback policies, audit log). Authority grant management is
 * available exclusively via the runtime AuthorityGrantOperation surface or
 * the REST admin endpoints — the duplicated streaming admin path was removed.
 * REST equivalents:
 * - GET /api/acl/rules → LIST_RULES
 * - POST /api/acl/rules → GRANT
 * - GET /api/acl/rules/{rule_id} → GET_RULE
 * - DELETE /api/acl/rules/{rule_id} → REVOKE
 * - GET /api/acl/audit → QUERY_AUDIT
 * - GET /api/acl/fallback-policy → GET_FALLBACK_POLICY
 * - PUT /api/acl/fallback-policy → SET_FALLBACK_POLICY
 * - POST /api/acl/cleanup/expired-rules → CLEANUP_EXPIRED
 * - POST /api/acl/cleanup/audit-logs → CLEANUP_AUDIT_LOGS
 */
export interface ACLOperation {
  'op'?: (_aether_v1_ACLOperation_OpType);
  /**
   * For GET_RULE, REVOKE: the rule ID to operate on
   * This is the unique rule_id from the database
   */
  'ruleId'?: (string);
  /**
   * For GET_FALLBACK_POLICY, SET_FALLBACK_POLICY: the rule category
   * Format: {principal_type}_{resource_type} (e.g., "user_workspace", "agent_workspace")
   */
  'ruleCategory'?: (string);
  /**
   * For CLEANUP_AUDIT_LOGS: retention period in days (default: 90)
   */
  'retentionDays'?: (number);
  /**
   * For LIST_RULES: filter parameters (added in subtask-6-2)
   */
  'ruleFilter'?: (_aether_v1_ACLRuleFilter | null);
  /**
   * For QUERY_AUDIT: filter parameters (added in subtask-6-2)
   */
  'auditFilter'?: (_aether_v1_ACLAuditFilter | null);
  /**
   * For GRANT: access grant request data (added in subtask-6-2)
   */
  'grantRequest'?: (_aether_v1_ACLGrantRequest | null);
  /**
   * For SET_FALLBACK_POLICY: fallback policy update data (added in subtask-6-2)
   */
  'fallbackRequest'?: (_aether_v1_ACLSetFallbackRequest | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
}

/**
 * ACLOperation allows clients to manage access control rules, authority grants,
 * fallback policies, and audit logs through the gRPC streaming interface.
 * This provides feature parity with the REST Admin API for ACL management
 * (rules, fallback policies, audit log). Authority grant management is
 * available exclusively via the runtime AuthorityGrantOperation surface or
 * the REST admin endpoints — the duplicated streaming admin path was removed.
 * REST equivalents:
 * - GET /api/acl/rules → LIST_RULES
 * - POST /api/acl/rules → GRANT
 * - GET /api/acl/rules/{rule_id} → GET_RULE
 * - DELETE /api/acl/rules/{rule_id} → REVOKE
 * - GET /api/acl/audit → QUERY_AUDIT
 * - GET /api/acl/fallback-policy → GET_FALLBACK_POLICY
 * - PUT /api/acl/fallback-policy → SET_FALLBACK_POLICY
 * - POST /api/acl/cleanup/expired-rules → CLEANUP_EXPIRED
 * - POST /api/acl/cleanup/audit-logs → CLEANUP_AUDIT_LOGS
 */
export interface ACLOperation__Output {
  'op': (_aether_v1_ACLOperation_OpType__Output);
  /**
   * For GET_RULE, REVOKE: the rule ID to operate on
   * This is the unique rule_id from the database
   */
  'ruleId': (string);
  /**
   * For GET_FALLBACK_POLICY, SET_FALLBACK_POLICY: the rule category
   * Format: {principal_type}_{resource_type} (e.g., "user_workspace", "agent_workspace")
   */
  'ruleCategory': (string);
  /**
   * For CLEANUP_AUDIT_LOGS: retention period in days (default: 90)
   */
  'retentionDays': (number);
  /**
   * For LIST_RULES: filter parameters (added in subtask-6-2)
   */
  'ruleFilter': (_aether_v1_ACLRuleFilter__Output | null);
  /**
   * For QUERY_AUDIT: filter parameters (added in subtask-6-2)
   */
  'auditFilter': (_aether_v1_ACLAuditFilter__Output | null);
  /**
   * For GRANT: access grant request data (added in subtask-6-2)
   */
  'grantRequest': (_aether_v1_ACLGrantRequest__Output | null);
  /**
   * For SET_FALLBACK_POLICY: fallback policy update data (added in subtask-6-2)
   */
  'fallbackRequest': (_aether_v1_ACLSetFallbackRequest__Output | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
}
