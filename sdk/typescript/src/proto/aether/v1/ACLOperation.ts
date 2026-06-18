// Original file: aether.proto

import type { ACLRuleFilter as _aether_v1_ACLRuleFilter, ACLRuleFilter__Output as _aether_v1_ACLRuleFilter__Output } from '../../aether/v1/ACLRuleFilter';
import type { ACLAuditFilter as _aether_v1_ACLAuditFilter, ACLAuditFilter__Output as _aether_v1_ACLAuditFilter__Output } from '../../aether/v1/ACLAuditFilter';
import type { ACLGrantRequest as _aether_v1_ACLGrantRequest, ACLGrantRequest__Output as _aether_v1_ACLGrantRequest__Output } from '../../aether/v1/ACLGrantRequest';
import type { ACLSetFallbackRequest as _aether_v1_ACLSetFallbackRequest, ACLSetFallbackRequest__Output as _aether_v1_ACLSetFallbackRequest__Output } from '../../aether/v1/ACLSetFallbackRequest';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { ACLGroupRequest as _aether_v1_ACLGroupRequest, ACLGroupRequest__Output as _aether_v1_ACLGroupRequest__Output } from '../../aether/v1/ACLGroupRequest';
import type { ACLRoleRequest as _aether_v1_ACLRoleRequest, ACLRoleRequest__Output as _aether_v1_ACLRoleRequest__Output } from '../../aether/v1/ACLRoleRequest';
import type { ACLGroupMemberRequest as _aether_v1_ACLGroupMemberRequest, ACLGroupMemberRequest__Output as _aether_v1_ACLGroupMemberRequest__Output } from '../../aether/v1/ACLGroupMemberRequest';
import type { ACLRoleAssignmentRequest as _aether_v1_ACLRoleAssignmentRequest, ACLRoleAssignmentRequest__Output as _aether_v1_ACLRoleAssignmentRequest__Output } from '../../aether/v1/ACLRoleAssignmentRequest';

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
  /**
   * Role/group authorization. REST equivalents under /api/acl/groups,
   * /api/acl/roles, and /api/acl/principals/{type}/{id}/{groups,roles}.
   */
  CREATE_GROUP: 'CREATE_GROUP',
  /**
   * Delete a group (name)
   */
  DELETE_GROUP: 'DELETE_GROUP',
  /**
   * Get a group (name)
   */
  GET_GROUP: 'GET_GROUP',
  /**
   * List all groups
   */
  LIST_GROUPS: 'LIST_GROUPS',
  /**
   * Add a member to a group (name + member_request)
   */
  ADD_GROUP_MEMBER: 'ADD_GROUP_MEMBER',
  /**
   * Remove a member from a group (name + principal)
   */
  REMOVE_GROUP_MEMBER: 'REMOVE_GROUP_MEMBER',
  /**
   * List members of a group (name)
   */
  LIST_GROUP_MEMBERS: 'LIST_GROUP_MEMBERS',
  /**
   * Create a role (role_request)
   */
  CREATE_ROLE: 'CREATE_ROLE',
  /**
   * Delete a role (name)
   */
  DELETE_ROLE: 'DELETE_ROLE',
  /**
   * Get a role (name)
   */
  GET_ROLE: 'GET_ROLE',
  /**
   * List all roles
   */
  LIST_ROLES: 'LIST_ROLES',
  /**
   * Assign a role (name + assignment_request)
   */
  ASSIGN_ROLE: 'ASSIGN_ROLE',
  /**
   * Unassign a role (name + principal)
   */
  UNASSIGN_ROLE: 'UNASSIGN_ROLE',
  /**
   * List assignees of a role (name)
   */
  LIST_ROLE_ASSIGNMENTS: 'LIST_ROLE_ASSIGNMENTS',
  /**
   * List groups a principal belongs to (principal)
   */
  LIST_PRINCIPAL_GROUPS: 'LIST_PRINCIPAL_GROUPS',
  /**
   * List roles assigned to a principal (principal)
   */
  LIST_PRINCIPAL_ROLES: 'LIST_PRINCIPAL_ROLES',
  /**
   * Explain effective access (principal + resource_type + resource_id [+ required_level])
   */
  EXPLAIN_ACCESS: 'EXPLAIN_ACCESS',
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
  /**
   * Role/group authorization. REST equivalents under /api/acl/groups,
   * /api/acl/roles, and /api/acl/principals/{type}/{id}/{groups,roles}.
   */
  | 'CREATE_GROUP'
  | 17
  /**
   * Delete a group (name)
   */
  | 'DELETE_GROUP'
  | 18
  /**
   * Get a group (name)
   */
  | 'GET_GROUP'
  | 19
  /**
   * List all groups
   */
  | 'LIST_GROUPS'
  | 20
  /**
   * Add a member to a group (name + member_request)
   */
  | 'ADD_GROUP_MEMBER'
  | 21
  /**
   * Remove a member from a group (name + principal)
   */
  | 'REMOVE_GROUP_MEMBER'
  | 22
  /**
   * List members of a group (name)
   */
  | 'LIST_GROUP_MEMBERS'
  | 23
  /**
   * Create a role (role_request)
   */
  | 'CREATE_ROLE'
  | 24
  /**
   * Delete a role (name)
   */
  | 'DELETE_ROLE'
  | 25
  /**
   * Get a role (name)
   */
  | 'GET_ROLE'
  | 26
  /**
   * List all roles
   */
  | 'LIST_ROLES'
  | 27
  /**
   * Assign a role (name + assignment_request)
   */
  | 'ASSIGN_ROLE'
  | 28
  /**
   * Unassign a role (name + principal)
   */
  | 'UNASSIGN_ROLE'
  | 29
  /**
   * List assignees of a role (name)
   */
  | 'LIST_ROLE_ASSIGNMENTS'
  | 30
  /**
   * List groups a principal belongs to (principal)
   */
  | 'LIST_PRINCIPAL_GROUPS'
  | 31
  /**
   * List roles assigned to a principal (principal)
   */
  | 'LIST_PRINCIPAL_ROLES'
  | 32
  /**
   * Explain effective access (principal + resource_type + resource_id [+ required_level])
   */
  | 'EXPLAIN_ACCESS'
  | 33

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
  /**
   * Role/group fields.
   * name: group/role name for GET/DELETE/LIST_*_MEMBERS/ASSIGNMENTS/ADD/ASSIGN.
   */
  'name'?: (string);
  /**
   * principal: member/assignee for REMOVE_GROUP_MEMBER, UNASSIGN_ROLE, and
   * the subject for LIST_PRINCIPAL_GROUPS / LIST_PRINCIPAL_ROLES.
   */
  'principal'?: (_aether_v1_PrincipalRef | null);
  /**
   * CREATE_GROUP
   */
  'groupRequest'?: (_aether_v1_ACLGroupRequest | null);
  /**
   * CREATE_ROLE
   */
  'roleRequest'?: (_aether_v1_ACLRoleRequest | null);
  /**
   * ADD_GROUP_MEMBER
   */
  'memberRequest'?: (_aether_v1_ACLGroupMemberRequest | null);
  /**
   * ASSIGN_ROLE
   */
  'assignmentRequest'?: (_aether_v1_ACLRoleAssignmentRequest | null);
  /**
   * EXPLAIN_ACCESS: the resource to explain against (principal carries the
   * subject; required_level is the threshold the decision is compared to).
   */
  'resourceType'?: (string);
  'resourceId'?: (string);
  'requiredLevel'?: (number);
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
  /**
   * Role/group fields.
   * name: group/role name for GET/DELETE/LIST_*_MEMBERS/ASSIGNMENTS/ADD/ASSIGN.
   */
  'name': (string);
  /**
   * principal: member/assignee for REMOVE_GROUP_MEMBER, UNASSIGN_ROLE, and
   * the subject for LIST_PRINCIPAL_GROUPS / LIST_PRINCIPAL_ROLES.
   */
  'principal': (_aether_v1_PrincipalRef__Output | null);
  /**
   * CREATE_GROUP
   */
  'groupRequest': (_aether_v1_ACLGroupRequest__Output | null);
  /**
   * CREATE_ROLE
   */
  'roleRequest': (_aether_v1_ACLRoleRequest__Output | null);
  /**
   * ADD_GROUP_MEMBER
   */
  'memberRequest': (_aether_v1_ACLGroupMemberRequest__Output | null);
  /**
   * ASSIGN_ROLE
   */
  'assignmentRequest': (_aether_v1_ACLRoleAssignmentRequest__Output | null);
  /**
   * EXPLAIN_ACCESS: the resource to explain against (principal carries the
   * subject; required_level is the threshold the decision is compared to).
   */
  'resourceType': (string);
  'resourceId': (string);
  'requiredLevel': (number);
}
