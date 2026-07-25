// Original file: aether.proto

import type { ACLRuleInfo as _aether_v1_ACLRuleInfo, ACLRuleInfo__Output as _aether_v1_ACLRuleInfo__Output } from '../../aether/v1/ACLRuleInfo';
import type { ACLFallbackPolicyInfo as _aether_v1_ACLFallbackPolicyInfo, ACLFallbackPolicyInfo__Output as _aether_v1_ACLFallbackPolicyInfo__Output } from '../../aether/v1/ACLFallbackPolicyInfo';
import type { ACLAuditEntryInfo as _aether_v1_ACLAuditEntryInfo, ACLAuditEntryInfo__Output as _aether_v1_ACLAuditEntryInfo__Output } from '../../aether/v1/ACLAuditEntryInfo';
import type { ACLCleanupResult as _aether_v1_ACLCleanupResult, ACLCleanupResult__Output as _aether_v1_ACLCleanupResult__Output } from '../../aether/v1/ACLCleanupResult';
import type { ACLAuthorityGrantInfo as _aether_v1_ACLAuthorityGrantInfo, ACLAuthorityGrantInfo__Output as _aether_v1_ACLAuthorityGrantInfo__Output } from '../../aether/v1/ACLAuthorityGrantInfo';
import type { ACLGroupInfo as _aether_v1_ACLGroupInfo, ACLGroupInfo__Output as _aether_v1_ACLGroupInfo__Output } from '../../aether/v1/ACLGroupInfo';
import type { ACLRoleInfo as _aether_v1_ACLRoleInfo, ACLRoleInfo__Output as _aether_v1_ACLRoleInfo__Output } from '../../aether/v1/ACLRoleInfo';
import type { ACLGroupMemberInfo as _aether_v1_ACLGroupMemberInfo, ACLGroupMemberInfo__Output as _aether_v1_ACLGroupMemberInfo__Output } from '../../aether/v1/ACLGroupMemberInfo';
import type { ACLRoleAssignmentInfo as _aether_v1_ACLRoleAssignmentInfo, ACLRoleAssignmentInfo__Output as _aether_v1_ACLRoleAssignmentInfo__Output } from '../../aether/v1/ACLRoleAssignmentInfo';
import type { ACLAccessExplanationInfo as _aether_v1_ACLAccessExplanationInfo, ACLAccessExplanationInfo__Output as _aether_v1_ACLAccessExplanationInfo__Output } from '../../aether/v1/ACLAccessExplanationInfo';

/**
 * ACLResponse is sent in response to ACLOperation.
 * Contains data based on the operation type that was requested.
 */
export interface ACLResponse {
  'success'?: (boolean);
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * Human-readable result message
   */
  'message'?: (string);
  /**
   * For GET_RULE operation (single rule)
   */
  'rule'?: (_aether_v1_ACLRuleInfo | null);
  /**
   * For LIST_RULES operation (multiple rules)
   */
  'rules'?: (_aether_v1_ACLRuleInfo)[];
  /**
   * For LIST_RULES: total count (may differ from returned if filtered)
   */
  'totalRules'?: (number);
  /**
   * For GET_FALLBACK_POLICY operation (single policy)
   */
  'fallbackPolicy'?: (_aether_v1_ACLFallbackPolicyInfo | null);
  /**
   * For QUERY_AUDIT operation (multiple audit entries)
   */
  'auditEntries'?: (_aether_v1_ACLAuditEntryInfo)[];
  /**
   * For QUERY_AUDIT: total count (may differ from returned if limited)
   */
  'totalAuditEntries'?: (number);
  /**
   * For CLEANUP_EXPIRED and CLEANUP_AUDIT_LOGS operations
   */
  'cleanupResult'?: (_aether_v1_ACLCleanupResult | null);
  /**
   * Echoed from the originating ACLOperation for correlation
   */
  'requestId'?: (string);
  /**
   * For GET_AUTHORITY_GRANT / CREATE_AUTHORITY_GRANT / RENEW_AUTHORITY_GRANT
   */
  'authorityGrant'?: (_aether_v1_ACLAuthorityGrantInfo | null);
  /**
   * For LIST_AUTHORITY_GRANTS
   */
  'authorityGrants'?: (_aether_v1_ACLAuthorityGrantInfo)[];
  'totalAuthorityGrants'?: (number);
  /**
   * Role/group results.
   */
  'group'?: (_aether_v1_ACLGroupInfo | null);
  /**
   * LIST_GROUPS
   */
  'groups'?: (_aether_v1_ACLGroupInfo)[];
  /**
   * GET_ROLE / CREATE_ROLE
   */
  'role'?: (_aether_v1_ACLRoleInfo | null);
  /**
   * LIST_ROLES
   */
  'roles'?: (_aether_v1_ACLRoleInfo)[];
  /**
   * LIST_GROUP_MEMBERS / LIST_PRINCIPAL_GROUPS
   */
  'groupMembers'?: (_aether_v1_ACLGroupMemberInfo)[];
  /**
   * LIST_ROLE_ASSIGNMENTS / LIST_PRINCIPAL_ROLES
   */
  'roleAssignments'?: (_aether_v1_ACLRoleAssignmentInfo)[];
  /**
   * EXPLAIN_ACCESS
   */
  'explanation'?: (_aether_v1_ACLAccessExplanationInfo | null);
}

/**
 * ACLResponse is sent in response to ACLOperation.
 * Contains data based on the operation type that was requested.
 */
export interface ACLResponse__Output {
  'success': (boolean);
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * Human-readable result message
   */
  'message': (string);
  /**
   * For GET_RULE operation (single rule)
   */
  'rule': (_aether_v1_ACLRuleInfo__Output | null);
  /**
   * For LIST_RULES operation (multiple rules)
   */
  'rules': (_aether_v1_ACLRuleInfo__Output)[];
  /**
   * For LIST_RULES: total count (may differ from returned if filtered)
   */
  'totalRules': (number);
  /**
   * For GET_FALLBACK_POLICY operation (single policy)
   */
  'fallbackPolicy': (_aether_v1_ACLFallbackPolicyInfo__Output | null);
  /**
   * For QUERY_AUDIT operation (multiple audit entries)
   */
  'auditEntries': (_aether_v1_ACLAuditEntryInfo__Output)[];
  /**
   * For QUERY_AUDIT: total count (may differ from returned if limited)
   */
  'totalAuditEntries': (number);
  /**
   * For CLEANUP_EXPIRED and CLEANUP_AUDIT_LOGS operations
   */
  'cleanupResult': (_aether_v1_ACLCleanupResult__Output | null);
  /**
   * Echoed from the originating ACLOperation for correlation
   */
  'requestId': (string);
  /**
   * For GET_AUTHORITY_GRANT / CREATE_AUTHORITY_GRANT / RENEW_AUTHORITY_GRANT
   */
  'authorityGrant': (_aether_v1_ACLAuthorityGrantInfo__Output | null);
  /**
   * For LIST_AUTHORITY_GRANTS
   */
  'authorityGrants': (_aether_v1_ACLAuthorityGrantInfo__Output)[];
  'totalAuthorityGrants': (number);
  /**
   * Role/group results.
   */
  'group': (_aether_v1_ACLGroupInfo__Output | null);
  /**
   * LIST_GROUPS
   */
  'groups': (_aether_v1_ACLGroupInfo__Output)[];
  /**
   * GET_ROLE / CREATE_ROLE
   */
  'role': (_aether_v1_ACLRoleInfo__Output | null);
  /**
   * LIST_ROLES
   */
  'roles': (_aether_v1_ACLRoleInfo__Output)[];
  /**
   * LIST_GROUP_MEMBERS / LIST_PRINCIPAL_GROUPS
   */
  'groupMembers': (_aether_v1_ACLGroupMemberInfo__Output)[];
  /**
   * LIST_ROLE_ASSIGNMENTS / LIST_PRINCIPAL_ROLES
   */
  'roleAssignments': (_aether_v1_ACLRoleAssignmentInfo__Output)[];
  /**
   * EXPLAIN_ACCESS
   */
  'explanation': (_aether_v1_ACLAccessExplanationInfo__Output | null);
}
