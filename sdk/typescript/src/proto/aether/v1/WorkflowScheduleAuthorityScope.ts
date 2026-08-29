// Original file: aether.proto

import type { ACLAuthorityGrantResourceScopeEntry as _aether_v1_ACLAuthorityGrantResourceScopeEntry, ACLAuthorityGrantResourceScopeEntry__Output as _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output } from '../../aether/v1/ACLAuthorityGrantResourceScopeEntry';
import type { WorkflowAuthorityLifetimeMode as _aether_v1_WorkflowAuthorityLifetimeMode, WorkflowAuthorityLifetimeMode__Output as _aether_v1_WorkflowAuthorityLifetimeMode__Output } from '../../aether/v1/WorkflowAuthorityLifetimeMode';
import type { Long } from '@grpc/proto-loader';

/**
 * Requested ceiling for the private authority attached to one schedule. The
 * gateway validates/attenuates this against the authenticated caller context;
 * the WorkflowEngine never trusts it directly and never stores it in action
 * JSON. Empty resource or operation scope is invalid.
 */
export interface WorkflowScheduleAuthorityScope {
  'workspaceScope'?: (string)[];
  'resourceScope'?: (_aether_v1_ACLAuthorityGrantResourceScopeEntry)[];
  'operationScope'?: (string)[];
  'maxAccessLevel'?: (number);
  'expiresAt'?: (number | string | Long);
  'renewableUntil'?: (number | string | Long);
  'requiredTaskAuthorityHops'?: (number);
  'lifetimeMode'?: (_aether_v1_WorkflowAuthorityLifetimeMode);
  /**
   * Version of the deterministic schedule-authority policy shape. Callers
   * currently send 1; unknown versions fail closed instead of being silently
   * reinterpreted after an upgrade.
   */
  'policyVersion'?: (number);
}

/**
 * Requested ceiling for the private authority attached to one schedule. The
 * gateway validates/attenuates this against the authenticated caller context;
 * the WorkflowEngine never trusts it directly and never stores it in action
 * JSON. Empty resource or operation scope is invalid.
 */
export interface WorkflowScheduleAuthorityScope__Output {
  'workspaceScope': (string)[];
  'resourceScope': (_aether_v1_ACLAuthorityGrantResourceScopeEntry__Output)[];
  'operationScope': (string)[];
  'maxAccessLevel': (number);
  'expiresAt': (string);
  'renewableUntil': (string);
  'requiredTaskAuthorityHops': (number);
  'lifetimeMode': (_aether_v1_WorkflowAuthorityLifetimeMode__Output);
  /**
   * Version of the deterministic schedule-authority policy shape. Callers
   * currently send 1; unknown versions fail closed instead of being silently
   * reinterpreted after an upgrade.
   */
  'policyVersion': (number);
}
