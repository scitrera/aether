// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { WorkflowAuthorityLifetimeMode as _aether_v1_WorkflowAuthorityLifetimeMode, WorkflowAuthorityLifetimeMode__Output as _aether_v1_WorkflowAuthorityLifetimeMode__Output } from '../../aether/v1/WorkflowAuthorityLifetimeMode';
import type { Long } from '@grpc/proto-loader';

/**
 * Gateway-authored workflow request identity and schedule authority. The
 * gateway clears any client-supplied value before forwarding. Schedule grant
 * IDs remain outside action JSON, task payload/metadata, and workflow response
 * data. Consumers must treat this object as trusted only on the authenticated
 * WorkflowEngine connection from the gateway.
 */
export interface WorkflowRequestContext {
  'actor'?: (_aether_v1_PrincipalRef | null);
  'subject'?: (_aether_v1_PrincipalRef | null);
  'actorSessionId'?: (string);
  'scheduleAuthorization'?: (_aether_v1_AuthorizationContext | null);
  'rootGrantId'?: (string);
  'sourceGrantId'?: (string);
  'expiresAtMs'?: (number | string | Long);
  'policyDigest'?: (string);
  'lifetimeMode'?: (_aether_v1_WorkflowAuthorityLifetimeMode);
  'policyVersion'?: (number);
}

/**
 * Gateway-authored workflow request identity and schedule authority. The
 * gateway clears any client-supplied value before forwarding. Schedule grant
 * IDs remain outside action JSON, task payload/metadata, and workflow response
 * data. Consumers must treat this object as trusted only on the authenticated
 * WorkflowEngine connection from the gateway.
 */
export interface WorkflowRequestContext__Output {
  'actor': (_aether_v1_PrincipalRef__Output | null);
  'subject': (_aether_v1_PrincipalRef__Output | null);
  'actorSessionId': (string);
  'scheduleAuthorization': (_aether_v1_AuthorizationContext__Output | null);
  'rootGrantId': (string);
  'sourceGrantId': (string);
  'expiresAtMs': (string);
  'policyDigest': (string);
  'lifetimeMode': (_aether_v1_WorkflowAuthorityLifetimeMode__Output);
  'policyVersion': (number);
}
