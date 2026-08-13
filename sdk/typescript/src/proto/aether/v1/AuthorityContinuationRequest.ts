// Original file: aether.proto

import type { AuthorityContinuationScope as _aether_v1_AuthorityContinuationScope, AuthorityContinuationScope__Output as _aether_v1_AuthorityContinuationScope__Output } from '../../aether/v1/AuthorityContinuationScope';

// Original file: aether.proto

export const _aether_v1_AuthorityContinuationRequest_ScopeMode = {
  SCOPE_MODE_UNSPECIFIED: 'SCOPE_MODE_UNSPECIFIED',
  /**
   * Service-only mode used when a trusted service must evaluate arbitrary
   * resources within the caller's existing authority ceiling.
   */
  SCOPE_MODE_INHERIT_PARENT: 'SCOPE_MODE_INHERIT_PARENT',
  /**
   * Required for agent recipients. The requested scope is validated as a
   * strict subset of the parent and the child is minted per invocation.
   */
  SCOPE_MODE_ATTENUATE: 'SCOPE_MODE_ATTENUATE',
} as const;

export type _aether_v1_AuthorityContinuationRequest_ScopeMode =
  | 'SCOPE_MODE_UNSPECIFIED'
  | 0
  /**
   * Service-only mode used when a trusted service must evaluate arbitrary
   * resources within the caller's existing authority ceiling.
   */
  | 'SCOPE_MODE_INHERIT_PARENT'
  | 1
  /**
   * Required for agent recipients. The requested scope is validated as a
   * strict subset of the parent and the child is minted per invocation.
   */
  | 'SCOPE_MODE_ATTENUATE'
  | 2

export type _aether_v1_AuthorityContinuationRequest_ScopeMode__Output = typeof _aether_v1_AuthorityContinuationRequest_ScopeMode[keyof typeof _aether_v1_AuthorityContinuationRequest_ScopeMode]

export interface AuthorityContinuationRequest {
  'scopeMode'?: (_aether_v1_AuthorityContinuationRequest_ScopeMode);
  /**
   * Opaque invocation identifier. Required for ATTENUATE and matched to the
   * checked-access correlation ID so the trusted receipt, child, and payload
   * can be validated as one call by the recipient.
   */
  'bindingId'?: (string);
  'scope'?: (_aether_v1_AuthorityContinuationScope | null);
}

export interface AuthorityContinuationRequest__Output {
  'scopeMode': (_aether_v1_AuthorityContinuationRequest_ScopeMode__Output);
  /**
   * Opaque invocation identifier. Required for ATTENUATE and matched to the
   * checked-access correlation ID so the trusted receipt, child, and payload
   * can be validated as one call by the recipient.
   */
  'bindingId': (string);
  'scope': (_aether_v1_AuthorityContinuationScope__Output | null);
}
