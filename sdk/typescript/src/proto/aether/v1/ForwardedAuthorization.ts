// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { AuthorityContinuationScope as _aether_v1_AuthorityContinuationScope, AuthorityContinuationScope__Output as _aether_v1_AuthorityContinuationScope__Output } from '../../aether/v1/AuthorityContinuationScope';
import type { Long } from '@grpc/proto-loader';

/**
 * Trusted authorization continuation carried outside the application payload.
 * The child grant is non-delegable, scope-attenuated to its parent, short-lived,
 * and linked into the parent's revocation cascade.
 */
export interface ForwardedAuthorization {
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  'rootGrantId'?: (string);
  'expiresAtMs'?: (number | string | Long);
  'deliveryTarget'?: (string);
  /**
   * Empty only for a reusable service continuation using INHERIT_PARENT.
   */
  'bindingId'?: (string);
  /**
   * Gateway-authored projection of the effective child scope. Recipients use
   * this to enforce their local, server-owned invocation authority profile.
   */
  'scope'?: (_aether_v1_AuthorityContinuationScope | null);
}

/**
 * Trusted authorization continuation carried outside the application payload.
 * The child grant is non-delegable, scope-attenuated to its parent, short-lived,
 * and linked into the parent's revocation cascade.
 */
export interface ForwardedAuthorization__Output {
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  'rootGrantId': (string);
  'expiresAtMs': (string);
  'deliveryTarget': (string);
  /**
   * Empty only for a reusable service continuation using INHERIT_PARENT.
   */
  'bindingId': (string);
  /**
   * Gateway-authored projection of the effective child scope. Recipients use
   * this to enforce their local, server-owned invocation authority profile.
   */
  'scope': (_aether_v1_AuthorityContinuationScope__Output | null);
}
