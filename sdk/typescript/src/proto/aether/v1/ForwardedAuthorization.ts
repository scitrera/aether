// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
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
}
