// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { ResolvedAuthorityInfo as _aether_v1_ResolvedAuthorityInfo, ResolvedAuthorityInfo__Output as _aether_v1_ResolvedAuthorityInfo__Output } from '../../aether/v1/ResolvedAuthorityInfo';

export interface AuthorizationContext {
  /**
   * "direct" or "on_behalf_of"
   */
  'authorityMode'?: (string);
  /**
   * Required for on_behalf_of
   */
  'subject'?: (_aether_v1_PrincipalRef | null);
  /**
   * Required for on_behalf_of
   */
  'grantId'?: (string);
  /**
   * Pre-resolved grant info populated by the gateway BEFORE publishing to a
   * terminator. The gateway already validated the grant under the calling
   * delegate when it ran proxyACLCheck — terminators in strict mode can use
   * this in lieu of issuing their own ResolveAuthorityRequest, eliminating a
   * round-trip per request and removing the delegate/audience actor mismatch
   * (the audience is not the delegate, so the audience-side resolver call
   * would otherwise fail acl.ResolveAuthority's actor==delegate check).
   * 
   * Trust anchor: the terminator's gRPC connection is mTLS-authenticated to
   * the same gateway that authenticated the originating delegate, so
   * gateway-stamped fields are trusted within a single Aether cluster.
   * Federated/multi-cluster topologies that introduce untrusted relays MUST
   * either re-resolve via the resolver path or extend this message with a
   * signed attestation.
   * 
   * Empty when unset (e.g. direct mode, plain SendMessage path, or older
   * gateway that doesn't yet stamp resolved info — terminators fall back to
   * the resolver path in that case).
   */
  'resolved'?: (_aether_v1_ResolvedAuthorityInfo | null);
}

export interface AuthorizationContext__Output {
  /**
   * "direct" or "on_behalf_of"
   */
  'authorityMode': (string);
  /**
   * Required for on_behalf_of
   */
  'subject': (_aether_v1_PrincipalRef__Output | null);
  /**
   * Required for on_behalf_of
   */
  'grantId': (string);
  /**
   * Pre-resolved grant info populated by the gateway BEFORE publishing to a
   * terminator. The gateway already validated the grant under the calling
   * delegate when it ran proxyACLCheck — terminators in strict mode can use
   * this in lieu of issuing their own ResolveAuthorityRequest, eliminating a
   * round-trip per request and removing the delegate/audience actor mismatch
   * (the audience is not the delegate, so the audience-side resolver call
   * would otherwise fail acl.ResolveAuthority's actor==delegate check).
   * 
   * Trust anchor: the terminator's gRPC connection is mTLS-authenticated to
   * the same gateway that authenticated the originating delegate, so
   * gateway-stamped fields are trusted within a single Aether cluster.
   * Federated/multi-cluster topologies that introduce untrusted relays MUST
   * either re-resolve via the resolver path or extend this message with a
   * signed attestation.
   * 
   * Empty when unset (e.g. direct mode, plain SendMessage path, or older
   * gateway that doesn't yet stamp resolved info — terminators fall back to
   * the resolver path in that case).
   */
  'resolved': (_aether_v1_ResolvedAuthorityInfo__Output | null);
}
