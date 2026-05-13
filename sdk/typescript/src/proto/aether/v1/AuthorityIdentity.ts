// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';

/**
 * AuthorityIdentity bundles the principal-set associated with a grant.
 * Used in new fields/messages going forward to avoid field drift between
 * the grant projections (ACLAuthorityGrantInfo, AuthorityGrantInfo,
 * ResolvedAuthorityInfo). Existing projections remain byte-identical for
 * wire compatibility; new code should prefer this sub-message.
 */
export interface AuthorityIdentity {
  'subject'?: (_aether_v1_PrincipalRef | null);
  'rootSubject'?: (_aether_v1_PrincipalRef | null);
  'delegate'?: (_aether_v1_PrincipalRef | null);
  'issuedBy'?: (_aether_v1_PrincipalRef | null);
}

/**
 * AuthorityIdentity bundles the principal-set associated with a grant.
 * Used in new fields/messages going forward to avoid field drift between
 * the grant projections (ACLAuthorityGrantInfo, AuthorityGrantInfo,
 * ResolvedAuthorityInfo). Existing projections remain byte-identical for
 * wire compatibility; new code should prefer this sub-message.
 */
export interface AuthorityIdentity__Output {
  'subject': (_aether_v1_PrincipalRef__Output | null);
  'rootSubject': (_aether_v1_PrincipalRef__Output | null);
  'delegate': (_aether_v1_PrincipalRef__Output | null);
  'issuedBy': (_aether_v1_PrincipalRef__Output | null);
}
