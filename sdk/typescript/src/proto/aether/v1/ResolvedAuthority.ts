// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { AuthorityGrantInfo as _aether_v1_AuthorityGrantInfo, AuthorityGrantInfo__Output as _aether_v1_AuthorityGrantInfo__Output } from '../../aether/v1/AuthorityGrantInfo';

/**
 * ResolvedAuthority is the validated authority envelope returned to authorized
 * callers. The grant projection (AuthorityGrantInfo) is a subset of the
 * internal AuthorityGrant — it intentionally omits ResourceScope,
 * OperationScope, and Metadata to avoid leaking shape that the terminator
 * doesn't need.
 */
export interface ResolvedAuthority {
  'actor'?: (_aether_v1_PrincipalRef | null);
  'subject'?: (_aether_v1_PrincipalRef | null);
  'grant'?: (_aether_v1_AuthorityGrantInfo | null);
}

/**
 * ResolvedAuthority is the validated authority envelope returned to authorized
 * callers. The grant projection (AuthorityGrantInfo) is a subset of the
 * internal AuthorityGrant — it intentionally omits ResourceScope,
 * OperationScope, and Metadata to avoid leaking shape that the terminator
 * doesn't need.
 */
export interface ResolvedAuthority__Output {
  'actor': (_aether_v1_PrincipalRef__Output | null);
  'subject': (_aether_v1_PrincipalRef__Output | null);
  'grant': (_aether_v1_AuthorityGrantInfo__Output | null);
}
