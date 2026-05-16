// Original file: aether.proto

import type { AuthorityRequestResourceScopeEntry as _aether_v1_AuthorityRequestResourceScopeEntry, AuthorityRequestResourceScopeEntry__Output as _aether_v1_AuthorityRequestResourceScopeEntry__Output } from '../../aether/v1/AuthorityRequestResourceScopeEntry';
import type { AccessLevel as _aether_v1_AccessLevel, AccessLevel__Output as _aether_v1_AccessLevel__Output } from '../../aether/v1/AccessLevel';
import type { Long } from '@grpc/proto-loader';

// Original file: aether.proto

export const _aether_v1_ResolveAuthorityRequestPayload_Decision = {
  DECISION_UNSPECIFIED: 'DECISION_UNSPECIFIED',
  APPROVE: 'APPROVE',
  DENY: 'DENY',
} as const;

export type _aether_v1_ResolveAuthorityRequestPayload_Decision =
  | 'DECISION_UNSPECIFIED'
  | 0
  | 'APPROVE'
  | 1
  | 'DENY'
  | 2

export type _aether_v1_ResolveAuthorityRequestPayload_Decision__Output = typeof _aether_v1_ResolveAuthorityRequestPayload_Decision[keyof typeof _aether_v1_ResolveAuthorityRequestPayload_Decision]

/**
 * ResolveAuthorityRequestPayload is the body of an AuthorityRequestOperation
 * with op=RESOLVE -- the approver's verdict. (Note: distinct from the
 * top-level `ResolveAuthorityRequest` message used for grant validation.)
 */
export interface ResolveAuthorityRequestPayload {
  'decision'?: (_aether_v1_ResolveAuthorityRequestPayload_Decision);
  /**
   * Optional approver-side scope refinements; populated when the approver wants
   * to grant a NARROWER authority than requested. Empty fields fall back to
   * the request's values. (Approvers cannot broaden; the gateway intersects.)
   */
  'grantedWorkspaceScope'?: (string)[];
  'grantedResourceScope'?: (_aether_v1_AuthorityRequestResourceScopeEntry)[];
  'grantedOperationScope'?: (string)[];
  'grantedAccessLevel'?: (_aether_v1_AccessLevel);
  'grantedDurationSeconds'?: (number | string | Long);
  /**
   * human-readable reason (esp. for DENY)
   */
  'reason'?: (string);
  'mayDelegate'?: (boolean);
  'remainingHops'?: (number);
}

/**
 * ResolveAuthorityRequestPayload is the body of an AuthorityRequestOperation
 * with op=RESOLVE -- the approver's verdict. (Note: distinct from the
 * top-level `ResolveAuthorityRequest` message used for grant validation.)
 */
export interface ResolveAuthorityRequestPayload__Output {
  'decision': (_aether_v1_ResolveAuthorityRequestPayload_Decision__Output);
  /**
   * Optional approver-side scope refinements; populated when the approver wants
   * to grant a NARROWER authority than requested. Empty fields fall back to
   * the request's values. (Approvers cannot broaden; the gateway intersects.)
   */
  'grantedWorkspaceScope': (string)[];
  'grantedResourceScope': (_aether_v1_AuthorityRequestResourceScopeEntry__Output)[];
  'grantedOperationScope': (string)[];
  'grantedAccessLevel': (_aether_v1_AccessLevel__Output);
  'grantedDurationSeconds': (string);
  /**
   * human-readable reason (esp. for DENY)
   */
  'reason': (string);
  'mayDelegate': (boolean);
  'remainingHops': (number);
}
