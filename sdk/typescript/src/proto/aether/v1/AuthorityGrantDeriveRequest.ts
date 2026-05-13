// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { ACLAuthorityGrantResourceScopeEntry as _aether_v1_ACLAuthorityGrantResourceScopeEntry, ACLAuthorityGrantResourceScopeEntry__Output as _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output } from '../../aether/v1/ACLAuthorityGrantResourceScopeEntry';
import type { Long } from '@grpc/proto-loader';

/**
 * AuthorityGrantDeriveRequest derives a child grant for a downstream delegate.
 */
export interface AuthorityGrantDeriveRequest {
  'parentGrantId'?: (string);
  'delegate'?: (_aether_v1_PrincipalRef | null);
  'workspaceScope'?: (string)[];
  'resourceScope'?: (_aether_v1_ACLAuthorityGrantResourceScopeEntry)[];
  'operationScope'?: (string)[];
  'maxAccessLevel'?: (number);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'validWhileAudienceActive'?: (boolean);
  'expiresAt'?: (number | string | Long);
  'renewableUntil'?: (number | string | Long);
  'mayDelegate'?: (boolean);
  'remainingHops'?: (number);
  'reason'?: (string);
  'metadata'?: ({[key: string]: string});
}

/**
 * AuthorityGrantDeriveRequest derives a child grant for a downstream delegate.
 */
export interface AuthorityGrantDeriveRequest__Output {
  'parentGrantId': (string);
  'delegate': (_aether_v1_PrincipalRef__Output | null);
  'workspaceScope': (string)[];
  'resourceScope': (_aether_v1_ACLAuthorityGrantResourceScopeEntry__Output)[];
  'operationScope': (string)[];
  'maxAccessLevel': (number);
  'audienceType': (string);
  'audienceId': (string);
  'validWhileAudienceActive': (boolean);
  'expiresAt': (string);
  'renewableUntil': (string);
  'mayDelegate': (boolean);
  'remainingHops': (number);
  'reason': (string);
  'metadata': ({[key: string]: string});
}
