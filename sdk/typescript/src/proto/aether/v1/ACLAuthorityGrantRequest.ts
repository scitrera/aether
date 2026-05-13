// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { ACLAuthorityGrantResourceScopeEntry as _aether_v1_ACLAuthorityGrantResourceScopeEntry, ACLAuthorityGrantResourceScopeEntry__Output as _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output } from '../../aether/v1/ACLAuthorityGrantResourceScopeEntry';
import type { Long } from '@grpc/proto-loader';

export interface ACLAuthorityGrantRequest {
  'subject'?: (_aether_v1_PrincipalRef | null);
  'delegate'?: (_aether_v1_PrincipalRef | null);
  'issuedBy'?: (_aether_v1_PrincipalRef | null);
  'rootSubject'?: (_aether_v1_PrincipalRef | null);
  'parentGrantId'?: (string);
  'mayDelegate'?: (boolean);
  'remainingHops'?: (number);
  'workspaceScope'?: (string)[];
  'resourceScope'?: (_aether_v1_ACLAuthorityGrantResourceScopeEntry)[];
  'operationScope'?: (string)[];
  'maxAccessLevel'?: (number);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'validWhileAudienceActive'?: (boolean);
  'expiresAt'?: (number | string | Long);
  'renewableUntil'?: (number | string | Long);
  'reason'?: (string);
  'metadata'?: ({[key: string]: string});
}

export interface ACLAuthorityGrantRequest__Output {
  'subject': (_aether_v1_PrincipalRef__Output | null);
  'delegate': (_aether_v1_PrincipalRef__Output | null);
  'issuedBy': (_aether_v1_PrincipalRef__Output | null);
  'rootSubject': (_aether_v1_PrincipalRef__Output | null);
  'parentGrantId': (string);
  'mayDelegate': (boolean);
  'remainingHops': (number);
  'workspaceScope': (string)[];
  'resourceScope': (_aether_v1_ACLAuthorityGrantResourceScopeEntry__Output)[];
  'operationScope': (string)[];
  'maxAccessLevel': (number);
  'audienceType': (string);
  'audienceId': (string);
  'validWhileAudienceActive': (boolean);
  'expiresAt': (string);
  'renewableUntil': (string);
  'reason': (string);
  'metadata': ({[key: string]: string});
}
