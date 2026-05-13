// Original file: aether.proto

import type { ACLAuthorityGrantResourceScopeEntry as _aether_v1_ACLAuthorityGrantResourceScopeEntry, ACLAuthorityGrantResourceScopeEntry__Output as _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output } from '../../aether/v1/ACLAuthorityGrantResourceScopeEntry';
import type { Long } from '@grpc/proto-loader';

/**
 * AuthorityGrantExchangeRequest bootstraps a grant for the current actor.
 * If source_session_id is empty, only a user may self-exchange.
 * If source_session_id is set, the caller must hold the exchange_authority_grants
 * permission and the referenced session must belong to an active user.
 * 
 * workspace_scope: list of workspace patterns the grant permits. Use the
 * magic value "_subject_workspaces" instead of "*" when the intent is to
 * inherit the workspace decision from the subject's own ACL (the subject
 * ACL check still runs and remains the security ceiling). "*" continues to
 * work but is treated by tooling as an over-broad grant.
 */
export interface AuthorityGrantExchangeRequest {
  'sourceSessionId'?: (string);
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
 * AuthorityGrantExchangeRequest bootstraps a grant for the current actor.
 * If source_session_id is empty, only a user may self-exchange.
 * If source_session_id is set, the caller must hold the exchange_authority_grants
 * permission and the referenced session must belong to an active user.
 * 
 * workspace_scope: list of workspace patterns the grant permits. Use the
 * magic value "_subject_workspaces" instead of "*" when the intent is to
 * inherit the workspace decision from the subject's own ACL (the subject
 * ACL check still runs and remains the security ceiling). "*" continues to
 * work but is treated by tooling as an over-broad grant.
 */
export interface AuthorityGrantExchangeRequest__Output {
  'sourceSessionId': (string);
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
