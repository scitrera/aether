// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { AuthorityRequestResourceScopeEntry as _aether_v1_AuthorityRequestResourceScopeEntry, AuthorityRequestResourceScopeEntry__Output as _aether_v1_AuthorityRequestResourceScopeEntry__Output } from '../../aether/v1/AuthorityRequestResourceScopeEntry';
import type { AccessLevel as _aether_v1_AccessLevel, AccessLevel__Output as _aether_v1_AccessLevel__Output } from '../../aether/v1/AccessLevel';
import type { AuthorityRequestRoutingTarget as _aether_v1_AuthorityRequestRoutingTarget, AuthorityRequestRoutingTarget__Output as _aether_v1_AuthorityRequestRoutingTarget__Output } from '../../aether/v1/AuthorityRequestRoutingTarget';
import type { Long } from '@grpc/proto-loader';

/**
 * CreateAuthorityRequestPayload is the body of an AuthorityRequestOperation
 * with op=CREATE -- the request the task submits when asking for elevation.
 */
export interface CreateAuthorityRequestPayload {
  /**
   * empty = caller's session identity
   */
  'requestingActor'?: (_aether_v1_PrincipalRef | null);
  /**
   * empty = requesting_actor
   */
  'targetSubject'?: (_aether_v1_PrincipalRef | null);
  'desiredWorkspaceScope'?: (string)[];
  'desiredResourceScope'?: (_aether_v1_AuthorityRequestResourceScopeEntry)[];
  'desiredOperationScope'?: (string)[];
  'requestedAccessLevel'?: (_aether_v1_AccessLevel);
  'requestedDurationSeconds'?: (number | string | Long);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'routingTarget'?: (_aether_v1_AuthorityRequestRoutingTarget | null);
  'reason'?: (string);
  'taskId'?: (string);
  'metadata'?: ({[key: string]: string});
}

/**
 * CreateAuthorityRequestPayload is the body of an AuthorityRequestOperation
 * with op=CREATE -- the request the task submits when asking for elevation.
 */
export interface CreateAuthorityRequestPayload__Output {
  /**
   * empty = caller's session identity
   */
  'requestingActor': (_aether_v1_PrincipalRef__Output | null);
  /**
   * empty = requesting_actor
   */
  'targetSubject': (_aether_v1_PrincipalRef__Output | null);
  'desiredWorkspaceScope': (string)[];
  'desiredResourceScope': (_aether_v1_AuthorityRequestResourceScopeEntry__Output)[];
  'desiredOperationScope': (string)[];
  'requestedAccessLevel': (_aether_v1_AccessLevel__Output);
  'requestedDurationSeconds': (string);
  'audienceType': (string);
  'audienceId': (string);
  'routingTarget': (_aether_v1_AuthorityRequestRoutingTarget__Output | null);
  'reason': (string);
  'taskId': (string);
  'metadata': ({[key: string]: string});
}
