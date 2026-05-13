// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { Long } from '@grpc/proto-loader';

/**
 * AuthorityGrantDeriveForTargetRequest derives a child grant idempotently.
 * If the actor already holds a visible derived grant matching
 * (parent_grant_id, target, audience_type, audience_id) that is still
 * active, the existing grant is returned. Otherwise a new grant is minted
 * using the supplied parameters via the standard derive flow.
 */
export interface AuthorityGrantDeriveForTargetRequest {
  'parentGrantId'?: (string);
  /**
   * The downstream delegate
   */
  'target'?: (_aether_v1_PrincipalRef | null);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'operationScope'?: (string)[];
  'maxAccessLevel'?: (number);
  'expiresAt'?: (number | string | Long);
  'renewableUntil'?: (number | string | Long);
  'mayDelegate'?: (boolean);
  'remainingHops'?: (number);
  'reason'?: (string);
}

/**
 * AuthorityGrantDeriveForTargetRequest derives a child grant idempotently.
 * If the actor already holds a visible derived grant matching
 * (parent_grant_id, target, audience_type, audience_id) that is still
 * active, the existing grant is returned. Otherwise a new grant is minted
 * using the supplied parameters via the standard derive flow.
 */
export interface AuthorityGrantDeriveForTargetRequest__Output {
  'parentGrantId': (string);
  /**
   * The downstream delegate
   */
  'target': (_aether_v1_PrincipalRef__Output | null);
  'audienceType': (string);
  'audienceId': (string);
  'operationScope': (string)[];
  'maxAccessLevel': (number);
  'expiresAt': (string);
  'renewableUntil': (string);
  'mayDelegate': (boolean);
  'remainingHops': (number);
  'reason': (string);
}
