// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * AuthoritySpan bundles the lifecycle and audience-binding fields of a
 * grant. Used in new fields/messages going forward; existing projections
 * remain byte-identical for wire compatibility.
 */
export interface AuthoritySpan {
  'workspaceScope'?: (string)[];
  'maxAccessLevel'?: (number);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'validWhileAudienceActive'?: (boolean);
  'expiresAt'?: (number | string | Long);
  'renewableUntil'?: (number | string | Long);
  'revoked'?: (boolean);
}

/**
 * AuthoritySpan bundles the lifecycle and audience-binding fields of a
 * grant. Used in new fields/messages going forward; existing projections
 * remain byte-identical for wire compatibility.
 */
export interface AuthoritySpan__Output {
  'workspaceScope': (string)[];
  'maxAccessLevel': (number);
  'audienceType': (string);
  'audienceId': (string);
  'validWhileAudienceActive': (boolean);
  'expiresAt': (string);
  'renewableUntil': (string);
  'revoked': (boolean);
}
