// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * AuthorityGrantInfo is the public-safe projection of an AuthorityGrant.
 * Distinct from ACLAuthorityGrantInfo (which is admin-facing and exposes the
 * full grant). Field set is locked to what the terminator needs to mint
 * X-Auth-* headers.
 */
export interface AuthorityGrantInfo {
  'grantId'?: (string);
  'subjectType'?: (string);
  'subjectId'?: (string);
  'rootSubjectType'?: (string);
  'rootSubjectId'?: (string);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'maxAccessLevel'?: (number);
  'workspaceScope'?: (string)[];
  /**
   * Unix seconds; matches the rest of the schema.
   */
  'expiresAt'?: (number | string | Long);
  'revoked'?: (boolean);
}

/**
 * AuthorityGrantInfo is the public-safe projection of an AuthorityGrant.
 * Distinct from ACLAuthorityGrantInfo (which is admin-facing and exposes the
 * full grant). Field set is locked to what the terminator needs to mint
 * X-Auth-* headers.
 */
export interface AuthorityGrantInfo__Output {
  'grantId': (string);
  'subjectType': (string);
  'subjectId': (string);
  'rootSubjectType': (string);
  'rootSubjectId': (string);
  'audienceType': (string);
  'audienceId': (string);
  'maxAccessLevel': (number);
  'workspaceScope': (string)[];
  /**
   * Unix seconds; matches the rest of the schema.
   */
  'expiresAt': (string);
  'revoked': (boolean);
}
