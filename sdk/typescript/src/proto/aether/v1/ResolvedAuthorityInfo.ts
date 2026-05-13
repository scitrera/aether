// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { Long } from '@grpc/proto-loader';

/**
 * ResolvedAuthorityInfo is the public-safe projection of an AuthorityGrant
 * that the gateway can stamp onto a ProxyHttpRequest after it has validated
 * the request's AuthorizationContext. Mirrors the fields that the SDK's
 * `_overlay_resolved_authority` consumes when minting the X-Auth-* headers
 * the backend uses to enforce caps (workspace scope, max access level,
 * audience binding, root-subject for audit).
 */
export interface ResolvedAuthorityInfo {
  'rootSubject'?: (_aether_v1_PrincipalRef | null);
  /**
   * "session", "task", "agent", "service"
   */
  'audienceType'?: (string);
  'audienceId'?: (string);
  /**
   * matches acl.AccessLevel numeric scale
   */
  'maxAccessLevel'?: (number);
  'workspaceScope'?: (string)[];
  /**
   * unix milliseconds; 0 means "no expiry recorded"
   */
  'expiresAtMs'?: (number | string | Long);
}

/**
 * ResolvedAuthorityInfo is the public-safe projection of an AuthorityGrant
 * that the gateway can stamp onto a ProxyHttpRequest after it has validated
 * the request's AuthorizationContext. Mirrors the fields that the SDK's
 * `_overlay_resolved_authority` consumes when minting the X-Auth-* headers
 * the backend uses to enforce caps (workspace scope, max access level,
 * audience binding, root-subject for audit).
 */
export interface ResolvedAuthorityInfo__Output {
  'rootSubject': (_aether_v1_PrincipalRef__Output | null);
  /**
   * "session", "task", "agent", "service"
   */
  'audienceType': (string);
  'audienceId': (string);
  /**
   * matches acl.AccessLevel numeric scale
   */
  'maxAccessLevel': (number);
  'workspaceScope': (string)[];
  /**
   * unix milliseconds; 0 means "no expiry recorded"
   */
  'expiresAtMs': (string);
}
