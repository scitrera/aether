// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';

/**
 * ResolveAuthorityRequest asks the gateway to validate a grant against the
 * caller's actor identity and the live audience, and return a public-safe
 * projection. When `actor` is empty the gateway substitutes the calling
 * session's authenticated identity (the common case — terminators don't need
 * to fill it in for self-resolution).
 */
export interface ResolveAuthorityRequest {
  'requestId'?: (string);
  /**
   * Empty → use caller's session identity.
   */
  'actor'?: (_aether_v1_PrincipalRef | null);
  'grantId'?: (string);
  'subject'?: (_aether_v1_PrincipalRef | null);
  /**
   * Audience is supplied by the caller for non-trivial cases. The gateway
   * additionally enforces validateGrantAudience using its own session/task
   * tracking, so audience here is informational/correlation, not the source
   * of truth for liveness.
   */
  'audienceType'?: (string);
  'audienceId'?: (string);
}

/**
 * ResolveAuthorityRequest asks the gateway to validate a grant against the
 * caller's actor identity and the live audience, and return a public-safe
 * projection. When `actor` is empty the gateway substitutes the calling
 * session's authenticated identity (the common case — terminators don't need
 * to fill it in for self-resolution).
 */
export interface ResolveAuthorityRequest__Output {
  'requestId': (string);
  /**
   * Empty → use caller's session identity.
   */
  'actor': (_aether_v1_PrincipalRef__Output | null);
  'grantId': (string);
  'subject': (_aether_v1_PrincipalRef__Output | null);
  /**
   * Audience is supplied by the caller for non-trivial cases. The gateway
   * additionally enforces validateGrantAudience using its own session/task
   * tracking, so audience here is informational/correlation, not the source
   * of truth for liveness.
   */
  'audienceType': (string);
  'audienceId': (string);
}
