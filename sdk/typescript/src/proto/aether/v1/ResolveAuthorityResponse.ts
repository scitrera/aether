// Original file: aether.proto

import type { ResolvedAuthority as _aether_v1_ResolvedAuthority, ResolvedAuthority__Output as _aether_v1_ResolvedAuthority__Output } from '../../aether/v1/ResolvedAuthority';

/**
 * ResolveAuthorityResponse carries the validated, projected authority. On
 * failure (`ok=false`) `authority` is unset and `error` carries a stable
 * human-readable message. Per the spec, no grant fields are populated on
 * authorization denial.
 */
export interface ResolveAuthorityResponse {
  'requestId'?: (string);
  'ok'?: (boolean);
  'error'?: (string);
  'authority'?: (_aether_v1_ResolvedAuthority | null);
}

/**
 * ResolveAuthorityResponse carries the validated, projected authority. On
 * failure (`ok=false`) `authority` is unset and `error` carries a stable
 * human-readable message. Per the spec, no grant fields are populated on
 * authorization denial.
 */
export interface ResolveAuthorityResponse__Output {
  'requestId': (string);
  'ok': (boolean);
  'error': (string);
  'authority': (_aether_v1_ResolvedAuthority__Output | null);
}
