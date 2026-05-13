// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

export interface ACLRenewAuthorityGrantRequest {
  'grantId'?: (string);
  'expiresAt'?: (number | string | Long);
  /**
   * Convenience: extend the lease by N seconds from the current server time.
   * When > 0 this takes precedence over expires_at. The resulting expiration
   * is still clamped against the grant's renewable_until window and (for
   * derived grants) the parent grant's expiry.
   */
  'extendSeconds'?: (number);
}

export interface ACLRenewAuthorityGrantRequest__Output {
  'grantId': (string);
  'expiresAt': (string);
  /**
   * Convenience: extend the lease by N seconds from the current server time.
   * When > 0 this takes precedence over expires_at. The resulting expiration
   * is still clamped against the grant's renewable_until window and (for
   * derived grants) the parent grant's expiry.
   */
  'extendSeconds': (number);
}
