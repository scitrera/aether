// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * AuthorityGrantRevocation is pushed to a connected delegate when one of
 * their grants is revoked (directly or by cascade from a parent revocation).
 * Lets SDKs proactively drop cached grants and re-mint instead of waiting
 * for the next call to fail. Best-effort: the gateway logs send failures
 * but does not retry.
 */
export interface AuthorityGrantRevocation {
  'grantId'?: (string);
  'rootGrantId'?: (string);
  'reason'?: (string);
  'revokedAt'?: (number | string | Long);
  /**
   * True if revocation cascaded from parent
   */
  'cascade'?: (boolean);
}

/**
 * AuthorityGrantRevocation is pushed to a connected delegate when one of
 * their grants is revoked (directly or by cascade from a parent revocation).
 * Lets SDKs proactively drop cached grants and re-mint instead of waiting
 * for the next call to fail. Best-effort: the gateway logs send failures
 * but does not retry.
 */
export interface AuthorityGrantRevocation__Output {
  'grantId': (string);
  'rootGrantId': (string);
  'reason': (string);
  'revokedAt': (string);
  /**
   * True if revocation cascaded from parent
   */
  'cascade': (boolean);
}
