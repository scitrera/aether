// Original file: aether.proto

import type { AuthorityGrantExchangeRequest as _aether_v1_AuthorityGrantExchangeRequest, AuthorityGrantExchangeRequest__Output as _aether_v1_AuthorityGrantExchangeRequest__Output } from '../../aether/v1/AuthorityGrantExchangeRequest';
import type { AuthorityGrantDeriveRequest as _aether_v1_AuthorityGrantDeriveRequest, AuthorityGrantDeriveRequest__Output as _aether_v1_AuthorityGrantDeriveRequest__Output } from '../../aether/v1/AuthorityGrantDeriveRequest';
import type { ACLRenewAuthorityGrantRequest as _aether_v1_ACLRenewAuthorityGrantRequest, ACLRenewAuthorityGrantRequest__Output as _aether_v1_ACLRenewAuthorityGrantRequest__Output } from '../../aether/v1/ACLRenewAuthorityGrantRequest';
import type { AuthorityGrantListRequest as _aether_v1_AuthorityGrantListRequest, AuthorityGrantListRequest__Output as _aether_v1_AuthorityGrantListRequest__Output } from '../../aether/v1/AuthorityGrantListRequest';
import type { AuthorityGrantBatchExchangeRequest as _aether_v1_AuthorityGrantBatchExchangeRequest, AuthorityGrantBatchExchangeRequest__Output as _aether_v1_AuthorityGrantBatchExchangeRequest__Output } from '../../aether/v1/AuthorityGrantBatchExchangeRequest';
import type { AuthorityGrantDeriveForTargetRequest as _aether_v1_AuthorityGrantDeriveForTargetRequest, AuthorityGrantDeriveForTargetRequest__Output as _aether_v1_AuthorityGrantDeriveForTargetRequest__Output } from '../../aether/v1/AuthorityGrantDeriveForTargetRequest';

// Original file: aether.proto

export const _aether_v1_AuthorityGrantOperation_OpType = {
  /**
   * Bootstrap a grant for the current actor
   */
  EXCHANGE: 'EXCHANGE',
  /**
   * Derive a child grant for another delegate from an existing parent grant
   */
  DERIVE: 'DERIVE',
  /**
   * Get a visible authority grant
   */
  GET: 'GET',
  /**
   * Renew a visible authority grant lease
   */
  RENEW: 'RENEW',
  /**
   * Revoke a visible authority grant
   */
  REVOKE: 'REVOKE',
  /**
   * List grants where actor is delegate or subject
   */
  LIST_MY_GRANTS: 'LIST_MY_GRANTS',
  /**
   * List grants where actor is subject (i.e., grants OTHERS hold on me)
   */
  LIST_GRANTS_ON_ME: 'LIST_GRANTS_ON_ME',
  /**
   * Multiple AuthorityGrantExchangeRequest in one round-trip
   */
  BATCH_EXCHANGE: 'BATCH_EXCHANGE',
  /**
   * Idempotent derive: returns existing visible grant or mints new
   */
  DERIVE_FOR_TARGET: 'DERIVE_FOR_TARGET',
} as const;

export type _aether_v1_AuthorityGrantOperation_OpType =
  /**
   * Bootstrap a grant for the current actor
   */
  | 'EXCHANGE'
  | 0
  /**
   * Derive a child grant for another delegate from an existing parent grant
   */
  | 'DERIVE'
  | 1
  /**
   * Get a visible authority grant
   */
  | 'GET'
  | 2
  /**
   * Renew a visible authority grant lease
   */
  | 'RENEW'
  | 3
  /**
   * Revoke a visible authority grant
   */
  | 'REVOKE'
  | 4
  /**
   * List grants where actor is delegate or subject
   */
  | 'LIST_MY_GRANTS'
  | 5
  /**
   * List grants where actor is subject (i.e., grants OTHERS hold on me)
   */
  | 'LIST_GRANTS_ON_ME'
  | 6
  /**
   * Multiple AuthorityGrantExchangeRequest in one round-trip
   */
  | 'BATCH_EXCHANGE'
  | 7
  /**
   * Idempotent derive: returns existing visible grant or mints new
   */
  | 'DERIVE_FOR_TARGET'
  | 8

export type _aether_v1_AuthorityGrantOperation_OpType__Output = typeof _aether_v1_AuthorityGrantOperation_OpType[keyof typeof _aether_v1_AuthorityGrantOperation_OpType]

/**
 * AuthorityGrantOperation allows ordinary streaming clients to exchange,
 * derive, inspect, renew, and revoke authority grants without going through
 * the admin ACL surface.
 */
export interface AuthorityGrantOperation {
  'op'?: (_aether_v1_AuthorityGrantOperation_OpType);
  /**
   * For GET, RENEW, REVOKE: the grant ID to operate on
   */
  'grantId'?: (string);
  /**
   * For EXCHANGE: authority grant exchange request
   */
  'exchangeRequest'?: (_aether_v1_AuthorityGrantExchangeRequest | null);
  /**
   * For DERIVE: child grant derivation request
   */
  'deriveRequest'?: (_aether_v1_AuthorityGrantDeriveRequest | null);
  /**
   * For RENEW: grant renewal request
   */
  'renewRequest'?: (_aether_v1_ACLRenewAuthorityGrantRequest | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
  /**
   * For LIST_MY_GRANTS / LIST_GRANTS_ON_ME
   */
  'listRequest'?: (_aether_v1_AuthorityGrantListRequest | null);
  /**
   * For BATCH_EXCHANGE
   */
  'batchExchangeRequest'?: (_aether_v1_AuthorityGrantBatchExchangeRequest | null);
  /**
   * For DERIVE_FOR_TARGET
   */
  'deriveForTargetRequest'?: (_aether_v1_AuthorityGrantDeriveForTargetRequest | null);
}

/**
 * AuthorityGrantOperation allows ordinary streaming clients to exchange,
 * derive, inspect, renew, and revoke authority grants without going through
 * the admin ACL surface.
 */
export interface AuthorityGrantOperation__Output {
  'op': (_aether_v1_AuthorityGrantOperation_OpType__Output);
  /**
   * For GET, RENEW, REVOKE: the grant ID to operate on
   */
  'grantId': (string);
  /**
   * For EXCHANGE: authority grant exchange request
   */
  'exchangeRequest': (_aether_v1_AuthorityGrantExchangeRequest__Output | null);
  /**
   * For DERIVE: child grant derivation request
   */
  'deriveRequest': (_aether_v1_AuthorityGrantDeriveRequest__Output | null);
  /**
   * For RENEW: grant renewal request
   */
  'renewRequest': (_aether_v1_ACLRenewAuthorityGrantRequest__Output | null);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
  /**
   * For LIST_MY_GRANTS / LIST_GRANTS_ON_ME
   */
  'listRequest': (_aether_v1_AuthorityGrantListRequest__Output | null);
  /**
   * For BATCH_EXCHANGE
   */
  'batchExchangeRequest': (_aether_v1_AuthorityGrantBatchExchangeRequest__Output | null);
  /**
   * For DERIVE_FOR_TARGET
   */
  'deriveForTargetRequest': (_aether_v1_AuthorityGrantDeriveForTargetRequest__Output | null);
}
