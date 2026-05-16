// Original file: aether.proto

import type { CreateAuthorityRequestPayload as _aether_v1_CreateAuthorityRequestPayload, CreateAuthorityRequestPayload__Output as _aether_v1_CreateAuthorityRequestPayload__Output } from '../../aether/v1/CreateAuthorityRequestPayload';
import type { ResolveAuthorityRequestPayload as _aether_v1_ResolveAuthorityRequestPayload, ResolveAuthorityRequestPayload__Output as _aether_v1_ResolveAuthorityRequestPayload__Output } from '../../aether/v1/ResolveAuthorityRequestPayload';
import type { AuthorityRequestListFilter as _aether_v1_AuthorityRequestListFilter, AuthorityRequestListFilter__Output as _aether_v1_AuthorityRequestListFilter__Output } from '../../aether/v1/AuthorityRequestListFilter';

// Original file: aether.proto

export const _aether_v1_AuthorityRequestOperation_OpType = {
  AUTHORITY_REQUEST_OP_UNSPECIFIED: 'AUTHORITY_REQUEST_OP_UNSPECIFIED',
  /**
   * submit a new request, payload = CreateAuthorityRequestPayload
   */
  CREATE: 'CREATE',
  /**
   * fetch by request_id
   */
  GET: 'GET',
  /**
   * list pending requests targeting the caller (principal or capability)
   */
  LIST_PENDING: 'LIST_PENDING',
  /**
   * approve or deny, payload = ResolveAuthorityRequestPayload
   */
  RESOLVE: 'RESOLVE',
  /**
   * requester withdraws an outstanding request
   */
  CANCEL: 'CANCEL',
} as const;

export type _aether_v1_AuthorityRequestOperation_OpType =
  | 'AUTHORITY_REQUEST_OP_UNSPECIFIED'
  | 0
  /**
   * submit a new request, payload = CreateAuthorityRequestPayload
   */
  | 'CREATE'
  | 1
  /**
   * fetch by request_id
   */
  | 'GET'
  | 2
  /**
   * list pending requests targeting the caller (principal or capability)
   */
  | 'LIST_PENDING'
  | 3
  /**
   * approve or deny, payload = ResolveAuthorityRequestPayload
   */
  | 'RESOLVE'
  | 4
  /**
   * requester withdraws an outstanding request
   */
  | 'CANCEL'
  | 5

export type _aether_v1_AuthorityRequestOperation_OpType__Output = typeof _aether_v1_AuthorityRequestOperation_OpType[keyof typeof _aether_v1_AuthorityRequestOperation_OpType]

/**
 * AuthorityRequestOperation is the upstream op for the request lifecycle.
 */
export interface AuthorityRequestOperation {
  'op'?: (_aether_v1_AuthorityRequestOperation_OpType);
  /**
   * for GET / RESOLVE / CANCEL; CREATE leaves empty (server-assigned)
   */
  'requestId'?: (string);
  'create'?: (_aether_v1_CreateAuthorityRequestPayload | null);
  'resolve'?: (_aether_v1_ResolveAuthorityRequestPayload | null);
  'listFilter'?: (_aether_v1_AuthorityRequestListFilter | null);
  /**
   * correlation
   */
  'clientRequestId'?: (string);
  /**
   * Generic free-text reason; primarily used by CANCEL to record why the
   * requester withdrew. CREATE/RESOLVE carry their own reason inside their
   * respective payloads.
   */
  'reason'?: (string);
}

/**
 * AuthorityRequestOperation is the upstream op for the request lifecycle.
 */
export interface AuthorityRequestOperation__Output {
  'op': (_aether_v1_AuthorityRequestOperation_OpType__Output);
  /**
   * for GET / RESOLVE / CANCEL; CREATE leaves empty (server-assigned)
   */
  'requestId': (string);
  'create': (_aether_v1_CreateAuthorityRequestPayload__Output | null);
  'resolve': (_aether_v1_ResolveAuthorityRequestPayload__Output | null);
  'listFilter': (_aether_v1_AuthorityRequestListFilter__Output | null);
  /**
   * correlation
   */
  'clientRequestId': (string);
  /**
   * Generic free-text reason; primarily used by CANCEL to record why the
   * requester withdrew. CREATE/RESOLVE carry their own reason inside their
   * respective payloads.
   */
  'reason': (string);
}
