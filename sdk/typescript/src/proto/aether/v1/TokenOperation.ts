// Original file: aether.proto

import type { TokenCreateRequest as _aether_v1_TokenCreateRequest, TokenCreateRequest__Output as _aether_v1_TokenCreateRequest__Output } from '../../aether/v1/TokenCreateRequest';
import type { TokenFilter as _aether_v1_TokenFilter, TokenFilter__Output as _aether_v1_TokenFilter__Output } from '../../aether/v1/TokenFilter';

// Original file: aether.proto

export const _aether_v1_TokenOperation_OpType = {
  /**
   * GET /api/tokens - List all API tokens
   */
  LIST: 'LIST',
  /**
   * GET /api/tokens/{token_id} - Get a specific token
   */
  GET: 'GET',
  /**
   * POST /api/tokens - Create a new API token
   */
  CREATE: 'CREATE',
  /**
   * DELETE /api/tokens/{token_id} - Delete a token
   */
  DELETE: 'DELETE',
  /**
   * POST /api/tokens/{token_id}/revoke - Revoke a token
   */
  REVOKE: 'REVOKE',
} as const;

export type _aether_v1_TokenOperation_OpType =
  /**
   * GET /api/tokens - List all API tokens
   */
  | 'LIST'
  | 0
  /**
   * GET /api/tokens/{token_id} - Get a specific token
   */
  | 'GET'
  | 1
  /**
   * POST /api/tokens - Create a new API token
   */
  | 'CREATE'
  | 2
  /**
   * DELETE /api/tokens/{token_id} - Delete a token
   */
  | 'DELETE'
  | 3
  /**
   * POST /api/tokens/{token_id}/revoke - Revoke a token
   */
  | 'REVOKE'
  | 4

export type _aether_v1_TokenOperation_OpType__Output = typeof _aether_v1_TokenOperation_OpType[keyof typeof _aether_v1_TokenOperation_OpType]

/**
 * TokenOperation allows clients to manage API tokens through the gRPC streaming
 * interface. This provides feature parity with the REST Admin API for token management.
 * REST equivalents:
 * - GET /api/tokens → LIST
 * - POST /api/tokens → CREATE
 * - GET /api/tokens/{token_id} → GET
 * - DELETE /api/tokens/{token_id} → DELETE
 * - POST /api/tokens/{token_id}/revoke → REVOKE
 */
export interface TokenOperation {
  'op'?: (_aether_v1_TokenOperation_OpType);
  /**
   * For GET, DELETE, REVOKE: the token ID to operate on
   */
  'tokenId'?: (string);
  /**
   * For CREATE: token creation parameters
   */
  'createRequest'?: (_aether_v1_TokenCreateRequest | null);
  /**
   * For LIST: optional filter parameters
   */
  'filter'?: (_aether_v1_TokenFilter | null);
  /**
   * Correlation ID for request/response matching
   */
  'requestId'?: (string);
}

/**
 * TokenOperation allows clients to manage API tokens through the gRPC streaming
 * interface. This provides feature parity with the REST Admin API for token management.
 * REST equivalents:
 * - GET /api/tokens → LIST
 * - POST /api/tokens → CREATE
 * - GET /api/tokens/{token_id} → GET
 * - DELETE /api/tokens/{token_id} → DELETE
 * - POST /api/tokens/{token_id}/revoke → REVOKE
 */
export interface TokenOperation__Output {
  'op': (_aether_v1_TokenOperation_OpType__Output);
  /**
   * For GET, DELETE, REVOKE: the token ID to operate on
   */
  'tokenId': (string);
  /**
   * For CREATE: token creation parameters
   */
  'createRequest': (_aether_v1_TokenCreateRequest__Output | null);
  /**
   * For LIST: optional filter parameters
   */
  'filter': (_aether_v1_TokenFilter__Output | null);
  /**
   * Correlation ID for request/response matching
   */
  'requestId': (string);
}
