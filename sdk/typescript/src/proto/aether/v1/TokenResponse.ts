// Original file: aether.proto

import type { TokenInfo as _aether_v1_TokenInfo, TokenInfo__Output as _aether_v1_TokenInfo__Output } from '../../aether/v1/TokenInfo';

/**
 * TokenResponse is sent in response to TokenOperation.
 */
export interface TokenResponse {
  'success'?: (boolean);
  /**
   * Error message if success is false
   */
  'error'?: (string);
  /**
   * Human-readable result message (for DELETE, REVOKE operations)
   */
  'message'?: (string);
  /**
   * For GET operation (single token)
   */
  'token'?: (_aether_v1_TokenInfo | null);
  /**
   * For LIST operation (multiple tokens)
   */
  'tokens'?: (_aether_v1_TokenInfo)[];
  /**
   * For LIST: total count
   */
  'totalCount'?: (number);
  /**
   * For CREATE operation: the plaintext token (only available at creation time)
   */
  'plaintextToken'?: (string);
  /**
   * For CREATE operation: the created token info
   */
  'createdToken'?: (_aether_v1_TokenInfo | null);
  /**
   * Correlation ID echoed from the originating request
   */
  'requestId'?: (string);
}

/**
 * TokenResponse is sent in response to TokenOperation.
 */
export interface TokenResponse__Output {
  'success': (boolean);
  /**
   * Error message if success is false
   */
  'error': (string);
  /**
   * Human-readable result message (for DELETE, REVOKE operations)
   */
  'message': (string);
  /**
   * For GET operation (single token)
   */
  'token': (_aether_v1_TokenInfo__Output | null);
  /**
   * For LIST operation (multiple tokens)
   */
  'tokens': (_aether_v1_TokenInfo__Output)[];
  /**
   * For LIST: total count
   */
  'totalCount': (number);
  /**
   * For CREATE operation: the plaintext token (only available at creation time)
   */
  'plaintextToken': (string);
  /**
   * For CREATE operation: the created token info
   */
  'createdToken': (_aether_v1_TokenInfo__Output | null);
  /**
   * Correlation ID echoed from the originating request
   */
  'requestId': (string);
}
