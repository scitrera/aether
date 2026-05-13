// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

export interface ErrorResponse {
  'code'?: (string);
  'message'?: (string);
  /**
   * Whether the client should retry this request
   */
  'retryable'?: (boolean);
  /**
   * Suggested retry delay in milliseconds (0 = use default backoff)
   */
  'retryAfterMs'?: (number | string | Long);
}

export interface ErrorResponse__Output {
  'code': (string);
  'message': (string);
  /**
   * Whether the client should retry this request
   */
  'retryable': (boolean);
  /**
   * Suggested retry delay in milliseconds (0 = use default backoff)
   */
  'retryAfterMs': (string);
}
