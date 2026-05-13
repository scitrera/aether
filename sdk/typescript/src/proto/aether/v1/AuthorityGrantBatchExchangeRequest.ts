// Original file: aether.proto

import type { AuthorityGrantExchangeRequest as _aether_v1_AuthorityGrantExchangeRequest, AuthorityGrantExchangeRequest__Output as _aether_v1_AuthorityGrantExchangeRequest__Output } from '../../aether/v1/AuthorityGrantExchangeRequest';

/**
 * AuthorityGrantBatchExchangeRequest performs multiple exchanges in one
 * round-trip. Useful for clients bootstrapping several grants at session
 * start (e.g., one per audience) without N gateway round-trips.
 */
export interface AuthorityGrantBatchExchangeRequest {
  'requests'?: (_aether_v1_AuthorityGrantExchangeRequest)[];
  /**
   * If true, abort batch on first failure
   */
  'stopOnFirstError'?: (boolean);
}

/**
 * AuthorityGrantBatchExchangeRequest performs multiple exchanges in one
 * round-trip. Useful for clients bootstrapping several grants at session
 * start (e.g., one per audience) without N gateway round-trips.
 */
export interface AuthorityGrantBatchExchangeRequest__Output {
  'requests': (_aether_v1_AuthorityGrantExchangeRequest__Output)[];
  /**
   * If true, abort batch on first failure
   */
  'stopOnFirstError': (boolean);
}
