// Original file: aether.proto

import type { AuthorityRequest as _aether_v1_AuthorityRequest, AuthorityRequest__Output as _aether_v1_AuthorityRequest__Output } from '../../aether/v1/AuthorityRequest';

/**
 * AuthorityRequestOperationResponse is the downstream reply for any
 * AuthorityRequestOperation.
 */
export interface AuthorityRequestOperationResponse {
  'success'?: (boolean);
  'error'?: (string);
  'clientRequestId'?: (string);
  /**
   * populated for GET / CREATE / RESOLVE / CANCEL
   */
  'request'?: (_aether_v1_AuthorityRequest | null);
  /**
   * populated for LIST_PENDING
   */
  'requests'?: (_aether_v1_AuthorityRequest)[];
  'totalCount'?: (number);
}

/**
 * AuthorityRequestOperationResponse is the downstream reply for any
 * AuthorityRequestOperation.
 */
export interface AuthorityRequestOperationResponse__Output {
  'success': (boolean);
  'error': (string);
  'clientRequestId': (string);
  /**
   * populated for GET / CREATE / RESOLVE / CANCEL
   */
  'request': (_aether_v1_AuthorityRequest__Output | null);
  /**
   * populated for LIST_PENDING
   */
  'requests': (_aether_v1_AuthorityRequest__Output)[];
  'totalCount': (number);
}
