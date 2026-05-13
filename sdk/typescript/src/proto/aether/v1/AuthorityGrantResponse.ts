// Original file: aether.proto

import type { ACLAuthorityGrantInfo as _aether_v1_ACLAuthorityGrantInfo, ACLAuthorityGrantInfo__Output as _aether_v1_ACLAuthorityGrantInfo__Output } from '../../aether/v1/ACLAuthorityGrantInfo';

/**
 * AuthorityGrantResponse is sent in response to AuthorityGrantOperation.
 */
export interface AuthorityGrantResponse {
  'success'?: (boolean);
  'error'?: (string);
  'message'?: (string);
  'grant'?: (_aether_v1_ACLAuthorityGrantInfo | null);
  'requestId'?: (string);
  /**
   * For LIST_MY_GRANTS / LIST_GRANTS_ON_ME / BATCH_EXCHANGE results.
   */
  'grants'?: (_aether_v1_ACLAuthorityGrantInfo)[];
  /**
   * Total matching rows ignoring pagination (LIST_*) or count of returned
   * grants (BATCH_EXCHANGE).
   */
  'total'?: (number);
  /**
   * Server's hint to clients on how often to revalidate cached grants
   * (seconds). Zero means "no hint" — clients fall back to their own policy.
   */
  'cacheHintTtlSeconds'?: (number);
}

/**
 * AuthorityGrantResponse is sent in response to AuthorityGrantOperation.
 */
export interface AuthorityGrantResponse__Output {
  'success': (boolean);
  'error': (string);
  'message': (string);
  'grant': (_aether_v1_ACLAuthorityGrantInfo__Output | null);
  'requestId': (string);
  /**
   * For LIST_MY_GRANTS / LIST_GRANTS_ON_ME / BATCH_EXCHANGE results.
   */
  'grants': (_aether_v1_ACLAuthorityGrantInfo__Output)[];
  /**
   * Total matching rows ignoring pagination (LIST_*) or count of returned
   * grants (BATCH_EXCHANGE).
   */
  'total': (number);
  /**
   * Server's hint to clients on how often to revalidate cached grants
   * (seconds). Zero means "no hint" — clients fall back to their own policy.
   */
  'cacheHintTtlSeconds': (number);
}
