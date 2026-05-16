// Original file: aether.proto

import type { AuthorityRequestStatus as _aether_v1_AuthorityRequestStatus, AuthorityRequestStatus__Output as _aether_v1_AuthorityRequestStatus__Output } from '../../aether/v1/AuthorityRequestStatus';

/**
 * AuthorityRequestListFilter narrows LIST_PENDING results.
 */
export interface AuthorityRequestListFilter {
  /**
   * 0 = no filter
   */
  'status'?: (_aether_v1_AuthorityRequestStatus);
  'workspace'?: (string);
  'limit'?: (number);
  'offset'?: (number);
  /**
   * Caller-declared capability gates: requests whose routing_capability matches
   * any entry are eligible. Stage C does not auto-discover the caller's full
   * held-capability set; the caller must enumerate which gates it is
   * representing when asking "what requests can I resolve?".
   */
  'matchingCapabilities'?: (string)[];
}

/**
 * AuthorityRequestListFilter narrows LIST_PENDING results.
 */
export interface AuthorityRequestListFilter__Output {
  /**
   * 0 = no filter
   */
  'status': (_aether_v1_AuthorityRequestStatus__Output);
  'workspace': (string);
  'limit': (number);
  'offset': (number);
  /**
   * Caller-declared capability gates: requests whose routing_capability matches
   * any entry are eligible. Stage C does not auto-discover the caller's full
   * held-capability set; the caller must enumerate which gates it is
   * representing when asking "what requests can I resolve?".
   */
  'matchingCapabilities': (string)[];
}
