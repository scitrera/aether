// Original file: aether.proto

import type { AccessDecisionReceipt as _aether_v1_AccessDecisionReceipt, AccessDecisionReceipt__Output as _aether_v1_AccessDecisionReceipt__Output } from '../../aether/v1/AccessDecisionReceipt';

export interface BatchAccessCheckResponse {
  'requestId'?: (string);
  'success'?: (boolean);
  'error'?: (string);
  /**
   * Same order and cardinality as BatchAccessCheckOperation.access.
   */
  'decisions'?: (_aether_v1_AccessDecisionReceipt)[];
}

export interface BatchAccessCheckResponse__Output {
  'requestId': (string);
  'success': (boolean);
  'error': (string);
  /**
   * Same order and cardinality as BatchAccessCheckOperation.access.
   */
  'decisions': (_aether_v1_AccessDecisionReceipt__Output)[];
}
