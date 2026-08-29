// Original file: aether.proto

import type { AccessDecisionReceipt as _aether_v1_AccessDecisionReceipt, AccessDecisionReceipt__Output as _aether_v1_AccessDecisionReceipt__Output } from '../../aether/v1/AccessDecisionReceipt';

export interface AccessCheckResponse {
  'requestId'?: (string);
  /**
   * evaluation completed; denial is success
   */
  'success'?: (boolean);
  'error'?: (string);
  'decision'?: (_aether_v1_AccessDecisionReceipt | null);
}

export interface AccessCheckResponse__Output {
  'requestId': (string);
  /**
   * evaluation completed; denial is success
   */
  'success': (boolean);
  'error': (string);
  'decision': (_aether_v1_AccessDecisionReceipt__Output | null);
}
