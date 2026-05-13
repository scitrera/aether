// Original file: aether.proto

import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';

/**
 * ConnectionStatusRequest asks the gateway whether a principal has a live
 * connection. Self-checks (principal == caller's identity) are trivially
 * allowed; cross-principal checks require `capability/query_connections`.
 */
export interface ConnectionStatusRequest {
  'requestId'?: (string);
  'principal'?: (_aether_v1_PrincipalRef | null);
}

/**
 * ConnectionStatusRequest asks the gateway whether a principal has a live
 * connection. Self-checks (principal == caller's identity) are trivially
 * allowed; cross-principal checks require `capability/query_connections`.
 */
export interface ConnectionStatusRequest__Output {
  'requestId': (string);
  'principal': (_aether_v1_PrincipalRef__Output | null);
}
