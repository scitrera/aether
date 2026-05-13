// Original file: aether.proto


// Original file: aether.proto

export const _aether_v1_ProxyError_Kind = {
  UNKNOWN: 'UNKNOWN',
  DIAL_FAILED: 'DIAL_FAILED',
  TIMEOUT: 'TIMEOUT',
  UPSTREAM_RESET: 'UPSTREAM_RESET',
  ACL_DENIED: 'ACL_DENIED',
  SIDECAR_UNAVAILABLE: 'SIDECAR_UNAVAILABLE',
  PAYLOAD_TOO_LARGE: 'PAYLOAD_TOO_LARGE',
  DECODE_FAILED: 'DECODE_FAILED',
} as const;

export type _aether_v1_ProxyError_Kind =
  | 'UNKNOWN'
  | 0
  | 'DIAL_FAILED'
  | 1
  | 'TIMEOUT'
  | 2
  | 'UPSTREAM_RESET'
  | 3
  | 'ACL_DENIED'
  | 4
  | 'SIDECAR_UNAVAILABLE'
  | 5
  | 'PAYLOAD_TOO_LARGE'
  | 6
  | 'DECODE_FAILED'
  | 7

export type _aether_v1_ProxyError_Kind__Output = typeof _aether_v1_ProxyError_Kind[keyof typeof _aether_v1_ProxyError_Kind]

/**
 * ProxyError describes a transport-layer failure that prevented the proxy
 * from delivering the request to the backend.
 */
export interface ProxyError {
  'kind'?: (_aether_v1_ProxyError_Kind);
  'message'?: (string);
}

/**
 * ProxyError describes a transport-layer failure that prevented the proxy
 * from delivering the request to the backend.
 */
export interface ProxyError__Output {
  'kind': (_aether_v1_ProxyError_Kind__Output);
  'message': (string);
}
