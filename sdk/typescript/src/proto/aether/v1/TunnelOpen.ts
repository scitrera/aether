// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { Long } from '@grpc/proto-loader';

// Original file: aether.proto

export const _aether_v1_TunnelOpen_Protocol = {
  TCP: 'TCP',
  UDP: 'UDP',
  WEBSOCKET: 'WEBSOCKET',
} as const;

export type _aether_v1_TunnelOpen_Protocol =
  | 'TCP'
  | 0
  | 'UDP'
  | 1
  | 'WEBSOCKET'
  | 2

export type _aether_v1_TunnelOpen_Protocol__Output = typeof _aether_v1_TunnelOpen_Protocol[keyof typeof _aether_v1_TunnelOpen_Protocol]

export interface TunnelOpen {
  'tunnelId'?: (string);
  'targetTopic'?: (string);
  'protocol'?: (_aether_v1_TunnelOpen_Protocol);
  'remoteHint'?: (string);
  'metadata'?: ({[key: string]: string});
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  'idleTimeoutMs'?: (number | string | Long);
  'maxBytes'?: (number | string | Long);
  /**
   * RESERVED for v2 reconnect/resume; ignored in v1
   */
  'sessionToken'?: (string);
  /**
   * Optional: explicit backend name on the target sidecar. When set, the
   * sidecar looks up the tunnel backend by BackendConfig.Name directly.
   * The named backend's allow_remote_hints still applies. When unset,
   * sidecar falls back to first-match by remote_hint glob.
   */
  'backendName'?: (string);
  /**
   * Hop count along a proxy chain. Each gateway forwarding the open
   * increments by 1. Rejected when exceeds proxy.max_chain_depth (default 8).
   * Same hybrid-floor clamping behavior as ProxyHttpRequest.proxy_chain_depth.
   */
  'proxyChainDepth'?: (number);
}

export interface TunnelOpen__Output {
  'tunnelId': (string);
  'targetTopic': (string);
  'protocol': (_aether_v1_TunnelOpen_Protocol__Output);
  'remoteHint': (string);
  'metadata': ({[key: string]: string});
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  'idleTimeoutMs': (string);
  'maxBytes': (string);
  /**
   * RESERVED for v2 reconnect/resume; ignored in v1
   */
  'sessionToken': (string);
  /**
   * Optional: explicit backend name on the target sidecar. When set, the
   * sidecar looks up the tunnel backend by BackendConfig.Name directly.
   * The named backend's allow_remote_hints still applies. When unset,
   * sidecar falls back to first-match by remote_hint glob.
   */
  'backendName': (string);
  /**
   * Hop count along a proxy chain. Each gateway forwarding the open
   * increments by 1. Rejected when exceeds proxy.max_chain_depth (default 8).
   * Same hybrid-floor clamping behavior as ProxyHttpRequest.proxy_chain_depth.
   */
  'proxyChainDepth': (number);
}
