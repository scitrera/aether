// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { Long } from '@grpc/proto-loader';

/**
 * ProxyHttpRequest is sent upstream by an initiator to ask the gateway to
 * forward an HTTP request to a target service principal, and is also routed
 * downstream to the target service (terminator sidecar) for execution. The
 * service handles the request and returns a ProxyHttpResponse.
 */
export interface ProxyHttpRequest {
  'requestId'?: (string);
  'targetTopic'?: (string);
  'method'?: (string);
  'path'?: (string);
  'headers'?: ({[key: string]: string});
  'body'?: (Buffer | Uint8Array | string);
  'bodyChunked'?: (boolean);
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  'appWorkspace'?: (string);
  'timeoutMs'?: (number | string | Long);
  'followRedirects'?: (boolean);
  /**
   * Optional: explicit backend name on the target sidecar. When set, the
   * sidecar looks up the backend by BackendConfig.Name directly. The named
   * backend's allow_methods/allow_paths still apply (defence in depth);
   * backend_name only chooses which backend's ACL is consulted, it does
   * not bypass it. When unset, sidecar falls back to first-match by ACL.
   */
  'backendName'?: (string);
  /**
   * Optional: opt into unbounded response streaming (SSE / log tails /
   * model token streams). When true, timeout_ms becomes time-to-first-byte
   * only; subsequent body bytes are governed by stream_idle_timeout_ms.
   * The response is always chunked (body_chunked=true). Default false
   * preserves bounded REST semantics.
   */
  'streamResponseIndefinitely'?: (boolean);
  /**
   * Optional (only meaningful when stream_response_indefinitely=true):
   * close the response stream with ProxyError{TIMEOUT} if no body bytes
   * flow for this duration. Default 30000 (30s) when unset.
   */
  'streamIdleTimeoutMs'?: (number | string | Long);
  /**
   * Optional cap on total response body bytes. Exceeding mid-stream closes
   * with ProxyError{PAYLOAD_TOO_LARGE}. Defaults to per-backend max body.
   */
  'maxResponseBodyBytes'?: (number | string | Long);
  /**
   * Hop count along a proxy chain. Each gateway forwarding the request
   * increments by 1. The gateway rejects requests where this value exceeds
   * proxy.max_chain_depth (default 8) with ProxyError{ACL_DENIED,
   * detail: "proxy_chain_depth_exceeded"}. Caller-set values are clamped
   * upward by the sidecar relay floor (sandboxes can bump higher but never
   * lower than the inbound chain depth they observed).
   */
  'proxyChainDepth'?: (number);
}

/**
 * ProxyHttpRequest is sent upstream by an initiator to ask the gateway to
 * forward an HTTP request to a target service principal, and is also routed
 * downstream to the target service (terminator sidecar) for execution. The
 * service handles the request and returns a ProxyHttpResponse.
 */
export interface ProxyHttpRequest__Output {
  'requestId': (string);
  'targetTopic': (string);
  'method': (string);
  'path': (string);
  'headers': ({[key: string]: string});
  'body': (Buffer);
  'bodyChunked': (boolean);
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  'appWorkspace': (string);
  'timeoutMs': (string);
  'followRedirects': (boolean);
  /**
   * Optional: explicit backend name on the target sidecar. When set, the
   * sidecar looks up the backend by BackendConfig.Name directly. The named
   * backend's allow_methods/allow_paths still apply (defence in depth);
   * backend_name only chooses which backend's ACL is consulted, it does
   * not bypass it. When unset, sidecar falls back to first-match by ACL.
   */
  'backendName': (string);
  /**
   * Optional: opt into unbounded response streaming (SSE / log tails /
   * model token streams). When true, timeout_ms becomes time-to-first-byte
   * only; subsequent body bytes are governed by stream_idle_timeout_ms.
   * The response is always chunked (body_chunked=true). Default false
   * preserves bounded REST semantics.
   */
  'streamResponseIndefinitely': (boolean);
  /**
   * Optional (only meaningful when stream_response_indefinitely=true):
   * close the response stream with ProxyError{TIMEOUT} if no body bytes
   * flow for this duration. Default 30000 (30s) when unset.
   */
  'streamIdleTimeoutMs': (string);
  /**
   * Optional cap on total response body bytes. Exceeding mid-stream closes
   * with ProxyError{PAYLOAD_TOO_LARGE}. Defaults to per-backend max body.
   */
  'maxResponseBodyBytes': (string);
  /**
   * Hop count along a proxy chain. Each gateway forwarding the request
   * increments by 1. The gateway rejects requests where this value exceeds
   * proxy.max_chain_depth (default 8) with ProxyError{ACL_DENIED,
   * detail: "proxy_chain_depth_exceeded"}. Caller-set values are clamped
   * upward by the sidecar relay floor (sandboxes can bump higher but never
   * lower than the inbound chain depth they observed).
   */
  'proxyChainDepth': (number);
}
