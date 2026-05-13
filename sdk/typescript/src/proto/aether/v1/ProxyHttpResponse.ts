// Original file: aether.proto

import type { ProxyError as _aether_v1_ProxyError, ProxyError__Output as _aether_v1_ProxyError__Output } from '../../aether/v1/ProxyError';

/**
 * ProxyHttpResponse is sent in reply to a ProxyHttpRequest. Errors are
 * signalled via the `error` field; non-error responses carry the backend's
 * status code, headers, and body (chunked when too large to inline).
 */
export interface ProxyHttpResponse {
  'requestId'?: (string);
  'statusCode'?: (number);
  'headers'?: ({[key: string]: string});
  'body'?: (Buffer | Uint8Array | string);
  'bodyChunked'?: (boolean);
  'error'?: (_aether_v1_ProxyError | null);
}

/**
 * ProxyHttpResponse is sent in reply to a ProxyHttpRequest. Errors are
 * signalled via the `error` field; non-error responses carry the backend's
 * status code, headers, and body (chunked when too large to inline).
 */
export interface ProxyHttpResponse__Output {
  'requestId': (string);
  'statusCode': (number);
  'headers': ({[key: string]: string});
  'body': (Buffer);
  'bodyChunked': (boolean);
  'error': (_aether_v1_ProxyError__Output | null);
}
