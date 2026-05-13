// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ConnectionStatusResponse reports whether the queried principal currently
 * has an active session lock. `last_seen_at` is populated only when cheaply
 * available; otherwise zero (callers must not interpret 0 as "never seen").
 */
export interface ConnectionStatusResponse {
  'requestId'?: (string);
  'ok'?: (boolean);
  'error'?: (string);
  'connected'?: (boolean);
  'lastSeenAt'?: (number | string | Long);
}

/**
 * ConnectionStatusResponse reports whether the queried principal currently
 * has an active session lock. `last_seen_at` is populated only when cheaply
 * available; otherwise zero (callers must not interpret 0 as "never seen").
 */
export interface ConnectionStatusResponse__Output {
  'requestId': (string);
  'ok': (boolean);
  'error': (string);
  'connected': (boolean);
  'lastSeenAt': (string);
}
