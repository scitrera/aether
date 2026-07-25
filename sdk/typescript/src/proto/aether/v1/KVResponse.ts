// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

export interface KVResponse {
  'success'?: (boolean);
  /**
   * for GET (bytes matches KVOperation.value type)
   */
  'value'?: (Buffer | Uint8Array | string);
  /**
   * for LIST
   */
  'keys'?: (string)[];
  /**
   * for LIST with values. Type MUST be `map<string, bytes>` (not
   * `map<string, string>`) so binary payloads (e.g., msgpack-encoded
   * values) survive transit. Go-side `string` accepts arbitrary bytes
   * transparently, but Python proto deserializes a `string` field as
   * UTF-8 `str` — destroying or silently mangling non-UTF-8 binary data.
   * The single-value `bytes value` field above already gets this right.
   */
  'kvMap'?: ({[key: string]: Buffer | Uint8Array | string});
  /**
   * Echoed from the originating KVOperation for correlation
   */
  'requestId'?: (string);
  /**
   * Result of INCREMENT/DECREMENT (and INCREMENT_IF/DECREMENT_IF)
   */
  'counterValue'?: (number | string | Long);
  /**
   * True iff a conditional mutation was applied. For INCREMENT_IF/DECREMENT_IF
   * this is the guard result; for SET_NX/COMPARE_AND_SET/COMPARE_AND_DELETE it
   * reports whether the write/delete took effect. For unguarded ops always
   * true on success. On a failed COMPARE_AND_SET/COMPARE_AND_DELETE, `value`
   * carries the live stored value so callers can observe the current holder.
   */
  'applied'?: (boolean);
  /**
   * LIST pagination. next_cursor is an opaque token to pass as
   * KVOperation.cursor for the next page; empty when iteration is complete.
   * has_more is true when more matching keys remain beyond this page.
   */
  'nextCursor'?: (string);
  'hasMore'?: (boolean);
}

export interface KVResponse__Output {
  'success': (boolean);
  /**
   * for GET (bytes matches KVOperation.value type)
   */
  'value': (Buffer);
  /**
   * for LIST
   */
  'keys': (string)[];
  /**
   * for LIST with values. Type MUST be `map<string, bytes>` (not
   * `map<string, string>`) so binary payloads (e.g., msgpack-encoded
   * values) survive transit. Go-side `string` accepts arbitrary bytes
   * transparently, but Python proto deserializes a `string` field as
   * UTF-8 `str` — destroying or silently mangling non-UTF-8 binary data.
   * The single-value `bytes value` field above already gets this right.
   */
  'kvMap': ({[key: string]: Buffer});
  /**
   * Echoed from the originating KVOperation for correlation
   */
  'requestId': (string);
  /**
   * Result of INCREMENT/DECREMENT (and INCREMENT_IF/DECREMENT_IF)
   */
  'counterValue': (string);
  /**
   * True iff a conditional mutation was applied. For INCREMENT_IF/DECREMENT_IF
   * this is the guard result; for SET_NX/COMPARE_AND_SET/COMPARE_AND_DELETE it
   * reports whether the write/delete took effect. For unguarded ops always
   * true on success. On a failed COMPARE_AND_SET/COMPARE_AND_DELETE, `value`
   * carries the live stored value so callers can observe the current holder.
   */
  'applied': (boolean);
  /**
   * LIST pagination. next_cursor is an opaque token to pass as
   * KVOperation.cursor for the next page; empty when iteration is complete.
   * has_more is true when more matching keys remain beyond this page.
   */
  'nextCursor': (string);
  'hasMore': (boolean);
}
