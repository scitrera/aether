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
   * True iff INCREMENT_IF/DECREMENT_IF mutation was applied; for unguarded ops always true on success
   */
  'applied'?: (boolean);
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
   * True iff INCREMENT_IF/DECREMENT_IF mutation was applied; for unguarded ops always true on success
   */
  'applied': (boolean);
}
