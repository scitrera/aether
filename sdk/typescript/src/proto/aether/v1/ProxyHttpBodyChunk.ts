// Original file: aether.proto


/**
 * ProxyHttpBodyChunk carries one fragment of a chunked request or response
 * body. Sequence numbers start at 0; fin=true marks the final chunk.
 */
export interface ProxyHttpBodyChunk {
  'requestId'?: (string);
  'isRequest'?: (boolean);
  'seq'?: (number);
  'data'?: (Buffer | Uint8Array | string);
  'fin'?: (boolean);
}

/**
 * ProxyHttpBodyChunk carries one fragment of a chunked request or response
 * body. Sequence numbers start at 0; fin=true marks the final chunk.
 */
export interface ProxyHttpBodyChunk__Output {
  'requestId': (string);
  'isRequest': (boolean);
  'seq': (number);
  'data': (Buffer);
  'fin': (boolean);
}
