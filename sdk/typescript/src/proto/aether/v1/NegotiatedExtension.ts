// Original file: aether.proto


/**
 * NegotiatedExtension is the server's response side of the extension
 * handshake. Carries the URI that was negotiated and a `supported` flag —
 * the intersection of supported extensions across client + server (with
 * agent-declared URIs widening the set for sessions targeting that agent).
 */
export interface NegotiatedExtension {
  /**
   * Echo of the client-declared URI.
   */
  'uri'?: (string);
  /**
   * Echo of the client-declared version (empty when the client did not
   * pin one). The server does not currently advertise its own version.
   */
  'version'?: (string);
  /**
   * True when the server (or a connected agent whose registration carries
   * this URI) supports the extension. False when the URI is unknown.
   */
  'supported'?: (boolean);
  /**
   * Human-readable reason populated when supported=false. Empty when
   * supported=true. Used by clients to surface "extension X is required
   * but unsupported" diagnostics.
   */
  'rejectionReason'?: (string);
}

/**
 * NegotiatedExtension is the server's response side of the extension
 * handshake. Carries the URI that was negotiated and a `supported` flag —
 * the intersection of supported extensions across client + server (with
 * agent-declared URIs widening the set for sessions targeting that agent).
 */
export interface NegotiatedExtension__Output {
  /**
   * Echo of the client-declared URI.
   */
  'uri': (string);
  /**
   * Echo of the client-declared version (empty when the client did not
   * pin one). The server does not currently advertise its own version.
   */
  'version': (string);
  /**
   * True when the server (or a connected agent whose registration carries
   * this URI) supports the extension. False when the URI is unknown.
   */
  'supported': (boolean);
  /**
   * Human-readable reason populated when supported=false. Empty when
   * supported=true. Used by clients to surface "extension X is required
   * but unsupported" diagnostics.
   */
  'rejectionReason': (string);
}
