// Original file: aether.proto


/**
 * BuildInfo captures language/runtime build metadata for both the client
 * SDK (sent in InitConnection.client_build_info) and the server
 * (returned in ConnectionAck.server_build_info). Each field is
 * best-effort; SDKs populate what they can determine from their own
 * language ecosystem and leave the rest empty.
 */
export interface BuildInfo {
  /**
   * short or full git SHA, optional
   */
  'commit'?: (string);
  /**
   * RFC3339 timestamp, optional
   */
  'builtAt'?: (string);
  /**
   * e.g. "go1.25.5", "python3.12", "node20"
   */
  'runtime'?: (string);
  /**
   * e.g. "linux/amd64"
   */
  'os'?: (string);
}

/**
 * BuildInfo captures language/runtime build metadata for both the client
 * SDK (sent in InitConnection.client_build_info) and the server
 * (returned in ConnectionAck.server_build_info). Each field is
 * best-effort; SDKs populate what they can determine from their own
 * language ecosystem and leave the rest empty.
 */
export interface BuildInfo__Output {
  /**
   * short or full git SHA, optional
   */
  'commit': (string);
  /**
   * RFC3339 timestamp, optional
   */
  'builtAt': (string);
  /**
   * e.g. "go1.25.5", "python3.12", "node20"
   */
  'runtime': (string);
  /**
   * e.g. "linux/amd64"
   */
  'os': (string);
}
