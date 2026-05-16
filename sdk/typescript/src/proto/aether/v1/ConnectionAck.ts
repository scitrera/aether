// Original file: aether.proto

import type { NegotiatedExtension as _aether_v1_NegotiatedExtension, NegotiatedExtension__Output as _aether_v1_NegotiatedExtension__Output } from '../../aether/v1/NegotiatedExtension';

/**
 * ConnectionAck is sent immediately after successful connection.
 * Contains the session ID that the client should store for reconnection.
 */
export interface ConnectionAck {
  'sessionId'?: (string);
  /**
   * Indicates if this was a resumed session (client provided matching resume_session_id)
   */
  'resumed'?: (boolean);
  /**
   * For non-unique tasks, the server-assigned task instance ID used to construct
   * the task's topic address (ta.{workspace}.{impl}.{assigned_id}).
   * Empty for all other principal types.
   */
  'assignedId'?: (string);
  /**
   * Phase 6: negotiation result for each extension the client declared
   * in InitConnection.extensions (or via the `Aether-Extensions` gRPC
   * metadata header). One entry per client declaration, in the order the
   * client supplied them; header-sourced declarations follow the
   * proto-sourced ones. Empty when the client declared no extensions.
   */
  'negotiatedExtensions'?: (_aether_v1_NegotiatedExtension)[];
  /**
   * Phase 6: extensions the server natively supports that the client did
   * NOT declare. Lets clients discover what optional extensions are
   * available without a separate descriptor endpoint. Empty when the
   * client already declared every server-supported URI.
   */
  'serverSupportedExtensions'?: (string)[];
}

/**
 * ConnectionAck is sent immediately after successful connection.
 * Contains the session ID that the client should store for reconnection.
 */
export interface ConnectionAck__Output {
  'sessionId': (string);
  /**
   * Indicates if this was a resumed session (client provided matching resume_session_id)
   */
  'resumed': (boolean);
  /**
   * For non-unique tasks, the server-assigned task instance ID used to construct
   * the task's topic address (ta.{workspace}.{impl}.{assigned_id}).
   * Empty for all other principal types.
   */
  'assignedId': (string);
  /**
   * Phase 6: negotiation result for each extension the client declared
   * in InitConnection.extensions (or via the `Aether-Extensions` gRPC
   * metadata header). One entry per client declaration, in the order the
   * client supplied them; header-sourced declarations follow the
   * proto-sourced ones. Empty when the client declared no extensions.
   */
  'negotiatedExtensions': (_aether_v1_NegotiatedExtension__Output)[];
  /**
   * Phase 6: extensions the server natively supports that the client did
   * NOT declare. Lets clients discover what optional extensions are
   * available without a separate descriptor endpoint. Empty when the
   * client already declared every server-supported URI.
   */
  'serverSupportedExtensions': (string)[];
}
