// Original file: aether.proto


/**
 * ExtensionDeclaration identifies an extension a participant supports or
 * is announcing as active on a given message. Modeled on the A2A extension
 * model: URI-typed identity, optional version, and a `required` flag.
 * 
 * At InitConnection time the client lists the extensions it wants active
 * on its session. The gateway returns ConnectionAck.negotiated_extensions
 * with one NegotiatedExtension per declaration. Per-message activation
 * rides on UpstreamMessage.active_extensions / DownstreamMessage.active_extensions
 * (a `repeated string` URI list — the full ExtensionDeclaration is not
 * re-sent per message).
 */
export interface ExtensionDeclaration {
  /**
   * Globally unique extension URI. Recommended form: "https://..." style.
   * Required.
   */
  'uri'?: (string);
  /**
   * Optional version string. Empty = any version.
   */
  'version'?: (string);
  /**
   * When set on a client-side declaration during InitConnection, the
   * gateway MUST reject the connection if it does not support this
   * extension. When set on a per-message declaration, peers MUST reject
   * the message as `ERR_EXTENSION_UNSUPPORTED` if they cannot handle the
   * listed extension.
   */
  'required'?: (boolean);
  /**
   * Optional JSON Schema (as a string) describing the shape of
   * extension-specific data. Informational only; Aether does not enforce.
   */
  'jsonSchema'?: (string);
}

/**
 * ExtensionDeclaration identifies an extension a participant supports or
 * is announcing as active on a given message. Modeled on the A2A extension
 * model: URI-typed identity, optional version, and a `required` flag.
 * 
 * At InitConnection time the client lists the extensions it wants active
 * on its session. The gateway returns ConnectionAck.negotiated_extensions
 * with one NegotiatedExtension per declaration. Per-message activation
 * rides on UpstreamMessage.active_extensions / DownstreamMessage.active_extensions
 * (a `repeated string` URI list — the full ExtensionDeclaration is not
 * re-sent per message).
 */
export interface ExtensionDeclaration__Output {
  /**
   * Globally unique extension URI. Recommended form: "https://..." style.
   * Required.
   */
  'uri': (string);
  /**
   * Optional version string. Empty = any version.
   */
  'version': (string);
  /**
   * When set on a client-side declaration during InitConnection, the
   * gateway MUST reject the connection if it does not support this
   * extension. When set on a per-message declaration, peers MUST reject
   * the message as `ERR_EXTENSION_UNSUPPORTED` if they cannot handle the
   * listed extension.
   */
  'required': (boolean);
  /**
   * Optional JSON Schema (as a string) describing the shape of
   * extension-specific data. Informational only; Aether does not enforce.
   */
  'jsonSchema': (string);
}
