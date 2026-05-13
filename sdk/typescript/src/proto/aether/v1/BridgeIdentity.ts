// Original file: aether.proto


export interface BridgeIdentity {
  /**
   * e.g., "aether-msgbridge", "webhook-bridge"
   */
  'implementation'?: (string);
  /**
   * Instance identifier for uniqueness (e.g., "default", "discord-1")
   */
  'specifier'?: (string);
}

export interface BridgeIdentity__Output {
  /**
   * e.g., "aether-msgbridge", "webhook-bridge"
   */
  'implementation': (string);
  /**
   * Instance identifier for uniqueness (e.g., "default", "discord-1")
   */
  'specifier': (string);
}
