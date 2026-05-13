// Original file: aether.proto


export interface ServiceIdentity {
  /**
   * e.g., "frontend-api", "platform-backend"
   */
  'implementation'?: (string);
  /**
   * Instance identifier for uniqueness (e.g., "pod-1", "default")
   */
  'specifier'?: (string);
}

export interface ServiceIdentity__Output {
  /**
   * e.g., "frontend-api", "platform-backend"
   */
  'implementation': (string);
  /**
   * Instance identifier for uniqueness (e.g., "pod-1", "default")
   */
  'specifier': (string);
}
