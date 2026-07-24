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
  /**
   * When true, the gateway does NOT add this service connection to the
   * pool-task worker index, so it is never targeted for POOL-mode task
   * assignments for its implementation. Default false = pool consumer
   * (back-compat: existing services keep receiving pool tasks). Serve-only
   * instances that register no task handlers (e.g. a MemoryLayer server with
   * its in-process worker disabled) set this so pool tasks are routed only to
   * real workers instead of being claimed-and-dropped (and stuck until
   * reconcile). Agents are always pool consumers regardless of this field.
   */
  'noPoolConsumer'?: (boolean);
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
  /**
   * When true, the gateway does NOT add this service connection to the
   * pool-task worker index, so it is never targeted for POOL-mode task
   * assignments for its implementation. Default false = pool consumer
   * (back-compat: existing services keep receiving pool tasks). Serve-only
   * instances that register no task handlers (e.g. a MemoryLayer server with
   * its in-process worker disabled) set this so pool tasks are routed only to
   * real workers instead of being claimed-and-dropped (and stuck until
   * reconcile). Agents are always pool consumers regardless of this field.
   */
  'noPoolConsumer': (boolean);
}
