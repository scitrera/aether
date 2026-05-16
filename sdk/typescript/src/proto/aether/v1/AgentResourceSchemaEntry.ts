// Original file: aether.proto


/**
 * AgentResourceSchemaEntry describes one resource family owned by an agent.
 */
export interface AgentResourceSchemaEntry {
  /**
   * The resource_type prefix the agent claims ownership of. Examples:
   * "chat/", "docmgmt/document", "workflow/run". Prefixes are checked
   * for collision across active registrations.
   */
  'resourceTypePrefix'?: (string);
  /**
   * Permission verbs the agent declares on this family. Examples: "read",
   * "write", "admin". Used by tooling/docs; Aether's CheckAccess still
   * routes through the existing operation/access-level model unchanged.
   */
  'permissionVerbs'?: (string)[];
  /**
   * Optional JSON Schema (as a string) describing the resource_id shape.
   * Aether does not currently enforce this; it's informational for
   * consumers and the future AgentCard generator (Phase 6).
   */
  'resourceIdSchema'?: (string);
}

/**
 * AgentResourceSchemaEntry describes one resource family owned by an agent.
 */
export interface AgentResourceSchemaEntry__Output {
  /**
   * The resource_type prefix the agent claims ownership of. Examples:
   * "chat/", "docmgmt/document", "workflow/run". Prefixes are checked
   * for collision across active registrations.
   */
  'resourceTypePrefix': (string);
  /**
   * Permission verbs the agent declares on this family. Examples: "read",
   * "write", "admin". Used by tooling/docs; Aether's CheckAccess still
   * routes through the existing operation/access-level model unchanged.
   */
  'permissionVerbs': (string)[];
  /**
   * Optional JSON Schema (as a string) describing the resource_id shape.
   * Aether does not currently enforce this; it's informational for
   * consumers and the future AgentCard generator (Phase 6).
   */
  'resourceIdSchema': (string);
}
