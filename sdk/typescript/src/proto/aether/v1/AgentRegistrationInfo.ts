// Original file: aether.proto

import type { AgentResourceSchemaEntry as _aether_v1_AgentResourceSchemaEntry, AgentResourceSchemaEntry__Output as _aether_v1_AgentResourceSchemaEntry__Output } from '../../aether/v1/AgentResourceSchemaEntry';
import type { Long } from '@grpc/proto-loader';

/**
 * AgentRegistrationInfo represents an agent registration.
 * Matches the AgentRegistrationInfo struct in state_provider.go.
 * Agent registrations define how to launch agent implementations via orchestrators.
 */
export interface AgentRegistrationInfo {
  /**
   * Unique implementation name (e.g., "data-processor", "code-reviewer")
   */
  'implementation'?: (string);
  /**
   * Profile name for orchestration (e.g., "kubernetes", "docker")
   */
  'orchestratorProfile'?: (string);
  /**
   * Human-readable description of the agent type
   */
  'description'?: (string);
  /**
   * Default launch parameters for the agent
   */
  'launchParams'?: ({[key: string]: string});
  /**
   * Unix timestamp when registration was created
   */
  'registeredAt'?: (number | string | Long);
  /**
   * Unix timestamp when registration was last updated
   */
  'updatedAt'?: (number | string | Long);
  /**
   * Phase 5: declared resource schema. Each entry describes a family of
   * resources the agent owns (by resource_type_prefix), the permission
   * verbs it understands on those resources, and an optional JSON Schema
   * describing the shape of resource ids in that family. Aether enforces
   * that no two registrations declare overlapping resource_type_prefix
   * values across active registrations.
   */
  'resourceSchema'?: (_aether_v1_AgentResourceSchemaEntry)[];
  /**
   * Phase 5: capability flags. Generic key→bool map for future A2A interop
   * (e.g. "streaming", "hibernation_aware", "extensions_supported").
   * Keys are agent-defined; consumers should treat unknown keys as false.
   */
  'capabilities'?: ({[key: string]: boolean});
  /**
   * Phase 5: URI list of supported extensions. Each extension is identified
   * by URI per the A2A extension model. Format: free string; recommended
   * "https://..." style URIs.
   */
  'extensions'?: (string)[];
}

/**
 * AgentRegistrationInfo represents an agent registration.
 * Matches the AgentRegistrationInfo struct in state_provider.go.
 * Agent registrations define how to launch agent implementations via orchestrators.
 */
export interface AgentRegistrationInfo__Output {
  /**
   * Unique implementation name (e.g., "data-processor", "code-reviewer")
   */
  'implementation': (string);
  /**
   * Profile name for orchestration (e.g., "kubernetes", "docker")
   */
  'orchestratorProfile': (string);
  /**
   * Human-readable description of the agent type
   */
  'description': (string);
  /**
   * Default launch parameters for the agent
   */
  'launchParams': ({[key: string]: string});
  /**
   * Unix timestamp when registration was created
   */
  'registeredAt': (string);
  /**
   * Unix timestamp when registration was last updated
   */
  'updatedAt': (string);
  /**
   * Phase 5: declared resource schema. Each entry describes a family of
   * resources the agent owns (by resource_type_prefix), the permission
   * verbs it understands on those resources, and an optional JSON Schema
   * describing the shape of resource ids in that family. Aether enforces
   * that no two registrations declare overlapping resource_type_prefix
   * values across active registrations.
   */
  'resourceSchema': (_aether_v1_AgentResourceSchemaEntry__Output)[];
  /**
   * Phase 5: capability flags. Generic key→bool map for future A2A interop
   * (e.g. "streaming", "hibernation_aware", "extensions_supported").
   * Keys are agent-defined; consumers should treat unknown keys as false.
   */
  'capabilities': ({[key: string]: boolean});
  /**
   * Phase 5: URI list of supported extensions. Each extension is identified
   * by URI per the A2A extension model. Format: free string; recommended
   * "https://..." style URIs.
   */
  'extensions': (string)[];
}
