// Original file: aether.proto


export interface OrchestratorIdentity {
  /**
   * e.g., "kubernetes-orchestrator", "docker-orchestrator"
   */
  'implementation'?: (string);
  /**
   * Client-generated UUID for uniqueness (multiple instances)
   */
  'specifier'?: (string);
  /**
   * Profiles this orchestrator supports
   */
  'supportedProfiles'?: (string)[];
}

export interface OrchestratorIdentity__Output {
  /**
   * e.g., "kubernetes-orchestrator", "docker-orchestrator"
   */
  'implementation': (string);
  /**
   * Client-generated UUID for uniqueness (multiple instances)
   */
  'specifier': (string);
  /**
   * Profiles this orchestrator supports
   */
  'supportedProfiles': (string)[];
}
