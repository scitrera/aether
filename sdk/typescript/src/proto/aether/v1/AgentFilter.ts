// Original file: aether.proto


/**
 * AgentFilter specifies filtering parameters for listing agent registrations.
 */
export interface AgentFilter {
  /**
   * Filter by orchestrator profile
   */
  'orchestratorProfile'?: (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit'?: (number);
  /**
   * Offset for pagination
   */
  'offset'?: (number);
}

/**
 * AgentFilter specifies filtering parameters for listing agent registrations.
 */
export interface AgentFilter__Output {
  /**
   * Filter by orchestrator profile
   */
  'orchestratorProfile': (string);
  /**
   * Maximum number of results (0 = default limit)
   */
  'limit': (number);
  /**
   * Offset for pagination
   */
  'offset': (number);
}
