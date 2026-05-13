// Original file: aether.proto


/**
 * AgentLaunchParams specifies parameters for launching an agent via orchestration.
 * Matches the LaunchAgentRequest struct in state_provider.go.
 */
export interface AgentLaunchParams {
  /**
   * Instance specifier for the new agent
   */
  'specifier'?: (string);
  /**
   * Workspace where the agent should operate
   */
  'workspace'?: (string);
  /**
   * Override default launch parameters
   */
  'paramOverrides'?: ({[key: string]: string});
}

/**
 * AgentLaunchParams specifies parameters for launching an agent via orchestration.
 * Matches the LaunchAgentRequest struct in state_provider.go.
 */
export interface AgentLaunchParams__Output {
  /**
   * Instance specifier for the new agent
   */
  'specifier': (string);
  /**
   * Workspace where the agent should operate
   */
  'workspace': (string);
  /**
   * Override default launch parameters
   */
  'paramOverrides': ({[key: string]: string});
}
