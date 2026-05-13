// Original file: aether.proto


/**
 * AgentLaunchResult represents the result of launching an agent via orchestration.
 * Matches the LaunchAgentResponse struct in state_provider.go.
 */
export interface AgentLaunchResult {
  /**
   * Task ID for tracking the orchestration request
   */
  'taskId'?: (string);
  /**
   * Human-readable status message
   */
  'message'?: (string);
}

/**
 * AgentLaunchResult represents the result of launching an agent via orchestration.
 * Matches the LaunchAgentResponse struct in state_provider.go.
 */
export interface AgentLaunchResult__Output {
  /**
   * Task ID for tracking the orchestration request
   */
  'taskId': (string);
  /**
   * Human-readable status message
   */
  'message': (string);
}
