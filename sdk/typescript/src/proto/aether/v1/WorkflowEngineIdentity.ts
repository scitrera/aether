// Original file: aether.proto


/**
 * WorkflowEngineIdentity identifies a workflow engine principal.
 * Replaces the previous bool sentinel to allow future extensibility.
 */
export interface WorkflowEngineIdentity {
  /**
   * Optional instance identifier for multiple workflow engines
   */
  'instanceId'?: (string);
}

/**
 * WorkflowEngineIdentity identifies a workflow engine principal.
 * Replaces the previous bool sentinel to allow future extensibility.
 */
export interface WorkflowEngineIdentity__Output {
  /**
   * Optional instance identifier for multiple workflow engines
   */
  'instanceId': (string);
}
