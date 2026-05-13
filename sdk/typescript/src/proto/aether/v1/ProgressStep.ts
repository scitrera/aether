// Original file: aether.proto


/**
 * ProgressStep describes a discrete step within a multi-step operation.
 */
export interface ProgressStep {
  /**
   * Step name / title (e.g., "Extracting text", "Analyzing document")
   */
  'name'?: (string);
  /**
   * Detailed description of what this step is doing
   */
  'detail'?: (string);
  /**
   * Step sequence number (1-based, for ordering)
   */
  'sequence'?: (number);
  /**
   * Total number of steps if known (0 = unknown)
   */
  'totalSteps'?: (number);
  /**
   * Step type for UI rendering hints (e.g., "llm_call", "tool_use", "processing")
   */
  'stepType'?: (string);
}

/**
 * ProgressStep describes a discrete step within a multi-step operation.
 */
export interface ProgressStep__Output {
  /**
   * Step name / title (e.g., "Extracting text", "Analyzing document")
   */
  'name': (string);
  /**
   * Detailed description of what this step is doing
   */
  'detail': (string);
  /**
   * Step sequence number (1-based, for ordering)
   */
  'sequence': (number);
  /**
   * Total number of steps if known (0 = unknown)
   */
  'totalSteps': (number);
  /**
   * Step type for UI rendering hints (e.g., "llm_call", "tool_use", "processing")
   */
  'stepType': (string);
}
