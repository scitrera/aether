// Original file: aether.proto


export interface TaskIdentity {
  'workspace'?: (string);
  'implementation'?: (string);
  /**
   * Empty for non-unique tasks
   */
  'uniqueSpecifier'?: (string);
}

export interface TaskIdentity__Output {
  'workspace': (string);
  'implementation': (string);
  /**
   * Empty for non-unique tasks
   */
  'uniqueSpecifier': (string);
}
