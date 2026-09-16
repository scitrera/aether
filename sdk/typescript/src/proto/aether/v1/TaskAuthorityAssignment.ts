// Original file: aether.proto


export interface TaskAuthorityAssignment {
  /**
   * Required, 1..86400 seconds. Authority cannot be renewed past this limit.
   */
  'expiresInSeconds'?: (number);
  /**
   * Required: read (10) or read/write (20). No administration rights.
   */
  'maxAccessLevel'?: (number);
}

export interface TaskAuthorityAssignment__Output {
  /**
   * Required, 1..86400 seconds. Authority cannot be renewed past this limit.
   */
  'expiresInSeconds': (number);
  /**
   * Required: read (10) or read/write (20). No administration rights.
   */
  'maxAccessLevel': (number);
}
