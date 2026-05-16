// Original file: aether.proto


/**
 * AuthorityRequestResourceScopeEntry mirrors ACLAuthorityGrantResourceScopeEntry
 * shape (the existing entry type used in grant exchange).
 */
export interface AuthorityRequestResourceScopeEntry {
  'resourceType'?: (string);
  'patterns'?: (string)[];
}

/**
 * AuthorityRequestResourceScopeEntry mirrors ACLAuthorityGrantResourceScopeEntry
 * shape (the existing entry type used in grant exchange).
 */
export interface AuthorityRequestResourceScopeEntry__Output {
  'resourceType': (string);
  'patterns': (string)[];
}
