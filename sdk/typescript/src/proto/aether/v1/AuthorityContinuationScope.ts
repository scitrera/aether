// Original file: aether.proto

import type { ACLAuthorityGrantResourceScopeEntry as _aether_v1_ACLAuthorityGrantResourceScopeEntry, ACLAuthorityGrantResourceScopeEntry__Output as _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output } from '../../aether/v1/ACLAuthorityGrantResourceScopeEntry';

/**
 * Explicit scope ceiling for a derived message authority continuation. Empty
 * axes retain the AuthorityGrant meaning of unrestricted, so an attenuated
 * agent continuation requires every axis to be populated and validated.
 */
export interface AuthorityContinuationScope {
  'workspaceScope'?: (string)[];
  'resourceScope'?: (_aether_v1_ACLAuthorityGrantResourceScopeEntry)[];
  'operationScope'?: (string)[];
  'maxAccessLevel'?: (number);
}

/**
 * Explicit scope ceiling for a derived message authority continuation. Empty
 * axes retain the AuthorityGrant meaning of unrestricted, so an attenuated
 * agent continuation requires every axis to be populated and validated.
 */
export interface AuthorityContinuationScope__Output {
  'workspaceScope': (string)[];
  'resourceScope': (_aether_v1_ACLAuthorityGrantResourceScopeEntry__Output)[];
  'operationScope': (string)[];
  'maxAccessLevel': (number);
}
