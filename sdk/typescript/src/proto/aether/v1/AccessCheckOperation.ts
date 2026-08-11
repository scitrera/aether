// Original file: aether.proto

import type { ResourceAccessRequest as _aether_v1_ResourceAccessRequest, ResourceAccessRequest__Output as _aether_v1_ResourceAccessRequest__Output } from '../../aether/v1/ResourceAccessRequest';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';

export interface AccessCheckOperation {
  'requestId'?: (string);
  'access'?: (_aether_v1_ResourceAccessRequest | null);
  'authorization'?: (_aether_v1_AuthorizationContext | null);
}

export interface AccessCheckOperation__Output {
  'requestId': (string);
  'access': (_aether_v1_ResourceAccessRequest__Output | null);
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
}
