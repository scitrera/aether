// Original file: aether.proto

import type { ResourceAccessRequest as _aether_v1_ResourceAccessRequest, ResourceAccessRequest__Output as _aether_v1_ResourceAccessRequest__Output } from '../../aether/v1/ResourceAccessRequest';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { Long } from '@grpc/proto-loader';

/**
 * AccessDecisionReceipt is gateway-authored transport metadata. Receivers may
 * trust it only when it arrived in the Aether envelope, never when an
 * equivalent object appears inside an application payload.
 */
export interface AccessDecisionReceipt {
  'decisionId'?: (string);
  'request'?: (_aether_v1_ResourceAccessRequest | null);
  'allowed'?: (boolean);
  /**
   * "ALLOW" or "DENY"
   */
  'decision'?: (string);
  'effectiveAccessLevel'?: (number);
  /**
   * authenticated connected principal
   */
  'actor'?: (_aether_v1_PrincipalRef | null);
  /**
   * populated for on-behalf-of checks
   */
  'subject'?: (_aether_v1_PrincipalRef | null);
  /**
   * populated when the grant records one
   */
  'rootSubject'?: (_aether_v1_PrincipalRef | null);
  /**
   * "direct" or "on_behalf_of"
   */
  'authorityMode'?: (string);
  'grantId'?: (string);
  'rootGrantId'?: (string);
  'evaluatedAtMs'?: (number | string | Long);
  'expiresAtMs'?: (number | string | Long);
  /**
   * stable code; empty for allowed checks
   */
  'denialCode'?: (string);
  /**
   * Populated for checked SendMessage and ProxyHTTP delivery. This binds the
   * receipt to the concrete post-wildcard-resolution target that received it.
   */
  'deliveryTarget'?: (string);
}

/**
 * AccessDecisionReceipt is gateway-authored transport metadata. Receivers may
 * trust it only when it arrived in the Aether envelope, never when an
 * equivalent object appears inside an application payload.
 */
export interface AccessDecisionReceipt__Output {
  'decisionId': (string);
  'request': (_aether_v1_ResourceAccessRequest__Output | null);
  'allowed': (boolean);
  /**
   * "ALLOW" or "DENY"
   */
  'decision': (string);
  'effectiveAccessLevel': (number);
  /**
   * authenticated connected principal
   */
  'actor': (_aether_v1_PrincipalRef__Output | null);
  /**
   * populated for on-behalf-of checks
   */
  'subject': (_aether_v1_PrincipalRef__Output | null);
  /**
   * populated when the grant records one
   */
  'rootSubject': (_aether_v1_PrincipalRef__Output | null);
  /**
   * "direct" or "on_behalf_of"
   */
  'authorityMode': (string);
  'grantId': (string);
  'rootGrantId': (string);
  'evaluatedAtMs': (string);
  'expiresAtMs': (string);
  /**
   * stable code; empty for allowed checks
   */
  'denialCode': (string);
  /**
   * Populated for checked SendMessage and ProxyHTTP delivery. This binds the
   * receipt to the concrete post-wildcard-resolution target that received it.
   */
  'deliveryTarget': (string);
}
