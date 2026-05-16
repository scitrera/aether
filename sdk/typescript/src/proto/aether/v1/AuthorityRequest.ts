// Original file: aether.proto

import type { AuthorityRequestStatus as _aether_v1_AuthorityRequestStatus, AuthorityRequestStatus__Output as _aether_v1_AuthorityRequestStatus__Output } from '../../aether/v1/AuthorityRequestStatus';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { AuthorityRequestResourceScopeEntry as _aether_v1_AuthorityRequestResourceScopeEntry, AuthorityRequestResourceScopeEntry__Output as _aether_v1_AuthorityRequestResourceScopeEntry__Output } from '../../aether/v1/AuthorityRequestResourceScopeEntry';
import type { AccessLevel as _aether_v1_AccessLevel, AccessLevel__Output as _aether_v1_AccessLevel__Output } from '../../aether/v1/AccessLevel';
import type { AuthorityRequestRoutingTarget as _aether_v1_AuthorityRequestRoutingTarget, AuthorityRequestRoutingTarget__Output as _aether_v1_AuthorityRequestRoutingTarget__Output } from '../../aether/v1/AuthorityRequestRoutingTarget';
import type { Long } from '@grpc/proto-loader';

/**
 * AuthorityRequest is a typed "sudo" -- a running task asks for elevated
 * authority and parks until an approver resolves it. Approval mints a
 * standard AuthorityGrant via the existing CreateAuthorityGrant path; no
 * parallel grant type exists.
 */
export interface AuthorityRequest {
  'requestId'?: (string);
  'status'?: (_aether_v1_AuthorityRequestStatus);
  /**
   * Authority being requested ("desired grant").
   */
  'requestingActor'?: (_aether_v1_PrincipalRef | null);
  /**
   * empty = same as requesting_actor (the common case)
   */
  'targetSubject'?: (_aether_v1_PrincipalRef | null);
  'desiredWorkspaceScope'?: (string)[];
  'desiredResourceScope'?: (_aether_v1_AuthorityRequestResourceScopeEntry)[];
  'desiredOperationScope'?: (string)[];
  'requestedAccessLevel'?: (_aether_v1_AccessLevel);
  'requestedDurationSeconds'?: (number | string | Long);
  'audienceType'?: (string);
  'audienceId'?: (string);
  /**
   * Routing: who can approve. Required.
   */
  'routingTarget'?: (_aether_v1_AuthorityRequestRoutingTarget | null);
  /**
   * Free-text narrative for the audit trail.
   */
  'reason'?: (string);
  /**
   * Optional: the task that is paused on this request (for waker integration).
   */
  'taskId'?: (string);
  /**
   * Optional metadata propagated to the resulting grant.
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Lifecycle timestamps (unix seconds).
   */
  'createdAt'?: (number | string | Long);
  /**
   * server-set; computed from requested_duration_seconds capped by policy
   */
  'expiresAt'?: (number | string | Long);
  /**
   * 0 until status moves out of PENDING
   */
  'resolvedAt'?: (number | string | Long);
  /**
   * Populated on APPROVED.
   */
  'grantedGrantId'?: (string);
  'resolvedBy'?: (_aether_v1_PrincipalRef | null);
  /**
   * Populated on DENIED / EXPIRED / CANCELLED.
   */
  'resolutionReason'?: (string);
}

/**
 * AuthorityRequest is a typed "sudo" -- a running task asks for elevated
 * authority and parks until an approver resolves it. Approval mints a
 * standard AuthorityGrant via the existing CreateAuthorityGrant path; no
 * parallel grant type exists.
 */
export interface AuthorityRequest__Output {
  'requestId': (string);
  'status': (_aether_v1_AuthorityRequestStatus__Output);
  /**
   * Authority being requested ("desired grant").
   */
  'requestingActor': (_aether_v1_PrincipalRef__Output | null);
  /**
   * empty = same as requesting_actor (the common case)
   */
  'targetSubject': (_aether_v1_PrincipalRef__Output | null);
  'desiredWorkspaceScope': (string)[];
  'desiredResourceScope': (_aether_v1_AuthorityRequestResourceScopeEntry__Output)[];
  'desiredOperationScope': (string)[];
  'requestedAccessLevel': (_aether_v1_AccessLevel__Output);
  'requestedDurationSeconds': (string);
  'audienceType': (string);
  'audienceId': (string);
  /**
   * Routing: who can approve. Required.
   */
  'routingTarget': (_aether_v1_AuthorityRequestRoutingTarget__Output | null);
  /**
   * Free-text narrative for the audit trail.
   */
  'reason': (string);
  /**
   * Optional: the task that is paused on this request (for waker integration).
   */
  'taskId': (string);
  /**
   * Optional metadata propagated to the resulting grant.
   */
  'metadata': ({[key: string]: string});
  /**
   * Lifecycle timestamps (unix seconds).
   */
  'createdAt': (string);
  /**
   * server-set; computed from requested_duration_seconds capped by policy
   */
  'expiresAt': (string);
  /**
   * 0 until status moves out of PENDING
   */
  'resolvedAt': (string);
  /**
   * Populated on APPROVED.
   */
  'grantedGrantId': (string);
  'resolvedBy': (_aether_v1_PrincipalRef__Output | null);
  /**
   * Populated on DENIED / EXPIRED / CANCELLED.
   */
  'resolutionReason': (string);
}
