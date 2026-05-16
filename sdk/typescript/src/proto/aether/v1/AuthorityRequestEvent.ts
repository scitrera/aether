// Original file: aether.proto

import type { AuthorityRequest as _aether_v1_AuthorityRequest, AuthorityRequest__Output as _aether_v1_AuthorityRequest__Output } from '../../aether/v1/AuthorityRequest';
import type { Long } from '@grpc/proto-loader';

// Original file: aether.proto

export const _aether_v1_AuthorityRequestEvent_EventType = {
  AUTHORITY_REQUEST_EVENT_UNSPECIFIED: 'AUTHORITY_REQUEST_EVENT_UNSPECIFIED',
  AUTHORITY_REQUEST_EVENT_CREATED: 'AUTHORITY_REQUEST_EVENT_CREATED',
  AUTHORITY_REQUEST_EVENT_APPROVED: 'AUTHORITY_REQUEST_EVENT_APPROVED',
  AUTHORITY_REQUEST_EVENT_DENIED: 'AUTHORITY_REQUEST_EVENT_DENIED',
  AUTHORITY_REQUEST_EVENT_EXPIRED: 'AUTHORITY_REQUEST_EVENT_EXPIRED',
  AUTHORITY_REQUEST_EVENT_CANCELLED: 'AUTHORITY_REQUEST_EVENT_CANCELLED',
} as const;

export type _aether_v1_AuthorityRequestEvent_EventType =
  | 'AUTHORITY_REQUEST_EVENT_UNSPECIFIED'
  | 0
  | 'AUTHORITY_REQUEST_EVENT_CREATED'
  | 1
  | 'AUTHORITY_REQUEST_EVENT_APPROVED'
  | 2
  | 'AUTHORITY_REQUEST_EVENT_DENIED'
  | 3
  | 'AUTHORITY_REQUEST_EVENT_EXPIRED'
  | 4
  | 'AUTHORITY_REQUEST_EVENT_CANCELLED'
  | 5

export type _aether_v1_AuthorityRequestEvent_EventType__Output = typeof _aether_v1_AuthorityRequestEvent_EventType[keyof typeof _aether_v1_AuthorityRequestEvent_EventType]

/**
 * AuthorityRequestEvent is the downstream notification published when a
 * request transitions. Used by Stage C (subscription/waker integration).
 */
export interface AuthorityRequestEvent {
  'eventType'?: (_aether_v1_AuthorityRequestEvent_EventType);
  'request'?: (_aether_v1_AuthorityRequest | null);
  'emittedAt'?: (number | string | Long);
}

/**
 * AuthorityRequestEvent is the downstream notification published when a
 * request transitions. Used by Stage C (subscription/waker integration).
 */
export interface AuthorityRequestEvent__Output {
  'eventType': (_aether_v1_AuthorityRequestEvent_EventType__Output);
  'request': (_aether_v1_AuthorityRequest__Output | null);
  'emittedAt': (string);
}
