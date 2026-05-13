// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { Long } from '@grpc/proto-loader';

// Original file: aether.proto

export const _aether_v1_KVOperation_OpType = {
  GET: 'GET',
  PUT: 'PUT',
  LIST: 'LIST',
  DELETE: 'DELETE',
  INCREMENT: 'INCREMENT',
  DECREMENT: 'DECREMENT',
  /**
   * Atomic increment that succeeds only if result <= guard_value
   */
  INCREMENT_IF: 'INCREMENT_IF',
  /**
   * Atomic decrement that succeeds only if result >= guard_value
   */
  DECREMENT_IF: 'DECREMENT_IF',
} as const;

export type _aether_v1_KVOperation_OpType =
  | 'GET'
  | 0
  | 'PUT'
  | 1
  | 'LIST'
  | 2
  | 'DELETE'
  | 3
  | 'INCREMENT'
  | 4
  | 'DECREMENT'
  | 5
  /**
   * Atomic increment that succeeds only if result <= guard_value
   */
  | 'INCREMENT_IF'
  | 6
  /**
   * Atomic decrement that succeeds only if result >= guard_value
   */
  | 'DECREMENT_IF'
  | 7

export type _aether_v1_KVOperation_OpType__Output = typeof _aether_v1_KVOperation_OpType[keyof typeof _aether_v1_KVOperation_OpType]

// Original file: aether.proto

/**
 * Scope identifies the (identity x sharing) cell of the KV matrix.
 * 
 * The four legacy values (GLOBAL, WORKSPACE, USER, USER_WORKSPACE) are
 * preserved with their original tag numbers and semantics:
 * GLOBAL/WORKSPACE         = cross-agent shared
 * USER/USER_WORKSPACE      = per-agent (agent-private)
 * 
 * The four new values complete the 4 (identity) x 2 (sharing) matrix:
 * GLOBAL_EXCLUSIVE         = per-agent, tenant-wide
 * WORKSPACE_EXCLUSIVE      = per-agent, per-workspace
 * USER_SHARED              = cross-agent, per-user
 * USER_WORKSPACE_SHARED    = cross-agent, per-user, per-workspace
 */
export const _aether_v1_KVOperation_Scope = {
  SCOPE_UNSPECIFIED: 'SCOPE_UNSPECIFIED',
  /**
   * shared, tenant-wide
   */
  GLOBAL: 'GLOBAL',
  /**
   * shared, per-workspace
   */
  WORKSPACE: 'WORKSPACE',
  /**
   * per-agent, per-user
   */
  USER: 'USER',
  /**
   * per-agent, per-user, per-workspace
   */
  USER_WORKSPACE: 'USER_WORKSPACE',
  /**
   * per-agent, tenant-wide
   */
  GLOBAL_EXCLUSIVE: 'GLOBAL_EXCLUSIVE',
  /**
   * per-agent, per-workspace
   */
  WORKSPACE_EXCLUSIVE: 'WORKSPACE_EXCLUSIVE',
  /**
   * shared, per-user
   */
  USER_SHARED: 'USER_SHARED',
  /**
   * shared, per-user, per-workspace
   */
  USER_WORKSPACE_SHARED: 'USER_WORKSPACE_SHARED',
} as const;

/**
 * Scope identifies the (identity x sharing) cell of the KV matrix.
 * 
 * The four legacy values (GLOBAL, WORKSPACE, USER, USER_WORKSPACE) are
 * preserved with their original tag numbers and semantics:
 * GLOBAL/WORKSPACE         = cross-agent shared
 * USER/USER_WORKSPACE      = per-agent (agent-private)
 * 
 * The four new values complete the 4 (identity) x 2 (sharing) matrix:
 * GLOBAL_EXCLUSIVE         = per-agent, tenant-wide
 * WORKSPACE_EXCLUSIVE      = per-agent, per-workspace
 * USER_SHARED              = cross-agent, per-user
 * USER_WORKSPACE_SHARED    = cross-agent, per-user, per-workspace
 */
export type _aether_v1_KVOperation_Scope =
  | 'SCOPE_UNSPECIFIED'
  | 0
  /**
   * shared, tenant-wide
   */
  | 'GLOBAL'
  | 1
  /**
   * shared, per-workspace
   */
  | 'WORKSPACE'
  | 2
  /**
   * per-agent, per-user
   */
  | 'USER'
  | 3
  /**
   * per-agent, per-user, per-workspace
   */
  | 'USER_WORKSPACE'
  | 4
  /**
   * per-agent, tenant-wide
   */
  | 'GLOBAL_EXCLUSIVE'
  | 5
  /**
   * per-agent, per-workspace
   */
  | 'WORKSPACE_EXCLUSIVE'
  | 6
  /**
   * shared, per-user
   */
  | 'USER_SHARED'
  | 7
  /**
   * shared, per-user, per-workspace
   */
  | 'USER_WORKSPACE_SHARED'
  | 8

/**
 * Scope identifies the (identity x sharing) cell of the KV matrix.
 * 
 * The four legacy values (GLOBAL, WORKSPACE, USER, USER_WORKSPACE) are
 * preserved with their original tag numbers and semantics:
 * GLOBAL/WORKSPACE         = cross-agent shared
 * USER/USER_WORKSPACE      = per-agent (agent-private)
 * 
 * The four new values complete the 4 (identity) x 2 (sharing) matrix:
 * GLOBAL_EXCLUSIVE         = per-agent, tenant-wide
 * WORKSPACE_EXCLUSIVE      = per-agent, per-workspace
 * USER_SHARED              = cross-agent, per-user
 * USER_WORKSPACE_SHARED    = cross-agent, per-user, per-workspace
 */
export type _aether_v1_KVOperation_Scope__Output = typeof _aether_v1_KVOperation_Scope[keyof typeof _aether_v1_KVOperation_Scope]

/**
 * NOTE: KV values are opaque bytes; use msgpack (or equivalent) for structured data.
 * NOTE: Proto regeneration required after this change (run protoc or go generate).
 */
export interface KVOperation {
  'op'?: (_aether_v1_KVOperation_OpType);
  'scope'?: (_aether_v1_KVOperation_Scope);
  'key'?: (string);
  'value'?: (Buffer | Uint8Array | string);
  /**
   * for user/user-workspace scopes
   */
  'userId'?: (string);
  /**
   * for workspace/user-workspace scopes
   */
  'workspace'?: (string);
  /**
   * TTL in seconds, 0 = no expiration
   */
  'ttl'?: (number | string | Long);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId'?: (string);
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  /**
   * floor for DECREMENT_IF, ceiling for INCREMENT_IF
   */
  'guardValue'?: (number | string | Long);
  /**
   * Step size for INCREMENT_IF / DECREMENT_IF. When 0 the server uses a
   * default of 1, matching unguarded INCREMENT/DECREMENT. Must be >= 0;
   * negative deltas are rejected by the server.
   */
  'deltaValue'?: (number | string | Long);
}

/**
 * NOTE: KV values are opaque bytes; use msgpack (or equivalent) for structured data.
 * NOTE: Proto regeneration required after this change (run protoc or go generate).
 */
export interface KVOperation__Output {
  'op': (_aether_v1_KVOperation_OpType__Output);
  'scope': (_aether_v1_KVOperation_Scope__Output);
  'key': (string);
  'value': (Buffer);
  /**
   * for user/user-workspace scopes
   */
  'userId': (string);
  /**
   * for workspace/user-workspace scopes
   */
  'workspace': (string);
  /**
   * TTL in seconds, 0 = no expiration
   */
  'ttl': (string);
  /**
   * Client-generated correlation ID for matching responses to requests
   */
  'requestId': (string);
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  /**
   * floor for DECREMENT_IF, ceiling for INCREMENT_IF
   */
  'guardValue': (string);
  /**
   * Step size for INCREMENT_IF / DECREMENT_IF. When 0 the server uses a
   * default of 1, matching unguarded INCREMENT/DECREMENT. Must be >= 0;
   * negative deltas are rejected by the server.
   */
  'deltaValue': (string);
}
