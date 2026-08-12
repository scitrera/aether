// Original file: aether.proto

import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { WorkflowScheduleAuthorityScope as _aether_v1_WorkflowScheduleAuthorityScope, WorkflowScheduleAuthorityScope__Output as _aether_v1_WorkflowScheduleAuthorityScope__Output } from '../../aether/v1/WorkflowScheduleAuthorityScope';
import type { WorkflowRequestContext as _aether_v1_WorkflowRequestContext, WorkflowRequestContext__Output as _aether_v1_WorkflowRequestContext__Output } from '../../aether/v1/WorkflowRequestContext';

// Original file: aether.proto

export const _aether_v1_WorkflowOperation_OpType = {
  /**
   * Rules
   */
  LIST_RULES: 'LIST_RULES',
  GET_RULE: 'GET_RULE',
  CREATE_RULE: 'CREATE_RULE',
  UPDATE_RULE: 'UPDATE_RULE',
  DELETE_RULE: 'DELETE_RULE',
  /**
   * Workflow Definitions
   */
  LIST_WORKFLOWS: 'LIST_WORKFLOWS',
  GET_WORKFLOW: 'GET_WORKFLOW',
  CREATE_WORKFLOW: 'CREATE_WORKFLOW',
  DELETE_WORKFLOW: 'DELETE_WORKFLOW',
  /**
   * Schedules
   */
  LIST_SCHEDULES: 'LIST_SCHEDULES',
  CREATE_SCHEDULE: 'CREATE_SCHEDULE',
  DELETE_SCHEDULE: 'DELETE_SCHEDULE',
  /**
   * Executions
   */
  LIST_EXECUTIONS: 'LIST_EXECUTIONS',
  GET_EXECUTION: 'GET_EXECUTION',
  CANCEL_EXECUTION: 'CANCEL_EXECUTION',
  /**
   * State Machines
   */
  LIST_STATE_MACHINES: 'LIST_STATE_MACHINES',
  GET_STATE_MACHINE: 'GET_STATE_MACHINE',
  CREATE_STATE_MACHINE: 'CREATE_STATE_MACHINE',
  DELETE_STATE_MACHINE: 'DELETE_STATE_MACHINE',
  /**
   * State Machine Instances
   */
  LIST_SM_INSTANCES: 'LIST_SM_INSTANCES',
  GET_SM_INSTANCE: 'GET_SM_INSTANCE',
  CREATE_SM_INSTANCE: 'CREATE_SM_INSTANCE',
  SEND_SM_EVENT: 'SEND_SM_EVENT',
  /**
   * Schedule upsert (idempotent create-or-update)
   */
  UPSERT_SCHEDULE: 'UPSERT_SCHEDULE',
  /**
   * Fan-in / barrier / coalesce join observability + operator control.
   */
  LIST_JOINS: 'LIST_JOINS',
  /**
   * inspect one instance: id=join_name, secondary_id=correlation_key
   */
  GET_JOIN: 'GET_JOIN',
  /**
   * operator GC of a wedged instance
   */
  CANCEL_JOIN: 'CANCEL_JOIN',
} as const;

export type _aether_v1_WorkflowOperation_OpType =
  /**
   * Rules
   */
  | 'LIST_RULES'
  | 0
  | 'GET_RULE'
  | 1
  | 'CREATE_RULE'
  | 2
  | 'UPDATE_RULE'
  | 3
  | 'DELETE_RULE'
  | 4
  /**
   * Workflow Definitions
   */
  | 'LIST_WORKFLOWS'
  | 5
  | 'GET_WORKFLOW'
  | 6
  | 'CREATE_WORKFLOW'
  | 7
  | 'DELETE_WORKFLOW'
  | 8
  /**
   * Schedules
   */
  | 'LIST_SCHEDULES'
  | 9
  | 'CREATE_SCHEDULE'
  | 10
  | 'DELETE_SCHEDULE'
  | 11
  /**
   * Executions
   */
  | 'LIST_EXECUTIONS'
  | 12
  | 'GET_EXECUTION'
  | 13
  | 'CANCEL_EXECUTION'
  | 14
  /**
   * State Machines
   */
  | 'LIST_STATE_MACHINES'
  | 15
  | 'GET_STATE_MACHINE'
  | 16
  | 'CREATE_STATE_MACHINE'
  | 17
  | 'DELETE_STATE_MACHINE'
  | 18
  /**
   * State Machine Instances
   */
  | 'LIST_SM_INSTANCES'
  | 19
  | 'GET_SM_INSTANCE'
  | 20
  | 'CREATE_SM_INSTANCE'
  | 21
  | 'SEND_SM_EVENT'
  | 22
  /**
   * Schedule upsert (idempotent create-or-update)
   */
  | 'UPSERT_SCHEDULE'
  | 23
  /**
   * Fan-in / barrier / coalesce join observability + operator control.
   */
  | 'LIST_JOINS'
  | 24
  /**
   * inspect one instance: id=join_name, secondary_id=correlation_key
   */
  | 'GET_JOIN'
  | 25
  /**
   * operator GC of a wedged instance
   */
  | 'CANCEL_JOIN'
  | 26

export type _aether_v1_WorkflowOperation_OpType__Output = typeof _aether_v1_WorkflowOperation_OpType[keyof typeof _aether_v1_WorkflowOperation_OpType]

/**
 * WorkflowOperation allows clients to manage workflow rules, definitions,
 * schedules, executions, and state machines through the gRPC streaming interface.
 * Operations are forwarded by the gateway to the connected workflow engine and
 * responses are relayed back to the requesting client.
 */
export interface WorkflowOperation {
  'op'?: (_aether_v1_WorkflowOperation_OpType);
  /**
   * Entity ID for GET/UPDATE/DELETE
   */
  'id'?: (string);
  /**
   * e.g., instance_id for SM instance ops
   */
  'secondaryId'?: (string);
  /**
   * Workspace filter
   */
  'workspace'?: (string);
  /**
   * JSON payload for CREATE/UPDATE ops
   */
  'data'?: (Buffer | Uint8Array | string);
  /**
   * Correlation ID
   */
  'requestId'?: (string);
  /**
   * For LIST_EXECUTIONS
   */
  'statusFilter'?: (string);
  /**
   * Optional caller OBO authority. Resolved by the gateway before the request
   * is authorized and forwarded.
   */
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  /**
   * Optional requested authority for CREATE_SCHEDULE / UPSERT_SCHEDULE. The
   * gateway derives or mints the exact WorkflowEngine schedule grant and
   * forwards only the resulting trusted request_context.
   */
  'scheduleAuthorityScope'?: (_aether_v1_WorkflowScheduleAuthorityScope | null);
  /**
   * Gateway-authored; caller values are always discarded.
   */
  'requestContext'?: (_aether_v1_WorkflowRequestContext | null);
}

/**
 * WorkflowOperation allows clients to manage workflow rules, definitions,
 * schedules, executions, and state machines through the gRPC streaming interface.
 * Operations are forwarded by the gateway to the connected workflow engine and
 * responses are relayed back to the requesting client.
 */
export interface WorkflowOperation__Output {
  'op': (_aether_v1_WorkflowOperation_OpType__Output);
  /**
   * Entity ID for GET/UPDATE/DELETE
   */
  'id': (string);
  /**
   * e.g., instance_id for SM instance ops
   */
  'secondaryId': (string);
  /**
   * Workspace filter
   */
  'workspace': (string);
  /**
   * JSON payload for CREATE/UPDATE ops
   */
  'data': (Buffer);
  /**
   * Correlation ID
   */
  'requestId': (string);
  /**
   * For LIST_EXECUTIONS
   */
  'statusFilter': (string);
  /**
   * Optional caller OBO authority. Resolved by the gateway before the request
   * is authorized and forwarded.
   */
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  /**
   * Optional requested authority for CREATE_SCHEDULE / UPSERT_SCHEDULE. The
   * gateway derives or mints the exact WorkflowEngine schedule grant and
   * forwards only the resulting trusted request_context.
   */
  'scheduleAuthorityScope': (_aether_v1_WorkflowScheduleAuthorityScope__Output | null);
  /**
   * Gateway-authored; caller values are always discarded.
   */
  'requestContext': (_aether_v1_WorkflowRequestContext__Output | null);
}
