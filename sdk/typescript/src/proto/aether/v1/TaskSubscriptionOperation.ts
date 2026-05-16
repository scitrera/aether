// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

// Original file: aether.proto

export const _aether_v1_TaskSubscriptionOperation_OpType = {
  TASK_SUBSCRIPTION_OP_UNSPECIFIED: 'TASK_SUBSCRIPTION_OP_UNSPECIFIED',
  /**
   * start streaming events for the given task_id
   */
  SUBSCRIBE: 'SUBSCRIBE',
  /**
   * stop streaming
   */
  UNSUBSCRIBE: 'UNSUBSCRIBE',
} as const;

export type _aether_v1_TaskSubscriptionOperation_OpType =
  | 'TASK_SUBSCRIPTION_OP_UNSPECIFIED'
  | 0
  /**
   * start streaming events for the given task_id
   */
  | 'SUBSCRIBE'
  | 1
  /**
   * stop streaming
   */
  | 'UNSUBSCRIBE'
  | 2

export type _aether_v1_TaskSubscriptionOperation_OpType__Output = typeof _aether_v1_TaskSubscriptionOperation_OpType[keyof typeof _aether_v1_TaskSubscriptionOperation_OpType]

/**
 * TaskSubscriptionOperation is the upstream op for the per-task event stream.
 * Clients send SUBSCRIBE to start streaming status / progress / child-lifecycle /
 * authority-request events for a specific task; UNSUBSCRIBE stops the stream.
 * The gateway emits TaskEvent messages downstream until the subscription is
 * cancelled or the session disconnects.
 */
export interface TaskSubscriptionOperation {
  'op'?: (_aether_v1_TaskSubscriptionOperation_OpType);
  'taskId'?: (string);
  /**
   * When true, also stream events for descendant tasks (children, grandchildren).
   * Stage B implements a snapshot-at-subscribe model: descendants known at
   * subscribe time are tracked, but tasks born after subscribe are NOT picked
   * up automatically. Default false = only events for task_id itself.
   */
  'recursive'?: (boolean);
  /**
   * Optional client-generated correlation id; echoed in TaskSubscriptionOperationResponse.
   */
  'clientRequestId'?: (string);
  /**
   * Optional: cold-start cursor. When non-empty, the gateway begins replay
   * from this unix-ms timestamp instead of "now". Empty / 0 = live-only.
   */
  'startTimestampUnixMs'?: (number | string | Long);
  /**
   * For UNSUBSCRIBE: the subscription_id issued by the server on SUBSCRIBE.
   * When empty the gateway falls back to (task_id, recursive) match.
   */
  'subscriptionId'?: (string);
}

/**
 * TaskSubscriptionOperation is the upstream op for the per-task event stream.
 * Clients send SUBSCRIBE to start streaming status / progress / child-lifecycle /
 * authority-request events for a specific task; UNSUBSCRIBE stops the stream.
 * The gateway emits TaskEvent messages downstream until the subscription is
 * cancelled or the session disconnects.
 */
export interface TaskSubscriptionOperation__Output {
  'op': (_aether_v1_TaskSubscriptionOperation_OpType__Output);
  'taskId': (string);
  /**
   * When true, also stream events for descendant tasks (children, grandchildren).
   * Stage B implements a snapshot-at-subscribe model: descendants known at
   * subscribe time are tracked, but tasks born after subscribe are NOT picked
   * up automatically. Default false = only events for task_id itself.
   */
  'recursive': (boolean);
  /**
   * Optional client-generated correlation id; echoed in TaskSubscriptionOperationResponse.
   */
  'clientRequestId': (string);
  /**
   * Optional: cold-start cursor. When non-empty, the gateway begins replay
   * from this unix-ms timestamp instead of "now". Empty / 0 = live-only.
   */
  'startTimestampUnixMs': (string);
  /**
   * For UNSUBSCRIBE: the subscription_id issued by the server on SUBSCRIBE.
   * When empty the gateway falls back to (task_id, recursive) match.
   */
  'subscriptionId': (string);
}
