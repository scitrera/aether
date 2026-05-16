// Original file: aether.proto


/**
 * TaskSubscriptionOperationResponse acknowledges a SUBSCRIBE / UNSUBSCRIBE.
 */
export interface TaskSubscriptionOperationResponse {
  'success'?: (boolean);
  'error'?: (string);
  'clientRequestId'?: (string);
  'taskId'?: (string);
  /**
   * Server-issued subscription id for unsubscribe correlation. Stable for the
   * lifetime of the subscription.
   */
  'subscriptionId'?: (string);
}

/**
 * TaskSubscriptionOperationResponse acknowledges a SUBSCRIBE / UNSUBSCRIBE.
 */
export interface TaskSubscriptionOperationResponse__Output {
  'success': (boolean);
  'error': (string);
  'clientRequestId': (string);
  'taskId': (string);
  /**
   * Server-issued subscription id for unsubscribe correlation. Stable for the
   * lifetime of the subscription.
   */
  'subscriptionId': (string);
}
