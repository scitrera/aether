// Original file: aether.proto

import type { AuthorityRequestEvent as _aether_v1_AuthorityRequestEvent, AuthorityRequestEvent__Output as _aether_v1_AuthorityRequestEvent__Output } from '../../aether/v1/AuthorityRequestEvent';

/**
 * TaskAuthorityRequestEventRelay re-emits a Phase 2 AuthorityRequestEvent onto
 * the per-task event stream when the request's task_id matches the subscribed
 * task. Just embeds the existing event type.
 */
export interface TaskAuthorityRequestEventRelay {
  'event'?: (_aether_v1_AuthorityRequestEvent | null);
}

/**
 * TaskAuthorityRequestEventRelay re-emits a Phase 2 AuthorityRequestEvent onto
 * the per-task event stream when the request's task_id matches the subscribed
 * task. Just embeds the existing event type.
 */
export interface TaskAuthorityRequestEventRelay__Output {
  'event': (_aether_v1_AuthorityRequestEvent__Output | null);
}
