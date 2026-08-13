// Original file: aether.proto

import type { MessageType as _aether_v1_MessageType, MessageType__Output as _aether_v1_MessageType__Output } from '../../aether/v1/MessageType';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';
import type { ResourceAccessRequest as _aether_v1_ResourceAccessRequest, ResourceAccessRequest__Output as _aether_v1_ResourceAccessRequest__Output } from '../../aether/v1/ResourceAccessRequest';
import type { AuthorityContinuationRequest as _aether_v1_AuthorityContinuationRequest, AuthorityContinuationRequest__Output as _aether_v1_AuthorityContinuationRequest__Output } from '../../aether/v1/AuthorityContinuationRequest';

export interface SendMessage {
  'targetTopic'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
  'messageType'?: (_aether_v1_MessageType);
  'authorization'?: (_aether_v1_AuthorizationContext | null);
  /**
   * Optional: user's active app workspace. Stamped by the ws-server (or a
   * user client that has this context) so the gateway can scope derived
   * task-authority grants correctly at triggerOrchestration + downstream
   * ops. Ignored when the sender is not a user principal. When absent the
   * gateway falls back to `sender.Workspace` (session-tracked workspace,
   * meaningful for agents/tasks but empty for users today). Will become
   * secondary once authproxy-issued root grants are used as the primary
   * scope source.
   */
  'appWorkspace'?: (string);
  /**
   * Optional exact logical-resource check evaluated in addition to ordinary
   * topic-route authorization. On allow, the resulting receipt is attached to
   * the trusted MessageEnvelope/IncomingMessage metadata; on deny, nothing is
   * published. Existing sends without this field retain their current path.
   */
  'checkedAccess'?: (_aether_v1_ResourceAccessRequest | null);
  /**
   * Explicitly request a gateway-derived, short-lived authorization context
   * for the resolved recipient. The gateway only honors this when the send is
   * already operating under a validated OBO grant with delegation capacity.
   * For sv::{implementation} targets, wildcard resolution happens first and
   * the child grant is bound to the concrete service instance. Exact agent
   * targets require an invocation-bound, explicitly attenuated scope. The
   * recipient receives the result in IncomingMessage.forwarded_authorization;
   * payload data can never populate that trusted field.
   */
  'authorityContinuation'?: (_aether_v1_AuthorityContinuationRequest | null);
}

export interface SendMessage__Output {
  'targetTopic': (string);
  'payload': (Buffer);
  'messageType': (_aether_v1_MessageType__Output);
  'authorization': (_aether_v1_AuthorizationContext__Output | null);
  /**
   * Optional: user's active app workspace. Stamped by the ws-server (or a
   * user client that has this context) so the gateway can scope derived
   * task-authority grants correctly at triggerOrchestration + downstream
   * ops. Ignored when the sender is not a user principal. When absent the
   * gateway falls back to `sender.Workspace` (session-tracked workspace,
   * meaningful for agents/tasks but empty for users today). Will become
   * secondary once authproxy-issued root grants are used as the primary
   * scope source.
   */
  'appWorkspace': (string);
  /**
   * Optional exact logical-resource check evaluated in addition to ordinary
   * topic-route authorization. On allow, the resulting receipt is attached to
   * the trusted MessageEnvelope/IncomingMessage metadata; on deny, nothing is
   * published. Existing sends without this field retain their current path.
   */
  'checkedAccess': (_aether_v1_ResourceAccessRequest__Output | null);
  /**
   * Explicitly request a gateway-derived, short-lived authorization context
   * for the resolved recipient. The gateway only honors this when the send is
   * already operating under a validated OBO grant with delegation capacity.
   * For sv::{implementation} targets, wildcard resolution happens first and
   * the child grant is bound to the concrete service instance. Exact agent
   * targets require an invocation-bound, explicitly attenuated scope. The
   * recipient receives the result in IncomingMessage.forwarded_authorization;
   * payload data can never populate that trusted field.
   */
  'authorityContinuation': (_aether_v1_AuthorityContinuationRequest__Output | null);
}
