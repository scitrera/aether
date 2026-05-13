// Original file: aether.proto

import type { MessageType as _aether_v1_MessageType, MessageType__Output as _aether_v1_MessageType__Output } from '../../aether/v1/MessageType';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from '../../aether/v1/AuthorizationContext';

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
}
