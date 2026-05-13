// Original file: aether.proto

import type { MessageType as _aether_v1_MessageType, MessageType__Output as _aether_v1_MessageType__Output } from '../../aether/v1/MessageType';

export interface IncomingMessage {
  'sourceTopic'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
  'messageType'?: (_aether_v1_MessageType);
  /**
   * Workspace context for this message, as declared by the sender (via
   * SendMessage.app_workspace or the workspace component of event::/metric::
   * target topics) and verified by the gateway. Empty when no workspace
   * applies (e.g., bridge messages, service messages). Mirrors
   * SendMessage.app_workspace on the receive side. Workflow engines and
   * metrics bridges, which subscribe to a workspace-agnostic fan-in shard,
   * recover the originating workspace from this field rather than from
   * source_topic (which carries the sender's identity-topic, not the
   * declared event/metric workspace).
   */
  'workspace'?: (string);
}

export interface IncomingMessage__Output {
  'sourceTopic': (string);
  'payload': (Buffer);
  'messageType': (_aether_v1_MessageType__Output);
  /**
   * Workspace context for this message, as declared by the sender (via
   * SendMessage.app_workspace or the workspace component of event::/metric::
   * target topics) and verified by the gateway. Empty when no workspace
   * applies (e.g., bridge messages, service messages). Mirrors
   * SendMessage.app_workspace on the receive side. Workflow engines and
   * metrics bridges, which subscribe to a workspace-agnostic fan-in shard,
   * recover the originating workspace from this field rather than from
   * source_topic (which carries the sender's identity-topic, not the
   * declared event/metric workspace).
   */
  'workspace': (string);
}
