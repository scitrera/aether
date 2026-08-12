// Original file: aether.proto

import type { MessageType as _aether_v1_MessageType, MessageType__Output as _aether_v1_MessageType__Output } from '../../aether/v1/MessageType';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { AccessDecisionReceipt as _aether_v1_AccessDecisionReceipt, AccessDecisionReceipt__Output as _aether_v1_AccessDecisionReceipt__Output } from '../../aether/v1/AccessDecisionReceipt';
import type { ForwardedAuthorization as _aether_v1_ForwardedAuthorization, ForwardedAuthorization__Output as _aether_v1_ForwardedAuthorization__Output } from '../../aether/v1/ForwardedAuthorization';

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
  /**
   * Gateway-set, spoof-proof resolved on-behalf-of subject, mirrored from
   * MessageEnvelope.on_behalf_subject at delivery time. Populated ONLY when the
   * sender's SendMessage carried an AuthorizationContext the gateway resolved
   * to an OBO subject. Lets a recipient identify the *user* a message was sent
   * for, distinct from the sending identity in source_topic. Empty for direct
   * (non-OBO) sends. See MessageEnvelope.on_behalf_subject.
   */
  'onBehalfSubject'?: (_aether_v1_PrincipalRef | null);
  /**
   * Gateway-authored receipt from SendMessage.checked_access. Never populated
   * from the application payload.
   */
  'accessReceipt'?: (_aether_v1_AccessDecisionReceipt | null);
  /**
   * Gateway-derived authority continuation for this exact delivery target.
   * Populated only when SendMessage.forward_authorization was explicitly set
   * and the sender's resolved grant could delegate. Recipients can pass the
   * authorization context to CheckAccess / BatchCheckAccess; root_grant_id,
   * expiry, and delivery_target are trusted binding/audit metadata.
   */
  'forwardedAuthorization'?: (_aether_v1_ForwardedAuthorization | null);
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
  /**
   * Gateway-set, spoof-proof resolved on-behalf-of subject, mirrored from
   * MessageEnvelope.on_behalf_subject at delivery time. Populated ONLY when the
   * sender's SendMessage carried an AuthorizationContext the gateway resolved
   * to an OBO subject. Lets a recipient identify the *user* a message was sent
   * for, distinct from the sending identity in source_topic. Empty for direct
   * (non-OBO) sends. See MessageEnvelope.on_behalf_subject.
   */
  'onBehalfSubject': (_aether_v1_PrincipalRef__Output | null);
  /**
   * Gateway-authored receipt from SendMessage.checked_access. Never populated
   * from the application payload.
   */
  'accessReceipt': (_aether_v1_AccessDecisionReceipt__Output | null);
  /**
   * Gateway-derived authority continuation for this exact delivery target.
   * Populated only when SendMessage.forward_authorization was explicitly set
   * and the sender's resolved grant could delegate. Recipients can pass the
   * authorization context to CheckAccess / BatchCheckAccess; root_grant_id,
   * expiry, and delivery_target are trusted binding/audit metadata.
   */
  'forwardedAuthorization': (_aether_v1_ForwardedAuthorization__Output | null);
}
