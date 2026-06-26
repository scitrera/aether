// Original file: aether.proto

import type { MessageType as _aether_v1_MessageType, MessageType__Output as _aether_v1_MessageType__Output } from '../../aether/v1/MessageType';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from '../../aether/v1/PrincipalRef';
import type { Long } from '@grpc/proto-loader';

/**
 * MessageEnvelope wraps message payloads with server-verified metadata.
 * This envelope is used internally for messages passing through the router.
 * The source is always set by the server based on authenticated identity,
 * ensuring recipients can trust the source information.
 * 
 * Routing information (target topic) is kept in the transport layer
 * (e.g., RabbitMQ stream name, Kafka topic/key) for routing efficiency.
 * Application metadata that doesn't affect routing goes in this envelope.
 */
export interface MessageEnvelope {
  /**
   * Server-verified source identity (topic format, e.g., "ag.prod.worker.inst-1")
   * This is set by the gateway based on the authenticated sender identity.
   */
  'source'?: (string);
  /**
   * The actual message payload (opaque bytes from the sender)
   */
  'payload'?: (Buffer | Uint8Array | string);
  /**
   * Message type for processing hints
   */
  'messageType'?: (_aether_v1_MessageType);
  /**
   * Server timestamp when message was received (Unix milliseconds)
   */
  'timestampMs'?: (number | string | Long);
  /**
   * Optional metadata for future extensibility
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Workspace context for the message, as declared by the sender (via
   * SendMessage.app_workspace or the workspace component of event::/metric::
   * target topics) and verified by the gateway. Empty when no workspace
   * applies (e.g., bridge messages, service messages). Propagated into
   * IncomingMessage.workspace at delivery time so workflow engines and
   * metrics bridges (which subscribe to a workspace-agnostic fan-in shard)
   * can recover the originating workspace.
   */
  'workspace'?: (string);
  /**
   * Gateway-set, spoof-proof resolved on-behalf-of subject. Populated ONLY
   * when the sender's SendMessage carried an AuthorizationContext that the
   * gateway resolved to an OBO subject (valid grant, delegate + audience
   * match) — set from the resolved authority in routeMessage, the same way
   * `source` is set from the authenticated sender and ProxyHttp mints X-Auth-*
   * identity headers. Empty for direct (non-OBO) sends and older gateways.
   * 
   * Lets a recipient identify the *user* a message was sent for, distinct from
   * the sending *identity* in `source`. Example: the agent-harness sends from
   * its agent identity but acts for a user; platform-bridge reads this to scope
   * the tool registry to that user. Subject identity only — recipients that
   * need to *act for* the subject use the task authority-grant path
   * (CreateTaskResponse.authority_grant_id), not this field.
   */
  'onBehalfSubject'?: (_aether_v1_PrincipalRef | null);
}

/**
 * MessageEnvelope wraps message payloads with server-verified metadata.
 * This envelope is used internally for messages passing through the router.
 * The source is always set by the server based on authenticated identity,
 * ensuring recipients can trust the source information.
 * 
 * Routing information (target topic) is kept in the transport layer
 * (e.g., RabbitMQ stream name, Kafka topic/key) for routing efficiency.
 * Application metadata that doesn't affect routing goes in this envelope.
 */
export interface MessageEnvelope__Output {
  /**
   * Server-verified source identity (topic format, e.g., "ag.prod.worker.inst-1")
   * This is set by the gateway based on the authenticated sender identity.
   */
  'source': (string);
  /**
   * The actual message payload (opaque bytes from the sender)
   */
  'payload': (Buffer);
  /**
   * Message type for processing hints
   */
  'messageType': (_aether_v1_MessageType__Output);
  /**
   * Server timestamp when message was received (Unix milliseconds)
   */
  'timestampMs': (string);
  /**
   * Optional metadata for future extensibility
   */
  'metadata': ({[key: string]: string});
  /**
   * Workspace context for the message, as declared by the sender (via
   * SendMessage.app_workspace or the workspace component of event::/metric::
   * target topics) and verified by the gateway. Empty when no workspace
   * applies (e.g., bridge messages, service messages). Propagated into
   * IncomingMessage.workspace at delivery time so workflow engines and
   * metrics bridges (which subscribe to a workspace-agnostic fan-in shard)
   * can recover the originating workspace.
   */
  'workspace': (string);
  /**
   * Gateway-set, spoof-proof resolved on-behalf-of subject. Populated ONLY
   * when the sender's SendMessage carried an AuthorizationContext that the
   * gateway resolved to an OBO subject (valid grant, delegate + audience
   * match) — set from the resolved authority in routeMessage, the same way
   * `source` is set from the authenticated sender and ProxyHttp mints X-Auth-*
   * identity headers. Empty for direct (non-OBO) sends and older gateways.
   * 
   * Lets a recipient identify the *user* a message was sent for, distinct from
   * the sending *identity* in `source`. Example: the agent-harness sends from
   * its agent identity but acts for a user; platform-bridge reads this to scope
   * the tool registry to that user. Subject identity only — recipients that
   * need to *act for* the subject use the task authority-grant path
   * (CreateTaskResponse.authority_grant_id), not this field.
   */
  'onBehalfSubject': (_aether_v1_PrincipalRef__Output | null);
}
