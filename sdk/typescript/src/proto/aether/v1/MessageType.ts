// Original file: aether.proto

export const MessageType = {
  MESSAGE_TYPE_UNSPECIFIED: 'MESSAGE_TYPE_UNSPECIFIED',
  CHAT: 'CHAT',
  CONTROL: 'CONTROL',
  TOOL_CALL: 'TOOL_CALL',
  EVENT: 'EVENT',
  METRIC: 'METRIC',
  /**
   * OPAQUE: sender and receiver own the payload schema. Aether forwards verbatim
   * and makes no payload assumptions — no validation, no text decoding for audit,
   * no per-type behavior. Default for the SDK convenience helpers (SendToAgent,
   * SendToUser, etc.); use CHAT explicitly for true conversational text.
   */
  OPAQUE: 'OPAQUE',
} as const;

export type MessageType =
  | 'MESSAGE_TYPE_UNSPECIFIED'
  | 0
  | 'CHAT'
  | 1
  | 'CONTROL'
  | 2
  | 'TOOL_CALL'
  | 3
  | 'EVENT'
  | 4
  | 'METRIC'
  | 5
  /**
   * OPAQUE: sender and receiver own the payload schema. Aether forwards verbatim
   * and makes no payload assumptions — no validation, no text decoding for audit,
   * no per-type behavior. Default for the SDK convenience helpers (SendToAgent,
   * SendToUser, etc.); use CHAT explicitly for true conversational text.
   */
  | 'OPAQUE'
  | 6

export type MessageType__Output = typeof MessageType[keyof typeof MessageType]
