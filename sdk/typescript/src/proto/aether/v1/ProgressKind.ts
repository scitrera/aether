// Original file: aether.proto

/**
 * ProgressKind classifies a progress update by its intended UI surface or
 * consumer, independent of where the sending agent is hosted. The WebSocket
 * relay uses this to decide which socket.io event to emit, avoiding the
 * overload of agent-hosting workspace values (e.g., `_apps`, `_chat`) for
 * UI-routing decisions.
 */
export const ProgressKind = {
  /**
   * Unclassified — receivers should fall back to legacy heuristics.
   */
  PROGRESS_KIND_UNSPECIFIED: 'PROGRESS_KIND_UNSPECIFIED',
  /**
   * Chat conversation progress (e.g., "Processing...", "Thinking...") that
   * should surface in a chat thread's progress bar. Requires a correlated
   * thread_id (typically carried in metadata) and a user recipient.
   */
  PROGRESS_KIND_CHAT: 'PROGRESS_KIND_CHAT',
  /**
   * App-dashboard progress for a user-visible long-running app or job.
   */
  PROGRESS_KIND_APP: 'PROGRESS_KIND_APP',
  /**
   * Task lifecycle progress (started, running, finished, failed). Consumed
   * primarily by parent agents and orchestrators via the pg::{workspace}
   * broadcast stream — not typically routed to a user surface.
   */
  PROGRESS_KIND_TASK: 'PROGRESS_KIND_TASK',
} as const;

/**
 * ProgressKind classifies a progress update by its intended UI surface or
 * consumer, independent of where the sending agent is hosted. The WebSocket
 * relay uses this to decide which socket.io event to emit, avoiding the
 * overload of agent-hosting workspace values (e.g., `_apps`, `_chat`) for
 * UI-routing decisions.
 */
export type ProgressKind =
  /**
   * Unclassified — receivers should fall back to legacy heuristics.
   */
  | 'PROGRESS_KIND_UNSPECIFIED'
  | 0
  /**
   * Chat conversation progress (e.g., "Processing...", "Thinking...") that
   * should surface in a chat thread's progress bar. Requires a correlated
   * thread_id (typically carried in metadata) and a user recipient.
   */
  | 'PROGRESS_KIND_CHAT'
  | 1
  /**
   * App-dashboard progress for a user-visible long-running app or job.
   */
  | 'PROGRESS_KIND_APP'
  | 2
  /**
   * Task lifecycle progress (started, running, finished, failed). Consumed
   * primarily by parent agents and orchestrators via the pg::{workspace}
   * broadcast stream — not typically routed to a user surface.
   */
  | 'PROGRESS_KIND_TASK'
  | 3

/**
 * ProgressKind classifies a progress update by its intended UI surface or
 * consumer, independent of where the sending agent is hosted. The WebSocket
 * relay uses this to decide which socket.io event to emit, avoiding the
 * overload of agent-hosting workspace values (e.g., `_apps`, `_chat`) for
 * UI-routing decisions.
 */
export type ProgressKind__Output = typeof ProgressKind[keyof typeof ProgressKind]
