// Original file: aether.proto

import type { ProgressStep as _aether_v1_ProgressStep, ProgressStep__Output as _aether_v1_ProgressStep__Output } from '../../aether/v1/ProgressStep';
import type { ProgressKind as _aether_v1_ProgressKind, ProgressKind__Output as _aether_v1_ProgressKind__Output } from '../../aether/v1/ProgressKind';
import type { Long } from '@grpc/proto-loader';

/**
 * ProgressUpdate is sent downstream to subscribers with progress from agents/tasks.
 * Contains the original report plus server-added metadata. Routed through
 * the pg.{workspace} RabbitMQ stream with server-side recipient filtering.
 */
export interface ProgressUpdate {
  /**
   * The identity of the agent/task reporting progress (topic format).
   */
  'source'?: (string);
  /**
   * Task or correlation ID this progress relates to.
   */
  'taskId'?: (string);
  /**
   * Current state of the work.
   */
  'state'?: (string);
  /**
   * Completion percentage (0.0 to 1.0). -1 means indeterminate.
   */
  'completion'?: (number | string);
  /**
   * Human-readable summary.
   */
  'summary'?: (string);
  /**
   * Structured step information.
   */
  'step'?: (_aether_v1_ProgressStep | null);
  /**
   * Server timestamp when progress was received (Unix milliseconds).
   */
  'timestampMs'?: (number | string | Long);
  /**
   * Workspace the progress originated from.
   */
  'workspace'?: (string);
  /**
   * Echoed from ProgressReport for correlation.
   */
  'requestId'?: (string);
  /**
   * Arbitrary metadata from the reporter.
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Target recipient (identity topic format). Used by the gateway for
   * server-side filtering. Empty = broadcast to all workspace subscribers.
   * For user-typed recipients, both "us::{user}::{window}" (exact match)
   * and bare "us::{user}" (prefix match → all that user's windows) are
   * accepted. See ProgressReport.recipient for full semantics.
   */
  'recipient'?: (string);
  /**
   * Propagated from ProgressReport.kind. See ProgressKind enum.
   */
  'kind'?: (_aether_v1_ProgressKind);
}

/**
 * ProgressUpdate is sent downstream to subscribers with progress from agents/tasks.
 * Contains the original report plus server-added metadata. Routed through
 * the pg.{workspace} RabbitMQ stream with server-side recipient filtering.
 */
export interface ProgressUpdate__Output {
  /**
   * The identity of the agent/task reporting progress (topic format).
   */
  'source': (string);
  /**
   * Task or correlation ID this progress relates to.
   */
  'taskId': (string);
  /**
   * Current state of the work.
   */
  'state': (string);
  /**
   * Completion percentage (0.0 to 1.0). -1 means indeterminate.
   */
  'completion': (number);
  /**
   * Human-readable summary.
   */
  'summary': (string);
  /**
   * Structured step information.
   */
  'step': (_aether_v1_ProgressStep__Output | null);
  /**
   * Server timestamp when progress was received (Unix milliseconds).
   */
  'timestampMs': (string);
  /**
   * Workspace the progress originated from.
   */
  'workspace': (string);
  /**
   * Echoed from ProgressReport for correlation.
   */
  'requestId': (string);
  /**
   * Arbitrary metadata from the reporter.
   */
  'metadata': ({[key: string]: string});
  /**
   * Target recipient (identity topic format). Used by the gateway for
   * server-side filtering. Empty = broadcast to all workspace subscribers.
   * For user-typed recipients, both "us::{user}::{window}" (exact match)
   * and bare "us::{user}" (prefix match → all that user's windows) are
   * accepted. See ProgressReport.recipient for full semantics.
   */
  'recipient': (string);
  /**
   * Propagated from ProgressReport.kind. See ProgressKind enum.
   */
  'kind': (_aether_v1_ProgressKind__Output);
}
