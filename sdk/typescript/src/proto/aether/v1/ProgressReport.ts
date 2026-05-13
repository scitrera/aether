// Original file: aether.proto

import type { ProgressStep as _aether_v1_ProgressStep, ProgressStep__Output as _aether_v1_ProgressStep__Output } from '../../aether/v1/ProgressStep';
import type { ProgressKind as _aether_v1_ProgressKind, ProgressKind__Output as _aether_v1_ProgressKind__Output } from '../../aether/v1/ProgressKind';

/**
 * ProgressReport is sent upstream by agents/tasks to report progress on work.
 * Progress is supplemental information while a task is running — connection
 * liveness (heartbeat/lock) handles death detection separately.
 * The gateway publishes progress to the pg.{workspace} stream and fans it
 * out to subscribers (users, agents, orchestrators) with server-side filtering.
 */
export interface ProgressReport {
  /**
   * Task or request identifier this progress relates to.
   * For orchestrated tasks, use the Aether task_id.
   * For ad-hoc work, use a client-chosen correlation ID.
   */
  'taskId'?: (string);
  /**
   * Current state of the work (e.g., "running", "finishing", "idle").
   * Not an enum to allow domain-specific states.
   */
  'state'?: (string);
  /**
   * Completion percentage (0.0 to 1.0). -1 means indeterminate.
   */
  'completion'?: (number | string);
  /**
   * Human-readable summary of what is currently happening.
   */
  'summary'?: (string);
  /**
   * Structured step information for multi-step progress.
   */
  'step'?: (_aether_v1_ProgressStep | null);
  /**
   * Target recipient for this progress update (identity topic format).
   * Empty = broadcast to all subscribers in the sender's workspace.
   * 
   * Recipient forms for user targeting:
   * - "us::{user}::{window}"  — exactly one window of that user
   * - "us::{user}"            — every open window of that user
   * (cross-workspace; bare-user form, no specifier)
   * 
   * Other identity topics (ag::, ta::, tu::) are matched exactly at delivery.
   * This is a privacy boundary enforced server-side.
   */
  'recipient'?: (string);
  /**
   * Optional: request_id for correlating with a specific user request.
   */
  'requestId'?: (string);
  /**
   * Arbitrary key-value metadata for domain-specific progress details.
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Classifies this report by its intended consumer / UI surface. When set,
   * the gateway propagates this onto ProgressUpdate.kind and receivers can
   * route without inspecting workspace pseudo-identifiers.
   */
  'kind'?: (_aether_v1_ProgressKind);
}

/**
 * ProgressReport is sent upstream by agents/tasks to report progress on work.
 * Progress is supplemental information while a task is running — connection
 * liveness (heartbeat/lock) handles death detection separately.
 * The gateway publishes progress to the pg.{workspace} stream and fans it
 * out to subscribers (users, agents, orchestrators) with server-side filtering.
 */
export interface ProgressReport__Output {
  /**
   * Task or request identifier this progress relates to.
   * For orchestrated tasks, use the Aether task_id.
   * For ad-hoc work, use a client-chosen correlation ID.
   */
  'taskId': (string);
  /**
   * Current state of the work (e.g., "running", "finishing", "idle").
   * Not an enum to allow domain-specific states.
   */
  'state': (string);
  /**
   * Completion percentage (0.0 to 1.0). -1 means indeterminate.
   */
  'completion': (number);
  /**
   * Human-readable summary of what is currently happening.
   */
  'summary': (string);
  /**
   * Structured step information for multi-step progress.
   */
  'step': (_aether_v1_ProgressStep__Output | null);
  /**
   * Target recipient for this progress update (identity topic format).
   * Empty = broadcast to all subscribers in the sender's workspace.
   * 
   * Recipient forms for user targeting:
   * - "us::{user}::{window}"  — exactly one window of that user
   * - "us::{user}"            — every open window of that user
   * (cross-workspace; bare-user form, no specifier)
   * 
   * Other identity topics (ag::, ta::, tu::) are matched exactly at delivery.
   * This is a privacy boundary enforced server-side.
   */
  'recipient': (string);
  /**
   * Optional: request_id for correlating with a specific user request.
   */
  'requestId': (string);
  /**
   * Arbitrary key-value metadata for domain-specific progress details.
   */
  'metadata': ({[key: string]: string});
  /**
   * Classifies this report by its intended consumer / UI surface. When set,
   * the gateway propagates this onto ProgressUpdate.kind and receivers can
   * route without inspecting workspace pseudo-identifiers.
   */
  'kind': (_aether_v1_ProgressKind__Output);
}
