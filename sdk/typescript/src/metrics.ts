/**
 * Metrics bridge client implementation for the Aether TypeScript SDK.
 *
 * The metrics bridge is a receive-only client that subscribes to metric.*
 * topics to collect telemetry data from agents and tasks. It is a singleton
 * per gateway deployment.
 *
 * @module metrics
 */

import { AetherClient } from "./client.js";
import type { AetherClientOptions } from "./client.js";
import { MessageType } from "./types.js";

// =============================================================================
// Metrics Bridge Client Options
// =============================================================================

/**
 * Configuration options for the MetricsBridgeClient.
 *
 * No additional identity fields are needed — the metrics bridge is
 * identified by its principal type alone (singleton per gateway).
 */
export type MetricsBridgeClientOptions = AetherClientOptions;

// =============================================================================
// MetricsBridgeClient
// =============================================================================

/**
 * Client for connecting to the Aether gateway as a metrics bridge.
 *
 * The metrics bridge is receive-only: it subscribes to metric.* topics
 * to collect telemetry data published by agents and tasks. It does not
 * send messages to other principals.
 *
 * @example
 * ```typescript
 * const bridge = new MetricsBridgeClient({
 *   address: "localhost:50051",
 * });
 *
 * bridge.onMessage((msg) => {
 *   // Process incoming metrics
 *   const metric = JSON.parse(new TextDecoder().decode(msg.payload));
 *   console.log(`Metric from ${msg.sourceTopic}:`, metric);
 * });
 *
 * await bridge.connect();
 * ```
 */
export class MetricsBridgeClient extends AetherClient {
  constructor(options: MetricsBridgeClientOptions) {
    super(options);
  }

  // ===========================================================================
  // Init Message
  // ===========================================================================

  /** @internal */
  protected override _buildInitMessage(): Record<string, unknown> {
    return {
      metricsBridge: {},
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // Messaging
  // ===========================================================================

  /**
   * Sends an acknowledgment to a source topic.
   *
   * @param targetTopic - The topic to send the acknowledgment to
   * @param payload - The acknowledgment payload
   */
  sendAcknowledgment(targetTopic: string, payload: Uint8Array): void {
    this._sendMessage(targetTopic, payload, MessageType.Control);
  }
}
