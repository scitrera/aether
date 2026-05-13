/**
 * Fluent builder for constructing Metric payloads.
 *
 * Metric is the canonical payload for SendMessage when message_type == METRIC.
 * All entries are interpreted as additive deltas; negative qty values require
 * the `capability/metric_credit` ACL permission on the sender.
 *
 * @module metrics-builder
 *
 * @example
 * ```typescript
 * import { newMetric } from "@scitrera/aether-client";
 *
 * const metric = newMetric()
 *   .trace("req-abc-123")
 *   .add("tokens_in", "modelA", 512)
 *   .add("tokens_out", "modelA", 128)
 *   .tag("source.version", "1.0.0")
 *   .clientTimestampMs(Date.now())
 *   .build();
 *
 * agent.sendMetric(metric);
 * ```
 */

import type { Metric } from './proto/aether/v1/Metric.js';
import type { MetricEntry } from './proto/aether/v1/MetricEntry.js';

export type { Metric, MetricEntry };

/**
 * Fluent builder for constructing a {@link Metric}.
 */
export class MetricBuilder {
  private m: Metric;

  constructor() {
    this.m = {
      traceId: '',
      entries: [],
      metadata: {},
      clientTimestampMs: 0,
    };
  }

  /**
   * Sets the optional correlation/trace ID tying these entries to upstream work.
   */
  trace(id: string): this {
    this.m.traceId = id;
    return this;
  }

  /**
   * Adds a metric entry (additive delta).
   *
   * @param name - Counter name, e.g. "tokens_in", "time_seconds"
   * @param kind - Sub-classifier, e.g. "modelA" (may be empty)
   * @param qty  - Additive delta; negative requires `capability/metric_credit`
   */
  add(name: string, kind: string, qty: number): this {
    if (!this.m.entries) {
      this.m.entries = [];
    }
    this.m.entries.push({ name, kind, qty });
    return this;
  }

  /**
   * Adds a free-form metadata tag.
   *
   * Well-known keys: lifecycle ("startup" | "shutdown"),
   * source.version, source.region, source.host, ...
   */
  tag(key: string, value: string): this {
    if (!this.m.metadata) {
      this.m.metadata = {};
    }
    this.m.metadata[key] = value;
    return this;
  }

  /**
   * Sets the optional client-side timestamp (ms since epoch).
   *
   * The server always stamps its own authoritative timestamp on the
   * MessageEnvelope; this field is an advisory ordering hint only.
   *
   * Accepts `number` or `string` to support int64 values that exceed
   * `Number.MAX_SAFE_INTEGER` (the generated proto type allows both,
   * driven by `--longs=String` in compile_protos.sh).
   */
  clientTimestampMs(ts: number | string): this {
    this.m.clientTimestampMs = ts;
    return this;
  }

  /**
   * Returns the constructed Metric object.
   */
  build(): Metric {
    return this.m;
  }
}

/**
 * Creates a new {@link MetricBuilder}.
 *
 * @example
 * ```typescript
 * const metric = newMetric()
 *   .add("tokens_in", "gpt4", 512)
 *   .build();
 * ```
 */
export function newMetric(): MetricBuilder {
  return new MetricBuilder();
}
