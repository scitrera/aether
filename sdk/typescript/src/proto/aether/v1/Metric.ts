// Original file: aether.proto

import type { MetricEntry as _aether_v1_MetricEntry, MetricEntry__Output as _aether_v1_MetricEntry__Output } from '../../aether/v1/MetricEntry';
import type { Long } from '@grpc/proto-loader';

/**
 * Metric is the canonical payload for SendMessage when message_type == METRIC.
 * All entries are interpreted as additive deltas; negative qty requires the
 * `capability/metric_credit` ACL permission on the sender.
 */
export interface Metric {
  /**
   * Optional correlation ID (request/trace) tying these entries to upstream work.
   */
  'traceId'?: (string);
  /**
   * One or more counter deltas. At least one entry is required.
   */
  'entries'?: (_aether_v1_MetricEntry)[];
  /**
   * Free-form tags / hints. Documented well-known keys:
   * lifecycle      = "startup" | "shutdown"
   * source.version, source.region, source.host, ...
   */
  'metadata'?: ({[key: string]: string});
  /**
   * Optional client-side timestamp (ms since epoch). The server always also
   * stamps MessageEnvelope.timestamp_ms; this is for client-side ordering hints.
   */
  'clientTimestampMs'?: (number | string | Long);
}

/**
 * Metric is the canonical payload for SendMessage when message_type == METRIC.
 * All entries are interpreted as additive deltas; negative qty requires the
 * `capability/metric_credit` ACL permission on the sender.
 */
export interface Metric__Output {
  /**
   * Optional correlation ID (request/trace) tying these entries to upstream work.
   */
  'traceId': (string);
  /**
   * One or more counter deltas. At least one entry is required.
   */
  'entries': (_aether_v1_MetricEntry__Output)[];
  /**
   * Free-form tags / hints. Documented well-known keys:
   * lifecycle      = "startup" | "shutdown"
   * source.version, source.region, source.host, ...
   */
  'metadata': ({[key: string]: string});
  /**
   * Optional client-side timestamp (ms since epoch). The server always also
   * stamps MessageEnvelope.timestamp_ms; this is for client-side ordering hints.
   */
  'clientTimestampMs': (string);
}
