// Original file: aether.proto


/**
 * MetricEntry is one additive delta within a Metric message.
 */
export interface MetricEntry {
  /**
   * e.g. "tokens_in", "time_seconds", "pages_rendered"
   */
  'name'?: (string);
  /**
   * sub-classifier, e.g. "modelA", "" (free-form, may be empty)
   */
  'kind'?: (string);
  /**
   * additive delta; negative requires capability/metric_credit
   */
  'qty'?: (number | string);
}

/**
 * MetricEntry is one additive delta within a Metric message.
 */
export interface MetricEntry__Output {
  /**
   * e.g. "tokens_in", "time_seconds", "pages_rendered"
   */
  'name': (string);
  /**
   * sub-classifier, e.g. "modelA", "" (free-form, may be empty)
   */
  'kind': (string);
  /**
   * additive delta; negative requires capability/metric_credit
   */
  'qty': (number);
}
