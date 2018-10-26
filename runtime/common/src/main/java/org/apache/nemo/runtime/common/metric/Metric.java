package org.apache.nemo.runtime.common.metric;

/**
 * Interface for all metrics.
 */
public interface Metric {
  /**
   * Get its unique id.
   * @return an unique id
   */
  String getId();

  /**
   * Process metric message from evaluators.
   * @param metricField field name of the metric.
   * @param metricValue byte array of serialized data value.
   * @return true if the metric was changed or false if not.
   */
  boolean processMetricMessage(final String metricField, final byte[] metricValue);
}
