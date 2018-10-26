package org.apache.nemo.runtime.master;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Metric message handler.
 */
@DefaultImplementation(MetricManagerMaster.class)
public interface MetricMessageHandler {

  /**
   * Handle the received metric message.
   * @param metricType a given type for the metric (ex. TaskMetric).
   * @param metricId  id of the metric.
   * @param metricField field name of the metric.
   * @param metricValue serialized metric data value.
   */
  void onMetricMessageReceived(final String metricType, final String metricId,
                               final String metricField, final byte[] metricValue);

  /**
   * Cleans up and terminates this handler.
   */
  void terminate();
}
