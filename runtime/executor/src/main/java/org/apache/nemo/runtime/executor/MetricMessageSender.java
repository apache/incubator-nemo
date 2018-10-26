package org.apache.nemo.runtime.executor;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for metric sender.
 */
@DefaultImplementation(MetricManagerWorker.class)
public interface MetricMessageSender extends AutoCloseable {

  /**
   * Send metric to master.
   * @param metricType type of the metric
   * @param metricId id of the metric
   * @param metricField field of the metric
   * @param metricValue value of the metric which is serialized
   */
  void send(final String metricType, final String metricId, final String metricField, final byte[] metricValue);

  /**
   * Flush all metric inside of the queue.
   */
  void flush();

  /**
   * Flush the metric queue and close the metric dispatch.
   */
  void close();
}
