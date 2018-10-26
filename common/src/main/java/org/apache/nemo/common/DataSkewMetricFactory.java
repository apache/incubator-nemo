package org.apache.nemo.common;

import java.util.Map;

/**
 * A {@link MetricFactory} which is used for data skew handling.
 */
public final class DataSkewMetricFactory implements MetricFactory<Map<Integer, KeyRange>> {
  private Map<Integer, KeyRange> metric;

  /**
   * Default constructor.
   */
  public DataSkewMetricFactory(final Map<Integer, KeyRange> metric) {
    this.metric = metric;
  }

  public Map<Integer, KeyRange> getMetric() {
    return metric;
  }
}
