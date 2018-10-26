package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.DataSkewMetricFactory;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * DataSkewMetric ExecutionProperty.
 */
public final class DataSkewMetricProperty extends EdgeExecutionProperty<DataSkewMetricFactory> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private DataSkewMetricProperty(final DataSkewMetricFactory value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DataSkewMetricProperty of(final DataSkewMetricFactory value) {
    return new DataSkewMetricProperty(value);
  }
}
