package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * MetricCollection ExecutionProperty.
 */
public final class MetricCollectionProperty extends EdgeExecutionProperty<MetricCollectionProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private MetricCollectionProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static MetricCollectionProperty of(final Value value) {
    return new MetricCollectionProperty(value);
  }

  /**
   * Possible values of MetricCollection ExecutionProperty.
   */
  public enum Value {
    DataSkewRuntimePass
  }
}
