package edu.snu.onyx.common.ir.edge.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * MetricCollection ExecutionProperty.
 */
public final class MetricCollectionProperty extends ExecutionProperty<MetricCollectionProperty.Value> {
  private MetricCollectionProperty(final Value value) {
    super(Key.MetricCollection, value);
  }

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
