package edu.snu.coral.common.ir.edge.executionproperty;

import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;

/**
 * MetricCollection ExecutionProperty.
 */
public final class MetricCollectionProperty extends ExecutionProperty<MetricCollectionProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private MetricCollectionProperty(final Value value) {
    super(Key.MetricCollection, value);
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
