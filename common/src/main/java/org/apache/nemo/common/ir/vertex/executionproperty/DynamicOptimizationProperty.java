package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * DynamicOptimizationType ExecutionProperty.
 */
public final class DynamicOptimizationProperty extends VertexExecutionProperty<DynamicOptimizationProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private DynamicOptimizationProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DynamicOptimizationProperty of(final Value value) {
    return new DynamicOptimizationProperty(value);
  }

  /**
   * Possible values of DynamicOptimization ExecutionProperty.
   */
  public enum Value {
    DataSkewRuntimePass
  }
}
