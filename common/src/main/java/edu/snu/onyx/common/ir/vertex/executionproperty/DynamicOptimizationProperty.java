package edu.snu.onyx.common.ir.vertex.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * DynamicOptimizationType ExecutionProperty.
 */
public final class DynamicOptimizationProperty extends ExecutionProperty<DynamicOptimizationProperty.Value> {
  private DynamicOptimizationProperty(final Value value) {
    super(Key.DynamicOptimizationType, value);
  }

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
