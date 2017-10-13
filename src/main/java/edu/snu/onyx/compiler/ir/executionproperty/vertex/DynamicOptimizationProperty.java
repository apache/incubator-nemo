package edu.snu.onyx.compiler.ir.executionproperty.vertex;

import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.optimizer.pass.runtime.RuntimePass;

/**
 * DynamicOptimizationType ExecutionProperty.
 */
public final class DynamicOptimizationProperty extends ExecutionProperty<Class<? extends RuntimePass>> {
  private DynamicOptimizationProperty(final Class<? extends RuntimePass> value) {
    super(Key.DynamicOptimizationType, value);
  }

  public static DynamicOptimizationProperty of(final Class<? extends RuntimePass> value) {
    return new DynamicOptimizationProperty(value);
  }
}
