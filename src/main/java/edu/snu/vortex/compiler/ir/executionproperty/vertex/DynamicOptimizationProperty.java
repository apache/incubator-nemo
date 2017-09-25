package edu.snu.vortex.compiler.ir.executionproperty.vertex;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.dynamic_optimization.DynamicOptimizationPass;

/**
 * DynamicOptimizationType ExecutionProperty.
 */
public final class DynamicOptimizationProperty extends ExecutionProperty<Class<? extends DynamicOptimizationPass>> {
  private DynamicOptimizationProperty(final Class<? extends DynamicOptimizationPass> value) {
    super(Key.DynamicOptimizationType, value);
  }

  public static DynamicOptimizationProperty of(final Class<? extends DynamicOptimizationPass> value) {
    return new DynamicOptimizationProperty(value);
  }
}
