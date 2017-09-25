package edu.snu.vortex.compiler.ir.executionproperty.edge;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.dynamic_optimization.DynamicOptimizationPass;

/**
 * MetricCollection ExecutionProperty.
 */
public final class MetricCollectionProperty extends ExecutionProperty<Class<? extends DynamicOptimizationPass>> {
  private MetricCollectionProperty(final Class<? extends DynamicOptimizationPass> value) {
    super(Key.MetricCollection, value);
  }

  public static MetricCollectionProperty of(final Class<? extends DynamicOptimizationPass> value) {
    return new MetricCollectionProperty(value);
  }
}
