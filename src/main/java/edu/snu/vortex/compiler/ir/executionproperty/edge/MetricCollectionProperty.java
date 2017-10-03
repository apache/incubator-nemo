package edu.snu.vortex.compiler.ir.executionproperty.edge;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.runtime.RuntimePass;

/**
 * MetricCollection ExecutionProperty.
 */
public final class MetricCollectionProperty extends ExecutionProperty<Class<? extends RuntimePass>> {
  private MetricCollectionProperty(final Class<? extends RuntimePass> value) {
    super(Key.MetricCollection, value);
  }

  public static MetricCollectionProperty of(final Class<? extends RuntimePass> value) {
    return new MetricCollectionProperty(value);
  }
}
