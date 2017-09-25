package edu.snu.vortex.compiler.ir.executionproperty.edge;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Partitioning;

/**
 * WriteOptimization ExecutionProperty.
 */
public final class PartitioningProperty extends ExecutionProperty<Class<? extends Partitioning>> {
  private PartitioningProperty(final Class<? extends Partitioning> value) {
    super(Key.Partitioning, value);
  }

  public static PartitioningProperty of(final Class<? extends Partitioning> value) {
    return new PartitioningProperty(value);
  }
}
