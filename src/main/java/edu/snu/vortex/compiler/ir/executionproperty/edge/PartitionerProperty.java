package edu.snu.vortex.compiler.ir.executionproperty.edge;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Partitioner;

/**
 * Partitioner ExecutionProperty.
 */
public final class PartitionerProperty extends ExecutionProperty<Class<? extends Partitioner>> {
  private PartitionerProperty(final Class<? extends Partitioner> value) {
    super(Key.Partitioner, value);
  }

  public static PartitionerProperty of(final Class<? extends Partitioner> value) {
    return new PartitionerProperty(value);
  }
}
