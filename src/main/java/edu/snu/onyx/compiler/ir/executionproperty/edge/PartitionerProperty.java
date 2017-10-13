package edu.snu.onyx.compiler.ir.executionproperty.edge;

import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.executor.datatransfer.partitioning.Partitioner;

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
