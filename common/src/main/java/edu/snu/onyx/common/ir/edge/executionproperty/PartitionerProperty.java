package edu.snu.onyx.common.ir.edge.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * Partitioner ExecutionProperty.
 */
public final class PartitionerProperty extends ExecutionProperty<PartitionerProperty.Value> {
  private PartitionerProperty(final Value value) {
    super(Key.Partitioner, value);
  }

  public static PartitionerProperty of(final Value value) {
    return new PartitionerProperty(value);
  }

  /**
   * Possible values of Partitioner ExecutionProperty.
   */
  public enum Value {
    DataSkewHashPartitioner,
    HashPartitioner,
    IntactPartitioner
  }
}
