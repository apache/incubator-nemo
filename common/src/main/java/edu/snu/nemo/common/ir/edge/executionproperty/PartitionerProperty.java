package edu.snu.nemo.common.ir.edge.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

/**
 * Partitioner ExecutionProperty.
 */
public final class PartitionerProperty extends ExecutionProperty<PartitionerProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private PartitionerProperty(final Value value) {
    super(Key.Partitioner, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
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
