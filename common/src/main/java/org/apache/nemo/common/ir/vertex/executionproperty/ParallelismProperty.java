package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * Parallelism ExecutionProperty.
 */
public final class ParallelismProperty extends VertexExecutionProperty<Integer> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private ParallelismProperty(final Integer value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ParallelismProperty of(final Integer value) {
    return new ParallelismProperty(value);
  }
}
