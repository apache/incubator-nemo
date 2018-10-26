package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Invariant data ExecutionProperty. Use to indicate same data edge when unrolling loop vertex.
 * See DuplicateEdgeGroupPropertyValue
 */
public final class DuplicateEdgeGroupProperty extends EdgeExecutionProperty<DuplicateEdgeGroupPropertyValue> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private DuplicateEdgeGroupProperty(final DuplicateEdgeGroupPropertyValue value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DuplicateEdgeGroupProperty of(final DuplicateEdgeGroupPropertyValue value) {
    return new DuplicateEdgeGroupProperty(value);
  }
}
