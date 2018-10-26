package org.apache.nemo.common.ir.executionproperty;

import java.io.Serializable;

/**
 * {@link ExecutionProperty} for {@link org.apache.nemo.common.ir.edge.IREdge}.
 * @param <T> Type of the value.
 */
public abstract class EdgeExecutionProperty<T extends Serializable> extends ExecutionProperty<T> {
  /**
   * Default constructor.
   * @param value value of the EdgeExecutionProperty.
   */
  public EdgeExecutionProperty(final T value) {
    super(value);
  }
}
