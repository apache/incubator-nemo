package org.apache.nemo.common.ir.executionproperty;

import java.io.Serializable;

/**
 * {@link ExecutionProperty} for {@link org.apache.nemo.common.ir.vertex.IRVertex}.
 * @param <T> Type of the value.
 */
public abstract class VertexExecutionProperty<T extends Serializable> extends ExecutionProperty<T> {
  /**
   * Default constructor.
   * @param value value of the VertexExecutionProperty.
   */
  public VertexExecutionProperty(final T value) {
    super(value);
  }
}
