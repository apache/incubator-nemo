package org.apache.nemo.common.ir.executionproperty;

import java.io.Serializable;

/**
 * An abstract class for each execution factors.
 * @param <T> Type of the value.
 */
public abstract class ExecutionProperty<T extends Serializable> implements Serializable {
  private T value;

  /**
   * Default constructor.
   * @param value value of the ExecutionProperty.
   */
  public ExecutionProperty(final T value) {
    this.value = value;
  }

  /**
   * @return the value of the execution property.
   */
  public final T getValue() {
    return this.value;
  }

  @Override
  public final boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExecutionProperty<?> that = (ExecutionProperty<?>) o;
    return value != null ? value.equals(that.value) : that.value == null;
  }

  @Override
  public final int hashCode() {
    return value != null ? value.hashCode() : 0;
  }
}
