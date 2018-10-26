package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * This property decides whether or not to schedule this vertex only on executors where
 * source (including intermediate) data reside.
 */
public final class ResourceLocalityProperty extends VertexExecutionProperty<Boolean> {
  private static final ResourceLocalityProperty SOURCE_TRUE = new ResourceLocalityProperty(true);
  private static final ResourceLocalityProperty SOURCE_FALSE = new ResourceLocalityProperty(false);

  /**
   * Default constructor.
   *
   * @param value value of the ExecutionProperty
   */
  private ResourceLocalityProperty(final boolean value) {
    super(value);
  }

  /**
   * Static method getting execution property.
   *
   * @param value value of the new execution property
   * @return the execution property
   */
  public static ResourceLocalityProperty of(final boolean value) {
    return value ? SOURCE_TRUE : SOURCE_FALSE;
  }
}
