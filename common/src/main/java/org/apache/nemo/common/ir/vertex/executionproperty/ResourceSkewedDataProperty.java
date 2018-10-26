package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * This property decides whether or not to handle skew when scheduling this vertex.
 */
public final class ResourceSkewedDataProperty extends VertexExecutionProperty<Boolean> {
  private static final ResourceSkewedDataProperty HANDLE_SKEW = new ResourceSkewedDataProperty(true);
  private static final ResourceSkewedDataProperty DONT_HANDLE_SKEW = new ResourceSkewedDataProperty(false);

  /**
   * Default constructor.
   *
   * @param value value of the ExecutionProperty
   */
  private ResourceSkewedDataProperty(final boolean value) {
    super(value);
  }

  /**
   * Static method getting execution property.
   *
   * @param value value of the new execution property
   * @return the execution property
   */
  public static ResourceSkewedDataProperty of(final boolean value) {
    return value ? HANDLE_SKEW : DONT_HANDLE_SKEW;
  }
}
