package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * This property decides whether or not to comply to slot restrictions when scheduling this vertex.
 */
public final class ResourceSlotProperty extends VertexExecutionProperty<Boolean> {
  private static final ResourceSlotProperty COMPLIANCE_TRUE = new ResourceSlotProperty(true);
  private static final ResourceSlotProperty COMPLIANCE_FALSE
      = new ResourceSlotProperty(false);

  /**
   * Default constructor.
   *
   * @param value value of the ExecutionProperty
   */
  private ResourceSlotProperty(final boolean value) {
    super(value);
  }

  /**
   * Static method getting execution property.
   *
   * @param value value of the new execution property
   * @return the execution property
   */
  public static ResourceSlotProperty of(final boolean value) {
    return value ? COMPLIANCE_TRUE : COMPLIANCE_FALSE;
  }
}
