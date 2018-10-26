package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * ExecutionPlacement ExecutionProperty.
 */
public final class ResourcePriorityProperty extends VertexExecutionProperty<String> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private ResourcePriorityProperty(final String value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ResourcePriorityProperty of(final String value) {
    return new ResourcePriorityProperty(value);
  }

  // List of default pre-configured values.
  public static final String NONE = "None";
  public static final String TRANSIENT = "Transient";
  public static final String RESERVED = "Reserved";
  public static final String COMPUTE = "Compute";
}
