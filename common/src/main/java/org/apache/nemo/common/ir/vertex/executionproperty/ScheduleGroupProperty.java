package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * ScheduleGroup ExecutionProperty.
 */
public final class ScheduleGroupProperty extends VertexExecutionProperty<Integer> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private ScheduleGroupProperty(final Integer value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ScheduleGroupProperty of(final Integer value) {
    return new ScheduleGroupProperty(value);
  }
}
