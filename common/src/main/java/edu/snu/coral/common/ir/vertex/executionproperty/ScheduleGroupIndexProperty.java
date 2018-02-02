package edu.snu.coral.common.ir.vertex.executionproperty;

import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;

/**
 * ScheduleGroupIndex ExecutionProperty.
 */
public final class ScheduleGroupIndexProperty extends ExecutionProperty<Integer> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private ScheduleGroupIndexProperty(final Integer value) {
    super(Key.ScheduleGroupIndex, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ScheduleGroupIndexProperty of(final Integer value) {
    return new ScheduleGroupIndexProperty(value);
  }
}
