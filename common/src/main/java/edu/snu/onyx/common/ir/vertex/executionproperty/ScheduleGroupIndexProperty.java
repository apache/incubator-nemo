package edu.snu.onyx.common.ir.vertex.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * ScheduleGroupIndex ExecutionProperty.
 */
public final class ScheduleGroupIndexProperty extends ExecutionProperty<Integer> {
  private ScheduleGroupIndexProperty(final Integer value) {
    super(Key.ScheduleGroupIndex, value);
  }

  public static ScheduleGroupIndexProperty of(final Integer value) {
    return new ScheduleGroupIndexProperty(value);
  }
}
