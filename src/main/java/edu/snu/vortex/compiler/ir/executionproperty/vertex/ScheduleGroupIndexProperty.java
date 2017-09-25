package edu.snu.vortex.compiler.ir.executionproperty.vertex;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;

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
