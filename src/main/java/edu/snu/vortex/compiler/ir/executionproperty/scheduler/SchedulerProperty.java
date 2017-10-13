package edu.snu.vortex.compiler.ir.executionproperty.scheduler;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

/**
 * SchedulerType ExecutionProperty.
 */
public final class SchedulerProperty extends ExecutionProperty<Class<? extends Scheduler>> {
  private SchedulerProperty(final Class<? extends Scheduler> value) {
    super(Key.SchedulerType, value);
  }

  public static SchedulerProperty of(final Class<? extends Scheduler> value) {
    return new SchedulerProperty(value);
  }
}
