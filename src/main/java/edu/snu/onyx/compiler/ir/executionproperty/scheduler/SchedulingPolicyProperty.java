package edu.snu.onyx.compiler.ir.executionproperty.scheduler;

import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.master.scheduler.SchedulingPolicy;

/**
 * SchedulingPolicy ExecutionProperty.
 */
public final class SchedulingPolicyProperty extends ExecutionProperty<Class<? extends SchedulingPolicy>> {
  private SchedulingPolicyProperty(final Class<? extends SchedulingPolicy> value) {
    super(Key.SchedulingPolicy, value);
  }

  public static SchedulingPolicyProperty of(final Class<? extends SchedulingPolicy> value) {
    return new SchedulingPolicyProperty(value);
  }
}
