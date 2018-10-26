package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;

/**
 * This policy finds executor that has free slot for a Task.
 */
@AssociatedProperty(ResourceSlotProperty.class)
public final class FreeSlotSchedulingConstraint implements SchedulingConstraint {

  @Inject
  private FreeSlotSchedulingConstraint() {
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    if (!task.getPropertyValue(ResourceSlotProperty.class).orElse(false)) {
      return true;
    }

    return executor.getNumOfComplyingRunningTasks() < executor.getExecutorCapacity();
  }
}
