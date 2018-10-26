package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;

/**
 * This policy find executors which has corresponding container type.
 */
@AssociatedProperty(ResourcePriorityProperty.class)
public final class ContainerTypeAwareSchedulingConstraint implements SchedulingConstraint {

  @Inject
  private ContainerTypeAwareSchedulingConstraint() {
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final String executorPlacementPropertyValue = task.getPropertyValue(ResourcePriorityProperty.class)
        .orElse(ResourcePriorityProperty.NONE);
    return executorPlacementPropertyValue.equals(ResourcePriorityProperty.NONE) ? true
        : executor.getContainerType().equals(executorPlacementPropertyValue);
  }
}
