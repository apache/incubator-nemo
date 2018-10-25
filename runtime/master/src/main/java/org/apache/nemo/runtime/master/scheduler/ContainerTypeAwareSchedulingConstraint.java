/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
