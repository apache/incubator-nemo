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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorSlotComplianceProperty;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;

/**
 * This policy finds executor that has free slot for a Task.
 */
@AssociatedProperty(ExecutorSlotComplianceProperty.class)
public final class FreeSlotSchedulingConstraint implements SchedulingConstraint {

  @Inject
  private FreeSlotSchedulingConstraint() {
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    if (!task.getPropertyValue(ExecutorSlotComplianceProperty.class).orElse(false)) {
      return true;
    }

    return executor.getNumOfComplyingRunningTasks() < executor.getExecutorCapacity();
  }
}
