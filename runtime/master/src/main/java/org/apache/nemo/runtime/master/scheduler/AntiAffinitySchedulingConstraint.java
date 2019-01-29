/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.scheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceAntiAffinityProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Optional;

/**
 * Check if one of the tasks running on the executor, and the task to schedule are both in the anti-affinity group.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(ResourceAntiAffinityProperty.class)
public final class AntiAffinitySchedulingConstraint implements SchedulingConstraint {
  @VisibleForTesting
  @Inject
  public AntiAffinitySchedulingConstraint() {
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    for (final Task runningTask : executor.getRunningTasks()) {
      if (isInAntiAffinityGroup(runningTask) && isInAntiAffinityGroup(task)) {
        return false;
      }
    }
    return true;
  }

  private boolean isInAntiAffinityGroup(final Task task) {
    final int taskIdx = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());
    final Optional<HashSet<Integer>> indices =
      task.getExecutionProperties().get(ResourceAntiAffinityProperty.class);
    return indices.isPresent() && indices.get().contains(taskIdx);
  }
}
