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

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourceSkewedDataProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.data.HashRange;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * This policy aims to distribute partitions with skewed keys to different executors.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(ResourceSkewedDataProperty.class)
public final class SkewnessAwareSchedulingConstraint implements SchedulingConstraint {

  @VisibleForTesting
  @Inject
  public SkewnessAwareSchedulingConstraint() {
  }

  public boolean hasSkewedData(final Task task) {
    final int taskIdx = RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId());
    for (StageEdge inEdge : task.getTaskIncomingEdges()) {
      final KeyRange hashRange = inEdge.getTaskIdxToKeyRange().get(taskIdx);
      if (((HashRange) hashRange).isSkewed()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    // Check if this executor had already received heavy tasks
    for (Task runningTask : executor.getRunningTasks()) {
      if (hasSkewedData(runningTask) && hasSkewedData(task)) {
        return false;
      }
    }
    return true;
  }
}
