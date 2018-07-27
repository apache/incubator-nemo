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
import edu.snu.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.SkewnessAwareSchedulingProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This policy aims to distribute partitions with skewed keys to different executors.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(SkewnessAwareSchedulingProperty.class)
public final class SkewnessAwareSchedulingConstraint implements SchedulingConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(SkewnessAwareSchedulingConstraint.class.getName());

  @VisibleForTesting
  @Inject
  public SkewnessAwareSchedulingConstraint() {
  }

  public boolean hasSkewedData(final Task task) {
    final int taskIdx = RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId());
    for (StageEdge inEdge : task.getTaskIncomingEdges()) {
      final Map<Integer, KeyRange> taskIdxToKeyRange =
          inEdge.getPropertyValue(DataSkewMetricProperty.class).get().getMetric();
      final KeyRange hashRange = taskIdxToKeyRange.get(taskIdx);
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
    /*
    if (!hasSkewedData(task)) {
      LOG.info("Non-Skewed {} can be assigned to {}", task.getTaskId(), executor.getNodeName());
    }
    */
    return true;
  }
}
