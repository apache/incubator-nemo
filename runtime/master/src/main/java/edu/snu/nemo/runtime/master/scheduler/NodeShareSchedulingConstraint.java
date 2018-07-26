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

import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.NodeNamesProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

/**
 * This constraint is to follow {@link NodeNamesProperty}.
 */
@AssociatedProperty(NodeNamesProperty.class)
public final class NodeShareSchedulingConstraint implements SchedulingConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(NodeShareSchedulingConstraint.class.getName());

  @Inject
  private NodeShareSchedulingConstraint() {
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

  private String getNodeName(final Map<String, Integer> propertyValue,
                             final ExecutorRepresenter executor,
                             final Task task) {
    final List<String> nodeNames = new ArrayList<>(propertyValue.keySet());
    Collections.sort(nodeNames, Comparator.naturalOrder());
    int index = RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId());
    for (final String nodeName : nodeNames) {
      if (index >= propertyValue.get(nodeName)) {
        index -= propertyValue.get(nodeName);
      } else {
        if (hasSkewedData(task)) {
          LOG.info("Skewed {} can be assigned to {}({})",
              task.getTaskId(), executor.getExecutorId(), nodeName);
        }
        return nodeName;
      }
    }
    throw new IllegalStateException("Detected excessive parallelism which NodeNamesProperty does not cover");
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final Map<String, Integer> propertyValue = task.getPropertyValue(NodeNamesProperty.class)
            .orElseThrow(() -> new RuntimeException("NodeNamesProperty expected"));
    if (propertyValue.isEmpty()) {
      return true;
    }
    try {
      return executor.getNodeName().equals(
          getNodeName(propertyValue, executor, task));
    } catch (final IllegalStateException e) {
      throw new RuntimeException(String.format("Cannot schedule %s", task.getTaskId(), e));
    }
  }
}
