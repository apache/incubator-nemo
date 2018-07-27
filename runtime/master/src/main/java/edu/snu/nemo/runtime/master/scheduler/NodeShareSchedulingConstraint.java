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
  private final List<String> fast;
  private final List<String> slow;

  @Inject
  private NodeShareSchedulingConstraint() {
    this.fast = new ArrayList<>();
    fast.add("skew-w2");
    fast.add("skew-w3");
    fast.add("skew-w4");
    fast.add("skew-w5");
    fast.add("skew-w6");
    this.slow = new ArrayList<>();
    slow.add("skew-w7");
    slow.add("skew-w8");
    slow.add("skew-w9");
    slow.add("skew-w10");
    slow.add("skew-w11");
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

  private List<String> getNodeName(final Map<String, Integer> propertyValue,
                             final ExecutorRepresenter executor,
                             final Task task) {
    final List<String> nodeNames = new ArrayList<>(propertyValue.keySet());
    Collections.sort(nodeNames, Comparator.naturalOrder());
    int index = RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId());
    for (final String nodeName : nodeNames) {
      if (index >= propertyValue.get(nodeName)) {
        index -= propertyValue.get(nodeName);
      } else {
        if (fast.contains(nodeName)) {
          return fast;
        } else {
          return slow;
        }
        /*
        if (hasSkewedData(task)) {
          List<String> candidateNodes = nodeNames.subList(nodeNames.indexOf(nodeName), nodeNames.size());
          return nodeNames.subList(nodeNames.indexOf(nodeName), nodeNames.size());
        } else {
          //LOG.info("Non-skewed {} can be assigned to {}", task.getTaskId(), nodeName);
          final List<String> candidateNode = new ArrayList<>();
          candidateNode.add(nodeName);
          return candidateNode;
        }*/
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
      List<String> candidateNodes = getNodeName(propertyValue, executor, task);
      return candidateNodes.contains(executor.getNodeName());
    } catch (final IllegalStateException e) {
      throw new RuntimeException(String.format("Cannot schedule %s", task.getTaskId(), e));
    }
  }
}
