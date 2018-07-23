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
import edu.snu.nemo.common.ir.vertex.executionproperty.NodeNamesProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.*;

/**
 * This constraint is to follow {@link NodeNamesProperty}.
 */
@AssociatedProperty(NodeNamesProperty.class)
public final class NodeShareSchedulingConstraint implements SchedulingConstraint {

  @Inject
  private NodeShareSchedulingConstraint() {
  }

  private String getNodeName(final Map<String, Integer> propertyValue, final int taskIndex) {
    final List<String> nodeNames = new ArrayList<>(propertyValue.keySet());
    Collections.sort(nodeNames, Comparator.naturalOrder());
    int index = taskIndex;
    for (final String nodeName : nodeNames) {
      if (index >= propertyValue.get(nodeName)) {
        index -= propertyValue.get(nodeName);
      } else {
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
          getNodeName(propertyValue, RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId())));
    } catch (final IllegalStateException e) {
      throw new RuntimeException(String.format("Cannot schedule %s", task.getTaskId(), e));
    }
  }
}
