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

import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSiteProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.*;

/**
 * This constraint is to follow {@link ResourceSiteProperty}.
 */
@AssociatedProperty(ResourceSiteProperty.class)
public final class NodeShareSchedulingConstraint implements SchedulingConstraint {

  @Inject
  private NodeShareSchedulingConstraint() {
  }

  private Optional<String> getNodeName(final Map<String, Integer> propertyValue, final int taskIndex) {
    final List<String> nodeNames = new ArrayList<>(propertyValue.keySet());
    Collections.sort(nodeNames, Comparator.naturalOrder());
    int index = taskIndex;
    for (final String nodeName : nodeNames) {
      if (index >= propertyValue.get(nodeName)) {
        index -= propertyValue.get(nodeName);
      } else {
        return Optional.of(nodeName);
      }
    }

    return Optional.empty();
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final Map<String, Integer> propertyValue = task.getPropertyValue(ResourceSiteProperty.class)
      .orElseThrow(() -> new RuntimeException("ResourceSiteProperty expected"));
    if (propertyValue.isEmpty()) {
      return true;
    }

    final String executorNodeName = executor.getNodeName();
    final Optional<String> taskNodeName =
      getNodeName(propertyValue, RuntimeIdManager.getIndexFromTaskId(task.getTaskId()));

    if (!taskNodeName.isPresent()) {
      throw new IllegalStateException(
        String.format("Detected excessive parallelism which ResourceSiteProperty does not cover: %s",
          task.getTaskId()));
    } else {
      return executorNodeName.equals(taskNodeName.get());
    }
  }
}
