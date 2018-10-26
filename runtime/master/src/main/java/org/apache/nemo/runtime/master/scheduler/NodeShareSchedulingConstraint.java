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
    throw new IllegalStateException("Detected excessive parallelism which ResourceSiteProperty does not cover");
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final Map<String, Integer> propertyValue = task.getPropertyValue(ResourceSiteProperty.class)
        .orElseThrow(() -> new RuntimeException("ResourceSiteProperty expected"));
    if (propertyValue.isEmpty()) {
      return true;
    }
    try {
      return executor.getNodeName().equals(
          getNodeName(propertyValue, RuntimeIdManager.getIndexFromTaskId(task.getTaskId())));
    } catch (final IllegalStateException e) {
      throw new RuntimeException(String.format("Cannot schedule %s", task.getTaskId(), e));
    }
  }
}
