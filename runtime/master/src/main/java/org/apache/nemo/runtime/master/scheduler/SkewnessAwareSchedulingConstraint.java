package org.apache.nemo.runtime.master.scheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSkewedDataProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Map;

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
    final int taskIdx = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());
    for (StageEdge inEdge : task.getTaskIncomingEdges()) {
      if (CommunicationPatternProperty.Value.Shuffle
      .equals(inEdge.getDataCommunicationPattern())) {
        final Map<Integer, KeyRange> taskIdxToKeyRange =
            inEdge.getPropertyValue(DataSkewMetricProperty.class).get().getMetric();
        final KeyRange hashRange = taskIdxToKeyRange.get(taskIdx);
        if (((HashRange) hashRange).isSkewed()) {
          return true;
        }
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
