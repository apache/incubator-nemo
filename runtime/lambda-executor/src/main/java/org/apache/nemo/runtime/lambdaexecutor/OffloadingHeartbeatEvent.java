package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;

import java.util.List;
import java.util.Map;

public final class OffloadingHeartbeatEvent {

  public final List<Pair<String, TaskMetrics.RetrievedMetrics>> taskMetrics;
  public final double cpuUse;
  public final String executorId;

  public OffloadingHeartbeatEvent(
    final String executorId,
    final List<Pair<String, TaskMetrics.RetrievedMetrics>> taskMetrics,
                                  final double cpuUse) {
    this.executorId = executorId;
    this.taskMetrics = taskMetrics;
    this.cpuUse = cpuUse;
  }
}
