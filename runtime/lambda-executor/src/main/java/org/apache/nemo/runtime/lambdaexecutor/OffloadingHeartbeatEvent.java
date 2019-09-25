package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;

import java.util.List;
import java.util.Map;

public final class OffloadingHeartbeatEvent {

  public final List<Pair<String, TaskMetrics.RetrievedMetrics>> taskMetrics;

  public OffloadingHeartbeatEvent(final List<Pair<String, TaskMetrics.RetrievedMetrics>> taskMetrics) {
    this.taskMetrics = taskMetrics;
  }
}
