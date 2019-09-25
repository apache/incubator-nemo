package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.TaskMetrics;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SFTaskMetrics {


  public final Map<String, TaskMetrics.RetrievedMetrics> sfTaskMetrics;

  @Inject
  private SFTaskMetrics() {
    this.sfTaskMetrics = new ConcurrentHashMap<>();
  }
}
