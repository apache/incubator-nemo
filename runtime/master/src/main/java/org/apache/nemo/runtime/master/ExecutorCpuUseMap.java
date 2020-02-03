package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ExecutorCpuUseMap {

  private final Map<String, Pair<Double, Double>> executorCpuUseMap;

  @Inject
  private ExecutorCpuUseMap() {
    this.executorCpuUseMap = new ConcurrentHashMap<>();
  }

  public Map<String, Pair<Double, Double>> getExecutorCpuUseMap() {
    return executorCpuUseMap;
  }
}
