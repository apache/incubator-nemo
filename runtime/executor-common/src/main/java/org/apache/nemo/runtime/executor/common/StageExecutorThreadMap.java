package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.ExecutorThread;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class StageExecutorThreadMap {
  private final Map<String, Pair<AtomicInteger, List<ExecutorThread>>> stageExecutorThreadMap;

  @Inject
  private StageExecutorThreadMap() {
    this.stageExecutorThreadMap = new ConcurrentHashMap<>();
  }

  public Map<String, Pair<AtomicInteger, List<ExecutorThread>>> getStageExecutorThreadMap() {
    return stageExecutorThreadMap;
  }
}
