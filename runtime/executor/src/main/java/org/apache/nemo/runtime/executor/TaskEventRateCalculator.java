package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.TaskExecutor;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class TaskEventRateCalculator {

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  @Inject
  private TaskEventRateCalculator(final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
  }

  public Pair<Integer, Integer> calculateProcessedEvent() {
    int sum = 0;
    int sum2 = 0;
    for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
      final AtomicInteger count = taskExecutor.getProcessedCnt();
      final AtomicInteger count2 = taskExecutor.getOffloadedCnt();
      final int cnt = count.get();
      final int cnt2 = count2.get();
      sum += cnt;
      sum2 += cnt2;
      count.getAndAdd(-cnt);
      count2.getAndAdd(-cnt2);
    }
    return Pair.of(sum, sum2);
  }
}
