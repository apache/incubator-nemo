package org.apache.nemo.common;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorMetrics {

  private final Map<String, AtomicLong> stageInputCounter;
  private final Map<String, AtomicLong> stageOutputCounter;

  @Inject
  private ExecutorMetrics() {
    this.stageInputCounter = new ConcurrentHashMap<>();
    this.stageOutputCounter = new ConcurrentHashMap<>();
  }

  public void increaseInputCounter(final String stageId) {
    if (stageInputCounter.containsKey(stageId)) {
      stageInputCounter.get(stageId).incrementAndGet();
    } else {
      stageInputCounter.putIfAbsent(stageId, new AtomicLong());
      stageInputCounter.get(stageId).incrementAndGet();
    }
  }

  public void increaseOutputCounter(final String stageId) {
    if (stageOutputCounter.containsKey(stageId)) {
      stageOutputCounter.get(stageId).incrementAndGet();
    } else {
      stageOutputCounter.putIfAbsent(stageId, new AtomicLong());
      stageOutputCounter.get(stageId).incrementAndGet();
    }
  }
}
