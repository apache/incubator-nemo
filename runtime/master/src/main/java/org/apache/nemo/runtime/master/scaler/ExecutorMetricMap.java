package org.apache.nemo.runtime.master.scaler;

import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ExecutorMetricMap {

  private final Map<String, ExecutorMetricInfo> map;

  @Inject
  private ExecutorMetricMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public synchronized ExecutorMetricInfo getAggregated() {
    final ExecutorMetricInfo result = map.values().stream().reduce((info1, info2) -> {
      final long p = info1.processEvent + info2.processEvent;
      final long r = info1.receiveEvent + info2.receiveEvent;
      return new ExecutorMetricInfo(p, r,
        info1.cpuUse + info2.cpuUse,
        info1.numExecutor + info2.numExecutor);
    })
    .orElse(new ExecutorMetricInfo(0,0,0,0));

    map.clear();

    return result;
  }

  public synchronized void setInfo(final String executorId,
                                   final ExecutorMetricInfo info) {
    map.put(executorId, info);
  }
}
