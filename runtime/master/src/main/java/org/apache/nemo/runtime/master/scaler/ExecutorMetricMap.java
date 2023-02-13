package org.apache.nemo.runtime.master.scaler;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ExecutorMetricMap {

  private final Map<String, Pair<Long, ExecutorMetricInfo>> map;

  @Inject
  private ExecutorMetricMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public synchronized ExecutorMetricInfo getAggregated() {
    final ExecutorMetricInfo result = map.values().stream()
      // .filter(info -> info.right().cpuUse > 0.05)
      .reduce((pair1, pair2) -> {
      final ExecutorMetricInfo info1 = pair1.right();
      final ExecutorMetricInfo info2 = pair2.right();

      if (System.currentTimeMillis() - pair1.left() > 3000 &&
        System.currentTimeMillis() - pair2.left() > 3000) {

        return Pair.of(System.currentTimeMillis(),
          new ExecutorMetricInfo(0,0,0,0, 0));

      } else if (System.currentTimeMillis() - pair2.left() > 3000) {
        // stale data
        return pair1;
      } else if (System.currentTimeMillis() - pair1.left() > 3000){
        return pair2;
      } else {
        final long p = info1.processEvent + info2.processEvent;
        final long r = info1.receiveEvent + info2.receiveEvent;
        return Pair.of(Math.max(pair1.left(), pair2.left()),
          new ExecutorMetricInfo(r, p,
            info1.cpuUse + info2.cpuUse,
            Math.max(info1.maxCpuUse, info2.maxCpuUse),
            info1.numExecutor + info2.numExecutor));
      }
    })
    .orElse(Pair.of(System.currentTimeMillis(),
      new ExecutorMetricInfo(0,0,0,0,0))).right();

    return result;
  }

  public synchronized void setInfo(final String executorId,
                                   final ExecutorMetricInfo info) {
    map.put(executorId, Pair.of(System.currentTimeMillis(), info));
  }



  final class AggInfo {
    public final double cpuUse;
    public final int numExecutor;
    public final long processEvent;
    public final long receiveEvent;
    public final double maxCpuUse;

    public AggInfo(final double cpuUse,
                   final int numExecutor,
                   final long processEvent,
                   final long receiveEvent,
                   final double maxCpuUse) {
      this.cpuUse = cpuUse;
      this.numExecutor = numExecutor;
      this.processEvent = processEvent;
      this.receiveEvent = receiveEvent;
      this.maxCpuUse = maxCpuUse;
    }

    public AggInfo concat(final AggInfo info) {
      return new AggInfo(this.cpuUse + info.cpuUse,
        this.numExecutor + info.numExecutor,
        this.processEvent + info.processEvent,
        this.receiveEvent + info.receiveEvent,
        Math.max(this.maxCpuUse, info.maxCpuUse));
    }
  }
}