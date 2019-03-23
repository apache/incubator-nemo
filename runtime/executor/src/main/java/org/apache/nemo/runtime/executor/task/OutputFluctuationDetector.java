package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class OutputFluctuationDetector {
  private static final Logger LOG = LoggerFactory.getLogger(OutputFluctuationDetector.class.getName());

  // key: timestamp, value: processed event
  //private final List<Pair<Long, Long>> processedEvents;
  private final Collection<Pair<OperatorMetricCollector, OutputCollector>> metricCollectors;
  private int length = 20;

  // timescale: sec
  public OutputFluctuationDetector(
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> metricCollectors) {
    //this.processedEvents = new LinkedList<>();
    this.metricCollectors = metricCollectors.values();
  }

  public synchronized void collect(final OperatorMetricCollector oc,
                      final long timestamp,
                      final long currProcessedEvent) {
    final List<Pair<Long, Long>> processedEvents = oc.processedEvents;
    if (processedEvents.size() >= length) {
      processedEvents.remove(0);
    }
    processedEvents.add(Pair.of(timestamp, currProcessedEvent));
    //processedEvents.add(Pair.of(timestamp, currProcessedEvent));
  }

  public synchronized List<Pair<OperatorMetricCollector, OutputCollector>> retrieveBurstyOutputCollectors(final long baseTime) {
    final List<Pair<OperatorMetricCollector, OutputCollector>> burstyCollectors = new ArrayList<>(metricCollectors.size());
    for (final Pair<OperatorMetricCollector, OutputCollector> pair : metricCollectors) {
      if (isOutputFluctuation(baseTime, pair.left().processedEvents)) {
        burstyCollectors.add(pair);
      }
    }

    return burstyCollectors;
  }

  // 어느 시점 (baseTime) 을 기준으로 fluctuation 하였는가?
  private boolean isOutputFluctuation(final long baseTime,
                                      final List<Pair<Long, Long>> processedEvents) {
    final List<Long> beforeBaseTime = new ArrayList<>();
    final List<Long> afterBaseTime = new ArrayList<>();

    for (final Pair<Long, Long> pair : processedEvents) {
      if (pair.left() < baseTime) {
        beforeBaseTime.add(pair.right());
      } else {
        afterBaseTime.add(pair.right());
      }
    }

    final long avgEventBeforeBaseTime = beforeBaseTime.stream()
      .reduce(0L, (x, y) -> x + y) / (Math.max(1, beforeBaseTime.size()));

    final long avgEventAfterBaseTime = afterBaseTime.stream()
      .reduce(0L, (x, y) -> x + y) / (Math.max(1, afterBaseTime.size()));

    LOG.info("avgEventBeforeBaseTime: {} (size: {}), avgEventAfterBaseTime: {} (size: {}), baseTime: {}",
      avgEventBeforeBaseTime, beforeBaseTime.size(), avgEventAfterBaseTime, afterBaseTime.size(), baseTime);

    processedEvents.clear();

    if (avgEventBeforeBaseTime * 2 < avgEventAfterBaseTime) {
      return true;
    } else {
      return false;
    }
  }
}
