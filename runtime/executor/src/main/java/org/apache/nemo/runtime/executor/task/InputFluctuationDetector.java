package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class InputFluctuationDetector {
  private static final Logger LOG = LoggerFactory.getLogger(InputFluctuationDetector.class.getName());

  // key: timestamp, value: processed event
  private final List<Pair<Long, Long>> processedEvents;

  // timescale: sec
  public InputFluctuationDetector() {
    this.processedEvents = new LinkedList<>();
  }

  public synchronized void collect(final long timestamp, final long currProcessedEvent) {
    processedEvents.add(Pair.of(timestamp, currProcessedEvent));
  }

  // 어느 시점을 기준으로 fluctuation 하였는가?
  public synchronized boolean isInputFluctuation(final long baseTime) {
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

    if (avgEventBeforeBaseTime * 2 < avgEventAfterBaseTime) {
      return true;
    } else {
      return false;
    }
  }

  public void clear() {
    processedEvents.clear();
  }
}
