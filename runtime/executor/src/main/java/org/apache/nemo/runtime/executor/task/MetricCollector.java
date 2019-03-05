package org.apache.nemo.runtime.executor.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollector.class.getName());

  long prevWindowTime = System.currentTimeMillis();
  final long windowsize = 2000;
  long latencySum = 0;
  int processCnt = 0;

  long adjustTime;

  public void setAdjustTime(final long adjTime) {
    adjustTime = adjTime;
  }

  public void processDone(final String vertexId, final long startTimestamp) {
    final long currTime = System.currentTimeMillis();
    final long latency = (currTime - startTimestamp) - adjustTime;
    latencySum += latency;
    processCnt += 1;

    if (currTime - prevWindowTime >= windowsize) {
      // logging!
      LOG.info("Avg Latency: {} (in window {}), processCnt: {}",
        latencySum / processCnt, currTime - prevWindowTime, processCnt);
        latencySum = 0;
        processCnt = 0;
        prevWindowTime = currTime;
    }
  }
}
