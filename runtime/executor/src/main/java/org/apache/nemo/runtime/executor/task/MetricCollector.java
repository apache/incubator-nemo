package org.apache.nemo.runtime.executor.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class MetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollector.class.getName());

  long prevWindowTime = System.currentTimeMillis();
  final long windowsize = 2000;
  long adjustTime;

  private final Map<String, LatencyAndCnt> vertexAndLatencySumMap = new HashMap<>();

  public void setAdjustTime(final long adjTime) {
    adjustTime = adjTime;
  }

  public void processDone(final String vertexId, final long startTimestamp) {
    final long currTime = System.currentTimeMillis();
    final long latency = (currTime - startTimestamp) - adjustTime;
    final LatencyAndCnt latencyAndCnt = vertexAndLatencySumMap.getOrDefault(vertexId, new LatencyAndCnt());
    latencyAndCnt.latencySum += latency;
    latencyAndCnt.count += 1;
    vertexAndLatencySumMap.putIfAbsent(vertexId, latencyAndCnt);


    if (currTime - prevWindowTime >= windowsize) {
      for (final String vid : vertexAndLatencySumMap.keySet()) {
        // logging!
        final LatencyAndCnt lac = vertexAndLatencySumMap.get(vid);
        LOG.info("Avg Latency {}, from vertex {}, processCnt {}",
          lac.latencySum / lac.count, vid, lac.count);
        lac.latencySum= 0;
        lac.count = 0;
        prevWindowTime = currTime;
      }
    }
  }

  class LatencyAndCnt {
    public long latencySum = 0;
    public int count = 0;
  }
}
