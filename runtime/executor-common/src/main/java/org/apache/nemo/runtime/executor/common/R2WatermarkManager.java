package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class R2WatermarkManager implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(R2WatermarkManager.class.getName());

  private long prevWatermark = 0L;
  private final Map<Pair<String, String>, Long> dataFetcherWatermarkMap;
  private final Map<Pair<String, String>, R2PairEdgeWatermarkTracker> dataFetcherWatermarkTracker;

  private final Map<String, Pair<String, String>> pairEdgeMap;
  private final String taskId;

  public R2WatermarkManager(final String taskId) {
    this.dataFetcherWatermarkMap = new HashMap<>();
    this.dataFetcherWatermarkTracker = new HashMap<>();
    this.pairEdgeMap = new HashMap<>();
    this.taskId = taskId;
  }

  public void addDataFetcher(String vmEdgeId, String lambdaEdgeId, int parallelism) {
    LOG.info("Add data fetcher for datafetcher {}/{}, parallelism: {}", vmEdgeId, lambdaEdgeId, parallelism);
    final R2PairEdgeWatermarkTracker stageWatermarkTracker =
      new R2PairEdgeWatermarkTracker(vmEdgeId, lambdaEdgeId, taskId, parallelism);
    final Pair<String, String> key = Pair.of(vmEdgeId, lambdaEdgeId);
    dataFetcherWatermarkMap.put(key, 0L);
    dataFetcherWatermarkTracker.put(key, stageWatermarkTracker);

    pairEdgeMap.put(vmEdgeId, key);
    pairEdgeMap.put(lambdaEdgeId, key);
  }

  public void stopAndToggleIndex(final int taskIndex,
                                 final String edgeId) {
    final Pair<String, String> key = pairEdgeMap.get(edgeId);
    dataFetcherWatermarkTracker.get(key).stopAndToggleIndex(taskIndex, edgeId);
  }

  public void startIndex(final int taskIndex,
                         final String edgeId) {
    final Pair<String, String> key = pairEdgeMap.get(edgeId);
    dataFetcherWatermarkTracker.get(key).startIndex(taskIndex, edgeId);
  }

  public Optional<Watermark> updateWatermark(final String edgeId,
                                             final int taskIndex, final long watermark) {
    try {
      final Pair<String, String> key = pairEdgeMap.get(edgeId);
      final WatermarkTracker stageWatermarkTracker = dataFetcherWatermarkTracker.get(key);
      final Optional<Long> val = stageWatermarkTracker.trackAndEmitWatermarks(edgeId, taskIndex, watermark);

      if (val.isPresent()) {
        // update output watermark!
        final long outputW = dataFetcherWatermarkMap.get(key);
        if (outputW > val.get()) {
          throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
        }

        dataFetcherWatermarkMap.put(key, val.get());
        final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());

        if (minWatermark > prevWatermark + Util.WATERMARK_PROGRESS) {
          // watermark progress
          prevWatermark = minWatermark;
          return Optional.of(new Watermark(minWatermark));
        }
      }

      return Optional.empty();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Watermark update failed ... edgeId " + edgeId + ", taskIndex " + taskIndex
        + " watermark " + watermark + ", " + " pairEdgeMap: " + pairEdgeMap +
        ", dataFetcherWatermarkTracker key: " + dataFetcherWatermarkTracker.keySet());
    }
  }

}
