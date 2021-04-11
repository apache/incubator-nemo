package org.apache.nemo.runtime.executor.common.tasks;

import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.StageWatermarkTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class CRTaskInputWatermarkManager implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CRTaskInputWatermarkManager.class.getName());

  private long prevWatermark = 0L;
  private final Map<String, Long> dataFetcherWatermarkMap;
  private final Map<String, Long> transientDataFetcherWatermarkMap;

  private final Map<String, StageWatermarkTracker> dataFetcherWatermarkTracker;
  private final Map<String, StageWatermarkTracker> transientDataFetcherWatermarkTracker;
  private final Transform.ConditionalRouting conditionalRouting;
  private boolean transientPathSentAllData = true;

  public CRTaskInputWatermarkManager(final Transform.ConditionalRouting conditionalRouting) {
    this.dataFetcherWatermarkMap = new HashMap<>();
    this.transientDataFetcherWatermarkMap = new HashMap<>();
    this.dataFetcherWatermarkTracker = new HashMap<>();
    this.transientDataFetcherWatermarkTracker = new HashMap<>();
    this.conditionalRouting = conditionalRouting;
  }

  public void addDataFetcher(RuntimeEdge edge, int parallelism) {
    LOG.info("Add data fetcher for datafetcher {}, parallelism: {}", edge.getId(), parallelism);

    if (edge.isTransientPath()) {
      transientDataFetcherWatermarkMap.put(edge.getId(), 0L);
      transientDataFetcherWatermarkTracker.put(edge.getId(), new StageWatermarkTracker(parallelism));
    } else {
      dataFetcherWatermarkMap.put(edge.getId(), 0L);
      dataFetcherWatermarkTracker.put(edge.getId(), new StageWatermarkTracker(parallelism));
    }
  }

  public void finishTransientPath(final String edgeId,
                                  final int taskIndex) {
    // TODO: finish transient path
  }

  public Optional<Watermark> updateWatermark(final String edgeId,
                                             final int taskIndex, final long watermark) {
    final StageWatermarkTracker stageWatermarkTracker = dataFetcherWatermarkTracker.get(edgeId);
    final Optional<Long> val = stageWatermarkTracker.trackAndEmitWatermarks(taskIndex, watermark);

    if (val.isPresent()) {
      // update output watermark!
      final long outputW = dataFetcherWatermarkMap.get(edgeId);
      if (outputW > val.get()) {
        throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
      }

      dataFetcherWatermarkMap.put(edgeId, val.get());
      final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());

      if (minWatermark > prevWatermark) {
        // watermark progress
        prevWatermark = minWatermark;
        return Optional.of(new Watermark(minWatermark));
      }
    }

    return Optional.empty();
  }

}
