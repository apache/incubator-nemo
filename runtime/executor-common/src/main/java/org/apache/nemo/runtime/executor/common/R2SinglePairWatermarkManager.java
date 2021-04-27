package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class R2SinglePairWatermarkManager implements R2WatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(R2SinglePairWatermarkManager.class.getName());

  private long prevWatermark = 0L;
  private long dataFetcherWatermark;
  private R2PairEdgeWatermarkTracker dataFetcherWatermarkTracker;
  private final String taskId;
  private String vmEdgeId;
  private String lambdaEdgeId;

  public R2SinglePairWatermarkManager(final String taskId) {
    this.dataFetcherWatermark = 0L;
    this.taskId = taskId;
  }

  private R2SinglePairWatermarkManager(final long prevWatermark,
                                       final long dataFetcherWatermark,
                                       final R2PairEdgeWatermarkTracker dataFetcherWatermarkTracker,
                                       final String taskId,
                                       final String vmEdgeId,
                                       final String lambdaEdgeId) {
    this.prevWatermark = prevWatermark;
    this.dataFetcherWatermarkTracker = dataFetcherWatermarkTracker;
    this.dataFetcherWatermark = dataFetcherWatermark;
    this.taskId = taskId;
    this.vmEdgeId = vmEdgeId;
    this.lambdaEdgeId = lambdaEdgeId;
  }

  public static R2SinglePairWatermarkManager decode(final String taskId,
                                                    final DataInputStream dis) {
    try {
      final long prevWatermark = dis.readLong();
      final long dataFetcherWatermark = dis.readLong();
      final String vmEdgeId = dis.readUTF();
      final String lambdaEdgeId = dis.readUTF();

      final R2PairEdgeWatermarkTracker dataFetcherWatermarkTracker = R2PairEdgeWatermarkTracker
        .decode(vmEdgeId, lambdaEdgeId, taskId, dis);

      return new R2SinglePairWatermarkManager(prevWatermark,
        dataFetcherWatermark, dataFetcherWatermarkTracker, taskId,
        vmEdgeId, lambdaEdgeId);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void encode(final String taskId,
                     final DataOutputStream dos) {
    try {
      dos.writeLong(prevWatermark);
      dos.writeLong(dataFetcherWatermark);
      dos.writeUTF(vmEdgeId);
      dos.writeUTF(lambdaEdgeId);
      dataFetcherWatermarkTracker.encode(dos);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addDataFetcher(String vmEdgeId, String lambdaEdgeId, int parallelism) {
    if (dataFetcherWatermarkTracker != null) {
      LOG.info("Skip data fetcher for datafetcher {}/{}, parallelism: {}", vmEdgeId, lambdaEdgeId, parallelism);
    } else {
      LOG.info("Add data fetcher for datafetcher {}/{}, parallelism: {}", vmEdgeId, lambdaEdgeId, parallelism);
      final R2PairEdgeWatermarkTracker stageWatermarkTracker =
        new R2PairEdgeWatermarkTracker(vmEdgeId, lambdaEdgeId, taskId, parallelism);
      this.vmEdgeId = vmEdgeId;
      this.lambdaEdgeId = lambdaEdgeId;
      final Pair<String, String> key = Pair.of(vmEdgeId, lambdaEdgeId);
      dataFetcherWatermarkTracker = stageWatermarkTracker;
    }
  }

  @Override
  public boolean stopIndex(final int taskIndex,
                           final String edgeId) {
    return dataFetcherWatermarkTracker.stopInputIndex(taskIndex, edgeId);
  }

  @Override
  public void startIndex(final int taskIndex,
                         final String edgeId) {
    dataFetcherWatermarkTracker.startIndex(taskIndex, edgeId, prevWatermark);
  }

  @Override
  public void startAndStopPairIndex(int taskIndex, String edgeId) {
    dataFetcherWatermarkTracker.startIndex(taskIndex, edgeId, prevWatermark);

    if (edgeId.equals(lambdaEdgeId)) {
      dataFetcherWatermarkTracker.stopInputIndex(taskIndex, vmEdgeId);
    } else {
      dataFetcherWatermarkTracker.stopInputIndex(taskIndex, lambdaEdgeId);
    }
  }

  @Override
  public Optional<Watermark> updateWatermark(final String edgeId,
                                             final int taskIndex, final long watermark) {
    try {
      final WatermarkTracker stageWatermarkTracker = dataFetcherWatermarkTracker;
      final Optional<Long> val = stageWatermarkTracker.trackAndEmitWatermarks(taskId, edgeId, taskIndex, watermark);

      if (val.isPresent()) {
        // update output watermark!
        final long outputW = dataFetcherWatermark;
        if (outputW > val.get()) {
          throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " +
            new Instant(outputW) + ", "
            + new Instant(val.get()) + ", index: " + taskIndex + " watermark " +
            new Instant(watermark) + ", " + taskId);
        }

        dataFetcherWatermark = val.get();
        final long minWatermark = dataFetcherWatermark;

        if (minWatermark >= prevWatermark + Util.WATERMARK_PROGRESS) {
          // watermark progress
          prevWatermark = minWatermark;
          return Optional.of(new Watermark(minWatermark));
        }
      }

      return Optional.empty();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Watermark update failed ... edgeId " + edgeId + ", taskIndex " + taskIndex
        + " watermark " + new Instant(watermark) + ", " + " single pair "
      + "R2PairEdgeTracker: " + dataFetcherWatermarkTracker);
    }
  }

}
