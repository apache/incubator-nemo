package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class R2MultiPairWatermarkManager implements R2WatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(R2MultiPairWatermarkManager.class.getName());

  private long prevWatermark = 0L;
  // pair: vm edge, lambda edge
  private final Map<Pair<String, String>, Long> dataFetcherWatermarkMap;
  private final Map<Pair<String, String>, R2PairEdgeWatermarkTracker> dataFetcherWatermarkTracker;
  private final Map<String, Pair<String, String>> pairEdgeMap;
  private final String taskId;

  public R2MultiPairWatermarkManager(final String taskId) {
    this.dataFetcherWatermarkMap = new HashMap<>();
    this.dataFetcherWatermarkTracker = new HashMap<>();
    this.pairEdgeMap = new HashMap<>();
    this.taskId = taskId;
  }

  private R2MultiPairWatermarkManager(final String taskId,
                                      final long prevWatermark,
                                      final Map<Pair<String, String>, Long> dataFetcherWatermarkMap,
                                      final Map<Pair<String, String>, R2PairEdgeWatermarkTracker> dataFetcherWatermarkTracker,
                                      final Map<String, Pair<String, String>> pairEdgeMap) {
    this.taskId = taskId;
    this.prevWatermark = prevWatermark;
    this.dataFetcherWatermarkMap = dataFetcherWatermarkMap;
    this.dataFetcherWatermarkTracker = dataFetcherWatermarkTracker;
    this.pairEdgeMap = pairEdgeMap;
  }

  public static R2MultiPairWatermarkManager decode(final String taskId,
                                                   final DataInputStream dis) {
    try {
      final long prevWatermark = dis.readLong();
      final int size = dis.readInt();
      final Map<Pair<String, String>, Long> dataFetcherWatermarkMap = new HashMap<>(size);
      final Map<Pair<String, String>, R2PairEdgeWatermarkTracker> dataFetcherWatermarkTracker = new HashMap<>(size);
      final Map<String, Pair<String, String>> pairEdgeMap = new HashMap<>(size);

      for (int i = 0; i < size; i++) {
        final String vmEdgeId = dis.readUTF();
        final String lambdaEdgeId = dis.readUTF();
        final long watermark = dis.readLong();
        final R2PairEdgeWatermarkTracker watermarkTracker = R2PairEdgeWatermarkTracker
          .decode(vmEdgeId, lambdaEdgeId, taskId, dis);

        final Pair<String, String> key = Pair.of(vmEdgeId, lambdaEdgeId);
        dataFetcherWatermarkMap.put(key, watermark);
        dataFetcherWatermarkTracker.put(key, watermarkTracker);
        pairEdgeMap.put(vmEdgeId, key);
        pairEdgeMap.put(lambdaEdgeId, key);
      }

      return new R2MultiPairWatermarkManager(taskId,
        prevWatermark,
        dataFetcherWatermarkMap,
        dataFetcherWatermarkTracker,
        pairEdgeMap);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void encode(final DataOutputStream dos) {
    try {
      dos.writeLong(prevWatermark);
      dos.writeInt(dataFetcherWatermarkMap.size());
      for (final Map.Entry<Pair<String, String>, Long> entry : dataFetcherWatermarkMap.entrySet()) {
        dos.writeUTF(entry.getKey().left());
        dos.writeUTF(entry.getKey().right());
        dos.writeLong(entry.getValue());
        // data fetcher watermark tracker
        final R2PairEdgeWatermarkTracker tracker = dataFetcherWatermarkTracker.get(entry.getKey());
        tracker.encode(dos);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addDataFetcher(String vmEdgeId, String lambdaEdgeId, int parallelism) {
    LOG.info("Add data fetcher for datafetcher {}/{}, parallelism: {}", vmEdgeId, lambdaEdgeId, parallelism);
    final Pair<String, String> key = Pair.of(vmEdgeId, lambdaEdgeId);

    if (dataFetcherWatermarkMap.containsKey(key)) {
      // This means that it is checkpointed and restored
      LOG.info("Skip add data fetcher for datafetcher {}/{}, parallelism: {}", vmEdgeId, lambdaEdgeId, parallelism);
    } else {
      final R2PairEdgeWatermarkTracker stageWatermarkTracker =
        new R2PairEdgeWatermarkTracker(vmEdgeId, lambdaEdgeId, taskId, false, parallelism );
      dataFetcherWatermarkMap.put(key, 0L);
      dataFetcherWatermarkTracker.put(key, stageWatermarkTracker);

      pairEdgeMap.put(vmEdgeId, key);
      pairEdgeMap.put(lambdaEdgeId, key);
    }
  }

  @Override
  public boolean stopIndex(final int taskIndex,
                           final String edgeId) {
    final Pair<String, String> key = pairEdgeMap.get(edgeId);
    return dataFetcherWatermarkTracker.get(key).stopInputIndex(taskIndex, edgeId);
  }

  @Override
  public void startIndex(final int taskIndex,
                         final String edgeId) {
    final Pair<String, String> key = pairEdgeMap.get(edgeId);
    dataFetcherWatermarkTracker.get(key).startIndex(taskIndex, edgeId, prevWatermark);
  }

  @Override
  public void startAndStopPairIndex(int taskIndex, String edgeId) {
    final Pair<String, String> key = pairEdgeMap.get(edgeId);
    dataFetcherWatermarkTracker.get(key).startIndex(taskIndex, edgeId, prevWatermark);

    if (key.left().equals(edgeId)) {
      dataFetcherWatermarkTracker.get(key).stopInputIndex(taskIndex, key.right());
    } else {
      dataFetcherWatermarkTracker.get(key).stopInputIndex(taskIndex, key.left());
    }
  }

  @Override
  public Optional<Watermark> updateWatermark(final String edgeId,
                                             final int taskIndex, final long watermark) {
    try {
      final Pair<String, String> key = pairEdgeMap.get(edgeId);
      final WatermarkTracker stageWatermarkTracker = dataFetcherWatermarkTracker.get(key);
      final Optional<Long> val = stageWatermarkTracker.trackAndEmitWatermarks(taskId, edgeId, taskIndex, watermark);

      if (val.isPresent()) {
        // update output watermark!
        final long outputW = dataFetcherWatermarkMap.get(key);
        if (outputW > val.get()) {
          throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " +
            new Instant(outputW) + ", " + new Instant(val.get()));
        }

        dataFetcherWatermarkMap.put(key, val.get());
        final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());

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
        + "prevWatermark: " + new Instant(prevWatermark) + ", watermark " +
        new Instant(watermark) + ", " + " pairEdgeMap: " + pairEdgeMap +
        ", dataFetcherWatermarkTracker key: " + dataFetcherWatermarkTracker.keySet() +
      ", watermark map: " + printDataFetcherWatermarkMap() + ", "
      + "R2PairEdgeTracker: " + dataFetcherWatermarkTracker.values());
    }
  }

  private String printDataFetcherWatermarkMap() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    dataFetcherWatermarkMap.forEach((key, val) -> {
      sb.append(key);
      sb.append(":");
      sb.append(new Instant(val));
      sb.append(",");
    });
    sb.append("}");
    return sb.toString();
  }

}
