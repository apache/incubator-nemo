package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public final class TaskInputWatermarkManager implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskInputWatermarkManager.class.getName());

  private long prevWatermark = 0L;
  private final Map<String, Long> dataFetcherWatermarkMap;
  private final Map<String, StageWatermarkTracker> dataFetcherWatermarkTracker;


  public TaskInputWatermarkManager() {
    this.dataFetcherWatermarkMap = new HashMap<>();
    this.dataFetcherWatermarkTracker = new HashMap<>();
  }

  public TaskInputWatermarkManager(final long prevWatermark,
                                   final Map<String, Long> dataFetcherWatermarkMap,
                                   final Map<String, StageWatermarkTracker> dataFetcherWatermarkTracker) {
    this.prevWatermark = prevWatermark;
    this.dataFetcherWatermarkTracker = dataFetcherWatermarkTracker;
    this.dataFetcherWatermarkMap = dataFetcherWatermarkMap;
  }

  public void encode(final OutputStream os) {
    try {
      final DataOutputStream dos = new DataOutputStream(os);
      dos.writeLong(prevWatermark);
      dos.writeInt(dataFetcherWatermarkMap.size());
      for (final String df : dataFetcherWatermarkMap.keySet()) {
        dos.writeUTF(df);
        dos.writeLong(dataFetcherWatermarkMap.get(df));
      }

      dos.writeInt(dataFetcherWatermarkTracker.size());
      for (final String df : dataFetcherWatermarkTracker.keySet()) {
        dos.writeUTF(df);
        dataFetcherWatermarkTracker.get(df).encode(dos);
      }

      dos.close();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static TaskInputWatermarkManager decode(final InputStream is) {
    final DataInputStream dis = new DataInputStream(is);
    try {
      final long prevWatermark = dis.readLong();
      final int size = dis.readInt();
      final Map<String, Long> dataFetcherWatermarkMap = new HashMap<>(size);

      for (int i = 0; i < size; i++) {
        final String df = dis.readUTF();
        final long wm = dis.readLong();
        dataFetcherWatermarkMap.put(df, wm);
      }

      final int size2 = dis.readInt();
      final Map<String, StageWatermarkTracker> dtaFetcherWatermarkTracker = new HashMap<>(size2);

      for (int i = 0; i < size2; i++) {
        final String df = dis.readUTF();
        final StageWatermarkTracker stageWatermarkTracker = StageWatermarkTracker.decode(dis);
        dtaFetcherWatermarkTracker.put(df, stageWatermarkTracker);
      }

      return new TaskInputWatermarkManager(prevWatermark, dataFetcherWatermarkMap, dtaFetcherWatermarkTracker);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void addDataFetcher(String edgeId, int parallelism) {
    LOG.info("Add data fetcher for datafetcher {}, parallelism: {}", edgeId, parallelism);
    dataFetcherWatermarkMap.put(edgeId, 0L);
    dataFetcherWatermarkTracker.put(edgeId, new StageWatermarkTracker(parallelism));
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
