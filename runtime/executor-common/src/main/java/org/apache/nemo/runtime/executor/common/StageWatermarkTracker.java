package org.apache.nemo.runtime.executor.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class StageWatermarkTracker {

  private final List<Long> watermarks;
  private int minWatermarkIndex;
  private Long currMinWatermark = Long.MIN_VALUE;


  public StageWatermarkTracker(final List<Long> watermarks,
                               final int minWatermarkIndex,
                               final Long currMinWatermark) {
    this.watermarks = watermarks;
    this.minWatermarkIndex = minWatermarkIndex;
    this.currMinWatermark = currMinWatermark;
  }

  public StageWatermarkTracker(final int numTasks) {
    this.watermarks = new ArrayList<>(numTasks);
    this.minWatermarkIndex = 0;

    for (int i = 0; i < numTasks; i++) {
      watermarks.add(Long.MIN_VALUE);
    }
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.size(); i++) {
      if (watermarks.get(i) < timestamp) {
        index = i;
        timestamp = watermarks.get(i);
      }
    }
    return index;
  }

  public synchronized long getWatermark(final int index) {
    //LOG.info("Watermark request index: {}. size: {},. get {}",
    //  index, watermarks.size(), watermarks.get(index));
    return watermarks.get(index);
  }


  public synchronized Optional<Long> trackAndEmitWatermarks(final int edgeIndex, final long watermark) {
    if (edgeIndex == minWatermarkIndex) {
      // update min watermark
      watermarks.set(minWatermarkIndex, watermark);

      // find min watermark
      final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
      final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

      if (nextMinWatermark <= currMinWatermark) {
        // it is possible
        minWatermarkIndex = nextMinWatermarkIndex;
        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = nextMinWatermarkIndex;
        currMinWatermark = nextMinWatermark;
        return Optional.of(currMinWatermark);
      }
    } else {
      // The recent watermark timestamp cannot be less than the previous one
      // because watermark is monotonically increasing.
      if (watermarks.get(edgeIndex) > watermark) {

      } else {
        watermarks.set(edgeIndex, watermark);
      }
    }

    return Optional.empty();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < watermarks.size(); i++) {
      sb.append(i);
      sb.append(": ");
      sb.append(watermarks.get(i));
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  public void encode(DataOutputStream dos) {
    try {
      dos.writeInt(watermarks.size());
      for (final Long watermark : watermarks) {
        dos.writeLong(watermark);
      }
      dos.writeInt(minWatermarkIndex);
      dos.writeLong(currMinWatermark);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);

    }
  }

  public static StageWatermarkTracker decode(final DataInputStream is) {
    try {
      final int size = is.readInt();
      final List<Long> watermarks = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        watermarks.add(is.readLong());
      }
      final int minWatermarkIndex = is.readInt();
      final long currMinWatermark = is.readLong();

      return new StageWatermarkTracker(watermarks, minWatermarkIndex, currMinWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
