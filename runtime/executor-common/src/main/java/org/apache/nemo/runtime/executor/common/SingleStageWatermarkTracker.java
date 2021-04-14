package org.apache.nemo.runtime.executor.common;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class SingleStageWatermarkTracker implements WatermarkTracker {

  private final List<Long> watermarks;
  private final List<Boolean> stoppedWatermarks;
  private int minWatermarkIndex;
  private Long currMinWatermark = Long.MIN_VALUE;
  private static final Logger LOG = LoggerFactory.getLogger(SingleStageWatermarkTracker.class.getName());
  private final String taskId;


  private SingleStageWatermarkTracker(final List<Long> watermarks,
                                      final List<Boolean> stoppedWatermarks,
                                      final int minWatermarkIndex,
                                      final Long currMinWatermark,
                                      final String taskId) {
    this.watermarks = watermarks;
    this.stoppedWatermarks = stoppedWatermarks;
    this.minWatermarkIndex = minWatermarkIndex;
    this.currMinWatermark = currMinWatermark;
    this.taskId = taskId;
  }

  public SingleStageWatermarkTracker(final int numTasks,
                                     final String taskId) {
    this.watermarks = new ArrayList<>(numTasks);
    this.stoppedWatermarks = new ArrayList<>(numTasks);
    this.minWatermarkIndex = 0;
    this.taskId = taskId;

    for (int i = 0; i < numTasks; i++) {
      watermarks.add(Long.MIN_VALUE);
      stoppedWatermarks.add(false);
    }
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.size(); i++) {
      if (!stoppedWatermarks.get(i)) {
        if (watermarks.get(i) < timestamp) {
          index = i;
          timestamp = watermarks.get(i);
        }
      }
    }
    return index;
  }

  public synchronized long getWatermark(final int index) {
    //LOG.info("Watermark request index: {}. size: {},. get {}",
    //  index, watermarks.size(), watermarks.get(index));
    return watermarks.get(index);
  }

  public void stopInputPipeWatermark(final int edgeIndex) {
    stoppedWatermarks.set(edgeIndex, true);
  }

  public List<Boolean> getStoppedWatermarks() {
    return stoppedWatermarks;
  }

  public void startInputPipeWatermark(final int edgeIndex,
                                      final long watermark) {
    stoppedWatermarks.set(edgeIndex, false);
    if (watermarks.get(edgeIndex) < watermark) {
      watermarks.set(edgeIndex, watermark);
    }
  }

  public Optional<Long> updateAndGetCurrentWatermark() {
    if (watermarks.size() == 1) {
      if (stoppedWatermarks.get(0)) {
        return Optional.empty();
      } else {
        return Optional.of(watermarks.get(0));
      }
    } else {
      // find min watermark
      final int nextMinWatermarkIndex = findNextMinWatermarkIndex();

      if (nextMinWatermarkIndex <= 0) {
        return Optional.empty();
      }

      final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

      if (nextMinWatermark > currMinWatermark) {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = nextMinWatermarkIndex;
        currMinWatermark = nextMinWatermark;
        return Optional.of(currMinWatermark);
      } else {
        return Optional.of(currMinWatermark);
      }
    }
  }

  @Override
  public synchronized Optional<Long> trackAndEmitWatermarks(final String edgeId,
                                                            final int edgeIndex,
                                                            final long watermark) {
    LOG.info("Track and emit watermark in task {} for edge {} / {} / {}, " +
        "minWatermark: {}, minWatermarkIndex: {}, watermarks: {}",
      taskId,
      edgeId, edgeIndex, new Instant(watermark),
      currMinWatermark,
      minWatermarkIndex,
      watermarks);

    if (watermarks.size() == 1) {
      // single o2o
      // update min watermark
      watermarks.set(0, watermark);

      // find min watermark
      final Long nextMinWatermark = watermarks.get(0);

      if (nextMinWatermark <= currMinWatermark) {
        // it is possible
        minWatermarkIndex = 0;
        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = 0;
        currMinWatermark = nextMinWatermark;
        return Optional.of(currMinWatermark);
      }
    } else {
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
          LOG.warn("Warning pre watermark {} is larger than current {}, index {}",
            new Instant(watermarks.get(edgeIndex)), new Instant(watermark), edgeIndex);
        } else {
          watermarks.set(edgeIndex, watermark);
        }
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
      for (int i = 0; i < watermarks.size(); i++) {
        dos.writeLong(watermarks.get(i));
        dos.writeBoolean(stoppedWatermarks.get(i));
      }
      dos.writeInt(minWatermarkIndex);
      dos.writeLong(currMinWatermark);
      dos.writeUTF(taskId);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);

    }
  }

  public static SingleStageWatermarkTracker decode(final DataInputStream is) {
    try {
      final int size = is.readInt();
      final List<Long> watermarks = new ArrayList<>(size);
      final List<Boolean> stoppedWatermarks = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        watermarks.add(is.readLong());
        stoppedWatermarks.add(is.readBoolean());
      }
      final int minWatermarkIndex = is.readInt();
      final long currMinWatermark = is.readLong();
      final String taskId = is.readUTF();

      return new SingleStageWatermarkTracker(watermarks, stoppedWatermarks, minWatermarkIndex, currMinWatermark, taskId);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
