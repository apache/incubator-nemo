package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class R2SingleStageWatermarkTracker implements WatermarkTracker {

  private final List<Long> watermarks;
  private final List<Boolean> stoppedWatermarks;
  private int minWatermarkIndex;
  private Long prevEmitWatermark = Long.MIN_VALUE;
  private static final Logger LOG = LoggerFactory.getLogger(R2SingleStageWatermarkTracker.class.getName());
  private boolean allStopped;

  private R2SingleStageWatermarkTracker(final List<Long> watermarks,
                                        final List<Boolean> stoppedWatermarks,
                                        final int minWatermarkIndex,
                                        final Long prevEmitWatermark) {
    this.watermarks = watermarks;
    this.stoppedWatermarks = stoppedWatermarks;
    this.minWatermarkIndex = minWatermarkIndex;
    this.prevEmitWatermark = prevEmitWatermark;
    this.allStopped = stoppedWatermarks.stream().allMatch(val -> val);
  }

  public R2SingleStageWatermarkTracker(final int numTasks) {
    this.watermarks = new ArrayList<>(numTasks);
    this.stoppedWatermarks = new ArrayList<>(numTasks);
    this.minWatermarkIndex = 0;

    for (int i = 0; i < numTasks; i++) {
      watermarks.add(Long.MIN_VALUE);
      stoppedWatermarks.add(false);
    }
  }

  private int findNextMinWatermarkIndex() {
    if (allStopped) {
      return -1;
    }

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
    if (stoppedWatermarks.stream().allMatch(val -> val)) {
      allStopped = true;
    }
  }

  public boolean isStopped(final int edgeIndex) {
    return stoppedWatermarks.get(edgeIndex);
  }

  public List<Boolean> getStoppedWatermarks() {
    return stoppedWatermarks;
  }

  public void startInputPipeWatermark(final int edgeIndex,
                                      final long watermark) {
    stoppedWatermarks.set(edgeIndex, false);
    watermarks.set(edgeIndex, watermark);
    allStopped = false;
  }

  public Optional<Long> updateAndGetCurrentWatermark() {
    if (allStopped) {
      return Optional.empty();
    }

    if (watermarks.size() == 1) {
      return Optional.of(watermarks.get(0));
    } else {
      // find min watermark
      final int nextMinWatermarkIndex = findNextMinWatermarkIndex();

      if (nextMinWatermarkIndex < 0) {
        return Optional.empty();
      }

      final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

      if (nextMinWatermark > prevEmitWatermark) {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = nextMinWatermarkIndex;
        prevEmitWatermark = nextMinWatermark;
        return Optional.of(nextMinWatermark);
      } else {
        LOG.warn("NextMinWatermark <= PrevEmitWatermark {} / {} / {} / {}",
          nextMinWatermark, prevEmitWatermark, watermarks, stoppedWatermarks);
        minWatermarkIndex = nextMinWatermarkIndex;
        prevEmitWatermark = nextMinWatermark;
        return Optional.of(nextMinWatermark);
      }
    }
  }

  @Override
  public synchronized Optional<Long> trackAndEmitWatermarks(final String taskId,
                                                            final String edgeId,
                                                            final int edgeIndex,
                                                            final long watermark) {

    if (allStopped) {
      return Optional.empty();
    }

    if (watermarks.size() == 1) {
      // single o2o
      // update min watermark
      watermarks.set(0, watermark);

      // find min watermark
      final Long nextMinWatermark = watermarks.get(0);

      if (nextMinWatermark < prevEmitWatermark) {
        // it is possible
        throw new RuntimeException("task " + taskId + " edge " + edgeId + "NexMinWatermar < CurrMinWatermark" +
          nextMinWatermark + " <= " + prevEmitWatermark + ", "
          + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
          " stopped: " + stoppedWatermarks);
        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = 0;

        if (nextMinWatermark > prevEmitWatermark) {
          prevEmitWatermark = nextMinWatermark;
          return Optional.of(nextMinWatermark);
        } else {
          return Optional.empty();
        }
      }
    } else {
      if (edgeIndex == minWatermarkIndex) {
        // update min watermark
        watermarks.set(minWatermarkIndex, watermark);

        // find min watermark
        final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
        final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

        if (nextMinWatermark < prevEmitWatermark) {
          // it is possible
          throw new RuntimeException("task " + taskId + " edge " + edgeId + "NexMinWatermar < CurrMinWatermark" +
          nextMinWatermark + " <= " + prevEmitWatermark + ", "
          + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
            " stopped: " + stoppedWatermarks);
          // minWatermarkIndex = nextMinWatermarkIndex;
          //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
        } else {
          // Watermark timestamp progress!
          // Emit the min watermark
          minWatermarkIndex = nextMinWatermarkIndex;

          if (nextMinWatermark > prevEmitWatermark) {
            prevEmitWatermark = nextMinWatermark;
            return Optional.of(nextMinWatermark);
          } else {
            return Optional.empty();
          }
        }
      } else {
        // The recent watermark timestamp cannot be less than the previous one
        // because watermark is monotonically increasing.
        if (watermarks.get(edgeIndex) > watermark) {
          throw new RuntimeException("task " + taskId + " edge " + edgeId + " watermarks.get(edgeIndex) > watermark" +
            watermarks.get(edgeIndex) + " > " + watermark + ", "
             + "edgeIndex: " + edgeIndex + ", " + prevEmitWatermark + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
            " stopped: " + stoppedWatermarks);

          // LOG.warn("Warning pre watermark {} is larger than current {}, index {}",
          //  new Instant(watermarks.get(edgeIndex)), new Instant(watermark), edgeIndex);
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
    sb.append("watermarks: ");
    sb.append(watermarks);
    sb.append(", stopped: ");
    sb.append(stoppedWatermarks);
    sb.append(", prevEmitWatermark: ");
    sb.append(prevEmitWatermark);
    sb.append(", minWatermarkIndex: ");
    sb.append(minWatermarkIndex);

    return sb.toString();
  }

  public void encode(final String taskId,
                     DataOutputStream dos) {
    try {
      dos.writeInt(watermarks.size());
      for (int i = 0; i < watermarks.size(); i++) {
        dos.writeLong(watermarks.get(i));
        dos.writeBoolean(stoppedWatermarks.get(i));
      }
      dos.writeInt(minWatermarkIndex);
      dos.writeLong(prevEmitWatermark);

      LOG.info("Encoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {}", taskId, watermarks, stoppedWatermarks, minWatermarkIndex, prevEmitWatermark);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);

    }
  }

  public static R2SingleStageWatermarkTracker decode(final String taskId,
                                                     final DataInputStream is) {
    try {
      final int size = is.readInt();
      final List<Long> watermarks = new ArrayList<>(size);
      final List<Boolean> stoppedWatermarks = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        watermarks.add(is.readLong());
        stoppedWatermarks.add(is.readBoolean());
      }
      final int minWatermarkIndex = is.readInt();
      final long prevEmitWatermark = is.readLong();

      LOG.info("Decoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {}", taskId, watermarks, stoppedWatermarks, minWatermarkIndex, prevEmitWatermark);

      return new R2SingleStageWatermarkTracker(watermarks,
        stoppedWatermarks, minWatermarkIndex, prevEmitWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
