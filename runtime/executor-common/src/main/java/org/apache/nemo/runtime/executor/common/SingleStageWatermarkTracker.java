package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class SingleStageWatermarkTracker implements WatermarkTracker {

  private final Long[] watermarks;
  private int minWatermarkIndex;
  private static final Logger LOG = LoggerFactory.getLogger(SingleStageWatermarkTracker.class.getName());
  private long prevEmitWatermark = Long.MIN_VALUE;

  private final WatermarkTracker watermarkTracker;


  private SingleStageWatermarkTracker(final List<Long> watermarks,
                                      final int minWatermarkIndex,
                                      final long prevEmitWatermark) {
    this.watermarks = new Long[watermarks.size()];
    watermarks.toArray(this.watermarks);
    this.minWatermarkIndex = minWatermarkIndex;
    this.prevEmitWatermark = prevEmitWatermark;
    this.watermarkTracker = this.watermarks.length == 1
      ? new SingleWatermarkTracker() : new MultiWatermarkTracker();
  }

  public SingleStageWatermarkTracker(final int numTasks) {
    this.watermarks = new Long[numTasks];
    this.minWatermarkIndex = 0;

    for (int i = 0; i < numTasks; i++) {
      watermarks[i] = Long.MIN_VALUE;
    }

    this.watermarkTracker = this.watermarks.length == 1
      ? new SingleWatermarkTracker() : new MultiWatermarkTracker();
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.length; i++) {
      if (watermarks[i] < timestamp) {
        index = i;
        timestamp = watermarks[i];
      }
    }
    return index;
  }

  @Override
  public synchronized Optional<Long> trackAndEmitWatermarks(final String taskId,
                                                            final String edgeId,
                                                            final int edgeIndex,
                                                            final long watermark) {
    /*
    LOG.info("Receive watermark {} / index {} edge {} task {} / " +
      "watermarks {} / minWatermarkIndex {} prevEmitWatermark {}", watermark, edgeIndex, edgeId, taskId,
    watermarks, minWatermarkIndex, prevEmitWatermark);
    */
    return watermarkTracker.trackAndEmitWatermarks(taskId, edgeId, edgeIndex, watermark);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < watermarks.length; i++) {
      sb.append(i);
      sb.append(": ");
      sb.append(watermarks[i]);
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  final class SingleWatermarkTracker implements WatermarkTracker {

    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {
      // single o2o
      // update min watermark
      watermarks[0] = watermark;

      // find min watermark
      final Long nextMinWatermark = watermarks[0];

      if (nextMinWatermark < prevEmitWatermark) {
        // it is possible
        throw new RuntimeException(taskId + " NexMinWatermar < CurrMinWatermark" +
          nextMinWatermark + " <= " + prevEmitWatermark + ", "
          + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
          " prevEmitWatermark: " + prevEmitWatermark);

        // return Optional.empty();

        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = 0;
        if (nextMinWatermark >= prevEmitWatermark + Util.WATERMARK_PROGRESS) {
          prevEmitWatermark = nextMinWatermark;
          return Optional.of(nextMinWatermark);
        } else {
          return Optional.empty();
        }
      }
    }
  }

  final class MultiWatermarkTracker implements WatermarkTracker {

    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {
      if (edgeIndex == minWatermarkIndex) {
        // update min watermark
        watermarks[minWatermarkIndex] = watermark;

        // find min watermark
        final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
        final Long nextMinWatermark = watermarks[nextMinWatermarkIndex];

        if (nextMinWatermark < prevEmitWatermark) {
          // it is possible
          throw new RuntimeException(taskId + " NexMinWatermar < CurrMinWatermark" +
            nextMinWatermark + " <= " + prevEmitWatermark + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
            " prevEmitWatermark: " + prevEmitWatermark);
          // minWatermarkIndex = nextMinWatermarkIndex;
          //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
        } else {
          // Watermark timestamp progress!
          // Emit the min watermark
          minWatermarkIndex = nextMinWatermarkIndex;
          if (nextMinWatermark >= prevEmitWatermark + Util.WATERMARK_PROGRESS) {
            prevEmitWatermark = nextMinWatermark;
            return Optional.of(nextMinWatermark);
          } else {
            return Optional.empty();
          }
        }
      } else {
        // The recent watermark timestamp cannot be less than the previous one
        // because watermark is monotonically increasing.
        if (watermarks[edgeIndex] > watermark) {
          throw new RuntimeException(taskId + " watermarks.get(edgeIndex) > watermark" +
            watermarks[edgeIndex] + " > " + watermark + ", "
            + "edgeIndex: " + edgeIndex  + ", " + prevEmitWatermark + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
            "prevEmitWatermark: " + prevEmitWatermark);

          // LOG.warn("Warning pre watermark {} is larger than current {}, index {}",
          //  new Instant(watermarks.get(edgeIndex)), new Instant(watermark), edgeIndex);
        } else {
          watermarks[edgeIndex] = watermark;
          return Optional.empty();
        }
      }
    }
  }

  public void encode(final String taskId,
                     DataOutputStream dos) {
    try {
      dos.writeInt(watermarks.length);
      for (int i = 0; i < watermarks.length; i++) {
        dos.writeLong(watermarks[i]);
      }
      dos.writeInt(minWatermarkIndex);
      dos.writeLong(prevEmitWatermark);

      LOG.info("Encoding single stage watermark tracker in {} watermarks: {} ," +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {}, ", taskId, watermarks, minWatermarkIndex,
        prevEmitWatermark);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);

    }
  }

  public static SingleStageWatermarkTracker decode(final String taskId,
                                                   final DataInputStream is) {
    try {
      final int size = is.readInt();
      final List<Long> watermarks = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        watermarks.add(is.readLong());
      }
      final int minWatermarkIndex = is.readInt();
      final long prevEmitWatermark = is.readLong();

      LOG.info("Decoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {} ", taskId, watermarks, minWatermarkIndex,
        prevEmitWatermark);

      return new SingleStageWatermarkTracker(watermarks,
        minWatermarkIndex, prevEmitWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
