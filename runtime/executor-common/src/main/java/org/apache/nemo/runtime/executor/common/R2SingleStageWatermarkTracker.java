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

public final class R2SingleStageWatermarkTracker implements WatermarkTracker {

  private final Long[] watermarks;
  private final Boolean[] stoppedWatermarks;
  private int minWatermarkIndex;
  private Long prevEmitWatermark = Long.MIN_VALUE;
  private static final Logger LOG = LoggerFactory.getLogger(R2SingleStageWatermarkTracker.class.getName());
  private boolean allStopped;

  private final WatermarkTracker watermarkTracker;
  private final WatermarkTracker stoppedWatermarkTracker;

  private WatermarkTracker currWatermarkTracker;

  private R2SingleStageWatermarkTracker(final List<Long> watermarks,
                                        final List<Boolean> stoppedWatermarks,
                                        final int minWatermarkIndex,
                                        final Long prevEmitWatermark) {
    this.watermarks = new Long[watermarks.size()];
    watermarks.toArray(this.watermarks);
    this.stoppedWatermarks = new Boolean[stoppedWatermarks.size()];
    stoppedWatermarks.toArray(this.stoppedWatermarks);
    this.minWatermarkIndex = minWatermarkIndex;
    this.prevEmitWatermark = prevEmitWatermark;
    this.allStopped = stoppedWatermarks.stream().allMatch(val -> val);
    this.watermarkTracker = watermarks.size() == 1 ? new SingleWatermarkTracker()
      : new MultiWatermarkTracker();
    this.stoppedWatermarkTracker = new StoppedWatermarkTracker();
    this.currWatermarkTracker = allStopped ? stoppedWatermarkTracker : watermarkTracker;
  }

  public R2SingleStageWatermarkTracker(final int numTasks, final boolean stopped) {
    this.watermarks = new Long[numTasks];
    this.stoppedWatermarks = new Boolean[numTasks];
    this.minWatermarkIndex = 0;

    for (int i = 0; i < numTasks; i++) {
      watermarks[i] = Long.MIN_VALUE;
      stoppedWatermarks[i] = stopped;
    }

    this.stoppedWatermarkTracker = new StoppedWatermarkTracker();
    this.watermarkTracker = numTasks == 1 ? new SingleWatermarkTracker()
      : new MultiWatermarkTracker();
    this.currWatermarkTracker = stopped ? stoppedWatermarkTracker : watermarkTracker;
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.length; i++) {
      if (!stoppedWatermarks[i]) {
        if (watermarks[i] < timestamp) {
          index = i;
          timestamp = watermarks[i];
        }
      }
    }
    return index;
  }


  public synchronized long getWatermark(final int index) {
    //LOG.info("Watermark request index: {}. size: {},. get {}",
    //  index, watermarks.size(), watermarks.get(index));
    return watermarks[index];
  }

  public void stopInputPipeWatermark(final int edgeIndex) {
    stoppedWatermarks[edgeIndex] = true;

    allStopped = true;
    for (int i = 0; i < stoppedWatermarks.length; i++) {
      if (!stoppedWatermarks[i]) {
        allStopped = false;
        break;
      }
    }

    if (allStopped) {
      currWatermarkTracker = stoppedWatermarkTracker;
    }
  }

  public boolean isStopped(final int edgeIndex) {
    return stoppedWatermarks[edgeIndex];
  }

  public void startInputPipeWatermark(final int edgeIndex) {
    if (!stoppedWatermarks[edgeIndex]) {
      throw new RuntimeException("Edge index " + edgeIndex + " already started.. but try to restart again");
    }

    stoppedWatermarks[edgeIndex] = false;
    /*
    if (watermarks[edgeIndex] < watermark) {
      watermarks[edgeIndex] = watermark;
    }
    */
    allStopped = false;
    currWatermarkTracker = watermarkTracker;
  }

  public void setWatermark(final int index,
                           final long watermark,
                           final String taskId,
                           final String edgeId) {
    if (watermarks[index] > watermark) {
      LOG.warn("Curr Watermark is smaller than prev watermark " + index + ", " + new Instant(watermark) + ", prev " +
        new Instant(watermarks[index]));
    }

//    if (taskId.contains("Stage4")) {
//      LOG.info("Set watermark {} in {} for {}/{}", new Instant(watermark), taskId, edgeId, index);
//    }

    watermarks[index] = watermark;
  }

  public Optional<Long> updateAndGetCurrentWatermark() {
    if (allStopped) {
      return Optional.empty();
    }

    if (watermarks.length == 1) {
      return Optional.of(watermarks[0]);
    } else {
      // find min watermark
      final int nextMinWatermarkIndex = findNextMinWatermarkIndex();

      if (nextMinWatermarkIndex < 0) {
        return Optional.empty();
      }

      final Long nextMinWatermark = watermarks[nextMinWatermarkIndex];

      if (nextMinWatermark > prevEmitWatermark) {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = nextMinWatermarkIndex;
        prevEmitWatermark = nextMinWatermark;
        return Optional.of(nextMinWatermark);
      } else {
        LOG.warn("NextMinWatermark <= PrevEmitWatermark {} / {} / {} / {}",
          new Instant(nextMinWatermark), new Instant(prevEmitWatermark),
          printWatermark(watermarks),
          buildArray(stoppedWatermarks));
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
    return currWatermarkTracker.trackAndEmitWatermarks(taskId, edgeId, edgeIndex, watermark);
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
        LOG.warn("task " + taskId + " edge " + edgeId + "NexMinWatermar < CurrMinWatermark " +
            new Instant(nextMinWatermark) + " <= " + new Instant(prevEmitWatermark) + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + printWatermark(watermarks) +
            " stopped: " + buildArray(stoppedWatermarks));
        return Optional.empty();
        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else {
        // Watermark timestamp progress!
        // Emit the min watermark
        if (nextMinWatermark > prevEmitWatermark) {
          prevEmitWatermark = nextMinWatermark;
          return Optional.of(nextMinWatermark);
        } else {
          return Optional.empty();
        }
      }
    }
  }

  final class StoppedWatermarkTracker implements WatermarkTracker {
    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {
      return Optional.empty();
    }
  }

  final class MultiWatermarkTracker implements WatermarkTracker {
    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {

//      if (taskId.contains("Stage4")) {
//        LOG.info("Receive watermark {} at {} from {}/{} {}",
//          new Instant(watermark), taskId, edgeId, edgeIndex, Thread.currentThread());
//      }

      if (edgeIndex == minWatermarkIndex) {
        // update min watermark
        watermarks[minWatermarkIndex] = watermark;

        // find min watermark
        final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
        final Long nextMinWatermark = watermarks[nextMinWatermarkIndex];

        if (nextMinWatermark < prevEmitWatermark) {
          // it is possible
          LOG.warn("task " + taskId + " edge " + edgeId + "NexMinWatermar < CurrMinWatermark " +
            new Instant(nextMinWatermark) + " <= " + new Instant(prevEmitWatermark) + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + printWatermark(watermarks) +
            " stopped: " + buildArray(stoppedWatermarks));
          // minWatermarkIndex = nextMinWatermarkIndex;
          //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
          return Optional.empty();
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
        if (watermarks[edgeIndex] > watermark) {

          throw new RuntimeException("task " + taskId + " edge " + edgeId + " watermarks.get(edgeIndex) > watermark" +
            watermarks[edgeIndex] + " > " +
            new Instant(watermark) + ", "
            + "edgeIndex: " + edgeIndex + ", " + new Instant(prevEmitWatermark) + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + printWatermark(watermarks) +
            " stopped: " + buildArray(stoppedWatermarks));

          // LOG.warn("Warning pre watermark {} is larger than current {}, index {}",
          //  new Instant(watermarks.get(edgeIndex)), new Instant(watermark), edgeIndex);
        } else {
          watermarks[edgeIndex] =  watermark;
          return Optional.empty();
        }
      }
    }
  }

  private static String printWatermark(Long[] array) {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < array.length; i++) {
      sb.append(new Instant(array[i]));
      sb.append(",");
    }
    sb.append("]");
    return sb.toString();
  }

  private static String buildArray(Object[] array) {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < array.length; i++) {
      sb.append(array[i]);
      sb.append(",");
    }
    sb.append("]");
    return sb.toString();
  }
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("watermarks: ");
    sb.append(printWatermark(watermarks));
    sb.append(", stopped: ");
    sb.append(buildArray(stoppedWatermarks));
    sb.append(", prevEmitWatermark: ");
    sb.append(new Instant(prevEmitWatermark));
    sb.append(", minWatermarkIndex: ");
    sb.append(new Instant(minWatermarkIndex));

    return sb.toString();
  }

  public void encode(final String taskId,
                     DataOutputStream dos) {
    try {
      dos.writeInt(watermarks.length);
      for (int i = 0; i < watermarks.length; i++) {
        dos.writeLong(watermarks[i]);
        dos.writeBoolean(stoppedWatermarks[i]);
      }
      dos.writeInt(minWatermarkIndex);
      dos.writeLong(prevEmitWatermark);

      LOG.info("Encoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {}", taskId,
        printWatermark(watermarks),
        buildArray(stoppedWatermarks), minWatermarkIndex,
        new Instant(prevEmitWatermark));

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
      final Long[] arr = new Long[watermarks.size()];
      watermarks.toArray(arr);

      LOG.info("Decoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {}", taskId,
        printWatermark(arr),
        buildArray(stoppedWatermarks.toArray()), minWatermarkIndex,
        new Instant(prevEmitWatermark));

      return new R2SingleStageWatermarkTracker(watermarks,
        stoppedWatermarks, minWatermarkIndex, prevEmitWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
