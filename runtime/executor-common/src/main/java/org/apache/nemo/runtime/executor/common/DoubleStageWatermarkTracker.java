package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Optional;

public final class DoubleStageWatermarkTracker implements WatermarkTracker {
  private static final Logger LOG = LoggerFactory.getLogger(DoubleStageWatermarkTracker.class.getName());

  private long prevWatermark = 0L;

  private final String firstEdgeId;
  private final String secondEdgeId;

  private final WatermarkTracker firstEdgeWTracker;
  private final WatermarkTracker secondEdgeWTracker;

  private long firstEdgePrevWatermark = 0;
  private long secondEdgePrevWatermark = 0;

  public DoubleStageWatermarkTracker(final String firstEdgeId,
                                     final int firstEdgeParallelism,
                                     final String secondEdgeId,
                                     final int secondEdgeParallelism) {
    this.firstEdgeId = firstEdgeId;
    this.secondEdgeId = secondEdgeId;

    this.firstEdgeWTracker = new SingleStageWatermarkTracker(firstEdgeParallelism);
    this.secondEdgeWTracker = new SingleStageWatermarkTracker(secondEdgeParallelism);
  }

  private DoubleStageWatermarkTracker(final long prevWatermark,
                                      final String firstEdgeId,
                                      final String secondEdgeId,
                                      final WatermarkTracker firstEdgeWTracker,
                                      final WatermarkTracker secondEdgeWTracker,
                                      final long firstEdgePrevWatermark,
                                      final long secondEdgePrevWatermark) {
    this.prevWatermark = prevWatermark;
    this.firstEdgeId = firstEdgeId;
    this.secondEdgeId = secondEdgeId;
    this.firstEdgeWTracker = firstEdgeWTracker;
    this.secondEdgeWTracker = secondEdgeWTracker;
    this.firstEdgePrevWatermark = firstEdgePrevWatermark;
    this.secondEdgePrevWatermark = secondEdgePrevWatermark;
  }

  @Override
  public Optional<Long> trackAndEmitWatermarks(final String taskId,
                                               final String edgeId,
                                               final int taskIndex,
                                               final long watermark) {
    try {
      if (edgeId.equals(firstEdgeId)) {
        final long outputW = firstEdgePrevWatermark;
        final Optional<Long> val = firstEdgeWTracker.trackAndEmitWatermarks(taskId,
          edgeId, taskIndex, watermark);

        if (val.isPresent()) {
          // update output watermark!
          if (outputW > val.get()) {
            throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
          }

          firstEdgePrevWatermark = val.get();
          final long minWatermark = firstEdgePrevWatermark < secondEdgePrevWatermark ?
            firstEdgePrevWatermark : secondEdgePrevWatermark;

          if (minWatermark >= prevWatermark + Util.WATERMARK_PROGRESS) {
            // watermark progress
            prevWatermark = minWatermark;
            return Optional.of(minWatermark);
          }
        }

        return Optional.empty();

      } else {
        final long outputW = secondEdgePrevWatermark;
        final Optional<Long> val = secondEdgeWTracker.trackAndEmitWatermarks(taskId,
          edgeId, taskIndex, watermark);

        if (val.isPresent()) {
          // update output watermark!
          if (outputW > val.get()) {
            throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
          }

          secondEdgePrevWatermark = val.get();
          final long minWatermark = firstEdgePrevWatermark < secondEdgePrevWatermark ?
            firstEdgePrevWatermark : secondEdgePrevWatermark;

          if (minWatermark >= prevWatermark + Util.WATERMARK_PROGRESS) {
            // watermark progress
            prevWatermark = minWatermark;
            return Optional.of(minWatermark);
          }
        }

        return Optional.empty();
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("UpdateWatermark exception " + edgeId + ", task "
        + taskId + ", watermark " + watermark +
        ", firstEdge: " + firstEdgeId +
        ", secondEdge: " + secondEdgeId +
        ", firstEdgePrevW: " + firstEdgePrevWatermark +
        ", secondEdgePrevW: " + secondEdgePrevWatermark +
        ", prevWatermark: " + prevWatermark);
    }
  }

  public void encode(final String taskId, DataOutputStream dos) {
    try {
      dos.writeLong(prevWatermark);
      ((SingleStageWatermarkTracker)firstEdgeWTracker).encode(taskId, dos);
      ((SingleStageWatermarkTracker)secondEdgeWTracker).encode(taskId, dos);
      dos.writeLong(firstEdgePrevWatermark);
      dos.writeLong(secondEdgePrevWatermark);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static DoubleStageWatermarkTracker decode(final String taskId,
                                                   final String firstEdgeId,
                                                   final String secondEdgeId,
                                                   final DataInputStream is) {
    try {
      final long prevWatermark = is.readLong();
      final SingleStageWatermarkTracker firstEdgeWTracker = SingleStageWatermarkTracker.decode(taskId, is);
      final SingleStageWatermarkTracker secondEdgeWTracker = SingleStageWatermarkTracker.decode(taskId, is);
      final long firstEdgePrevWatermark = is.readLong();
      final long secondEdgePrevWatermark = is.readLong();

      return new DoubleStageWatermarkTracker(
        prevWatermark,
        firstEdgeId,
        secondEdgeId,
        firstEdgeWTracker,
        secondEdgeWTracker,
        firstEdgePrevWatermark,
        secondEdgePrevWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
