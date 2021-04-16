package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Optional;

public final class SingleSingleWatermarkTracker implements WatermarkTracker {

  private boolean stoppedWatermark;
  private static final Logger LOG = LoggerFactory.getLogger(SingleSingleWatermarkTracker.class.getName());
  private long prevEmitWatermark = Long.MIN_VALUE;

  private SingleSingleWatermarkTracker(final boolean stoppedWatermark,
                                       final long prevEmitWatermark) {
    this.stoppedWatermark = stoppedWatermark;
    this.prevEmitWatermark = prevEmitWatermark;
  }

  public SingleSingleWatermarkTracker() {
    this.stoppedWatermark = false;
  }

  @Override
  public synchronized Optional<Long> trackAndEmitWatermarks(final String taskId,
                                                            final String edgeId,
                                                            final int edgeIndex,
                                                            final long watermark) {

    if (stoppedWatermark) {
      return Optional.empty();
    }

    // single o2o
    // update min watermark

    // find min watermark
    if (watermark < prevEmitWatermark) {
      // it is possible
      throw new RuntimeException(taskId + " Watermark < PrevEmitWatermark" +
         watermark + " <= " + prevEmitWatermark + ", " +
        " stopped: " + stoppedWatermark);
      // return Optional.empty();
      //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
      //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
    } else {
      // Watermark timestamp progress!
      // Emit the min watermark
      if (watermark >= prevEmitWatermark + Util.WATERMARK_PROGRESS) {
        prevEmitWatermark = watermark;
        return Optional.of(watermark);
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    return sb.toString();
  }

  public void encode(final String taskId,
                     DataOutputStream dos) {
    try {
      dos.writeBoolean(stoppedWatermark);
      dos.writeLong(prevEmitWatermark);

      LOG.info("Encoding single single stage watermark tracker in {} watermark: {} ," +
        "stoppedWmark: {}, ", taskId, prevEmitWatermark, stoppedWatermark);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);

    }
  }

  public static SingleSingleWatermarkTracker decode(final String taskId,
                                                    final DataInputStream is) {
    try {
      final boolean stoppedWatermark = is.readBoolean();
      final long prevEmitWatermark = is.readLong();

      LOG.info("Decoding single single stage watermark tracker in {} watermark: {} ," +
        "stoppedWmark: {}, ", taskId, prevEmitWatermark, stoppedWatermark);
      return new SingleSingleWatermarkTracker(stoppedWatermark, prevEmitWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
