package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.punctuation.Watermark;

import java.io.Serializable;

/**
 * This contains a watermark and the src task index. It is used for {@link PipeOutputWriter}
 * and {@link org.apache.nemo.runtime.executor.task.MultiThreadParentTaskDataFetcher}
 * to transfer the src task index and watermark in streaming.
 */
public final class WatermarkWithIndex implements Serializable {
  private final Watermark watermark;
  private final int index;

  public WatermarkWithIndex(final Watermark watermark, final int index) {
    this.watermark = watermark;
    this.index = index;
  }

  public Watermark getWatermark() {
    return watermark;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(watermark);
    sb.append(" from ");
    sb.append(index);
    return sb.toString();
  }
}
