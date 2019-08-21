package org.apache.nemo.common;

public final class WatermarkSend {

  public final String taskId;
  public final long watermark;

  public WatermarkSend(final String taskId,
                       final long watermark) {
    this.taskId = taskId;
    this.watermark = watermark;
  }
}
