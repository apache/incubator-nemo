package org.apache.nemo.common;

public final class WatermarkResponse {

  public final String taskId;
  public final long watermark;

  public WatermarkResponse(final String taskId,
                           final long watermark) {
    this.taskId = taskId;
    this.watermark = watermark;
  }
}
