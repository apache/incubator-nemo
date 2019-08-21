package org.apache.nemo.common;

public final class WatermarkResponse {

  public final String stageId;
  public final long watermark;

  public WatermarkResponse(final String stageId,
                           final long watermark) {
    this.stageId = stageId;
    this.watermark = watermark;
  }
}
