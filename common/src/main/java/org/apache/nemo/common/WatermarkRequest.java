package org.apache.nemo.common;

public final class WatermarkRequest {

  public final String stageId;

  public WatermarkRequest(final String stageId) {
    this.stageId = stageId;
  }
}
