package org.apache.nemo.common;

public final class WatermarkRequest {

  public final String taskId;

  public WatermarkRequest(final String taskId) {
    this.taskId = taskId;
  }
}
