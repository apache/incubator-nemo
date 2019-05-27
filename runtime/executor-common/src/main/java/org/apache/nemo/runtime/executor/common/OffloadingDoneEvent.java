package org.apache.nemo.runtime.executor.common;

public class OffloadingDoneEvent {
  public final String taskId;

  public OffloadingDoneEvent(final String taskId) {
    this.taskId = taskId;
  }
}
