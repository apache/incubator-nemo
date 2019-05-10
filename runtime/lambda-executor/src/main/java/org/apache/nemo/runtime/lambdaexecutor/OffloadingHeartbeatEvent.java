package org.apache.nemo.runtime.lambdaexecutor;

public final class OffloadingHeartbeatEvent {

  public final String taskId;
  public final int taskIndex;
  public final long time;

  public OffloadingHeartbeatEvent(final String taskId,
                                  final int taskIndex,
                                  final long time) {
    this.taskId = taskId;
    this.taskIndex = taskIndex;
    this.time = time;
  }
}
