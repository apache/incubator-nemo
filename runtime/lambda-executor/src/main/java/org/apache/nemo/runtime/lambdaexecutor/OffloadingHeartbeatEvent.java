package org.apache.nemo.runtime.lambdaexecutor;

public final class OffloadingHeartbeatEvent {

  public final int taskIndex;
  public final long time;

  public OffloadingHeartbeatEvent(final int taskIndex,
                                  final long time) {
    this.taskIndex = taskIndex;
    this.time = time;
  }
}
