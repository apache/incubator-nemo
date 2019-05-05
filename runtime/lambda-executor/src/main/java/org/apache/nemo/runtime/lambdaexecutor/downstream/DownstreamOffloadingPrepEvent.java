package org.apache.nemo.runtime.lambdaexecutor.downstream;

public final class DownstreamOffloadingPrepEvent {
  // key: srcId, destId, value: data
  public final int taskIndex;

  public DownstreamOffloadingPrepEvent(final int taskIndex) {
    this.taskIndex = taskIndex;
  }
}
