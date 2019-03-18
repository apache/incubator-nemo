package org.apache.nemo.runtime.executor.task;

public final class OffloadingRequestEvent {
  public final boolean isEnd;
  public final boolean isStart;
  public final long startTime;

  public OffloadingRequestEvent(final boolean isStart, final long startTime) {
    this.isStart = isStart;
    this.isEnd = !isStart;
    this.startTime = startTime;
  }
}
