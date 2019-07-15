package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.executor.TinyTaskWorker;

public final class OffloadingRequestEvent {
  public final boolean isEnd;
  public final boolean isStart;
  public final long startTime;
  public final TinyTaskWorker worker;

  public OffloadingRequestEvent(final boolean isStart, final long startTime,
                                final TinyTaskWorker worker) {
    this.isStart = isStart;
    this.isEnd = !isStart;
    this.startTime = startTime;
    this.worker = worker;
  }
}
