package org.apache.nemo.runtime.master.metric;

public final class ExecutorMetricInfo {

  public final long receiveEvent;
  public final long processEvent;
  public final long sourceEvent;
  public final double cpuUse;
  public final int numExecutor;

  public ExecutorMetricInfo(final long receiveEvent,
                            final long processEvent,
                            final long sourceEvent,
                            final double cpuUse) {
    this(receiveEvent, processEvent, sourceEvent, cpuUse, 1);
  }


  public ExecutorMetricInfo(final long receiveEvent,
                            final long processEvent,
                            final long sourceEvent,
                            final double cpuUse,
                            final int numExecutor) {
    this.receiveEvent = receiveEvent;
    this.processEvent = processEvent;
    this.sourceEvent = sourceEvent;
    this.cpuUse = cpuUse;
    this.numExecutor = numExecutor;
  }

}
