package org.apache.nemo.runtime.master.metric;

public final class ExecutorMetricInfo {

  public final long receiveEvent;
  public final long processEvent;
  public final double cpuUse;
  public final int numExecutor;

  public ExecutorMetricInfo(final long receiveEvent,
                            final long processEvent,
                            final double cpuUse) {
    this(receiveEvent, processEvent, cpuUse, 1);
  }


  public ExecutorMetricInfo(final long receiveEvent,
                            final long processEvent,
                            final double cpuUse,
                            final int numExecutor) {
    this.receiveEvent = receiveEvent;
    this.processEvent = processEvent;
    this.cpuUse = cpuUse;
    this.numExecutor = numExecutor;
  }

}
