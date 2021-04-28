package org.apache.nemo.runtime.master.metric;

public final class ExecutorMetricInfo {

  public final long receiveEvent;
  public final long processEvent;

  public ExecutorMetricInfo(final long receiveEvent,
                     final long processEvent) {
    this.receiveEvent = receiveEvent;
    this.processEvent = processEvent;
  }

}
