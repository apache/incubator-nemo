package org.apache.nemo.offloading.common;

import java.util.List;

public final class TaskCaching {

  public final List taskIncomingEdges;
  public final List taskOutgoingEdges;
  public final Object executionProperties;
  // public final Object irDag;

  public TaskCaching(final List taskIncomingEdges,
                     final List taskOutgoingEdges,
                     final Object executionProperties) {
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.executionProperties = executionProperties;
    // this.irDag = irDag;
  }

}
