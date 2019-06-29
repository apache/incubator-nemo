package org.apache.nemo.runtime.lambdaexecutor;

public final class ThpEvent {

  public final String taskId;
  public final String opId;
  public final long thp;

  public ThpEvent(final String taskId,
                  final String opId,
                  final long thp) {
    this.taskId = taskId;
    this.opId = opId;
    this.thp = thp;
  }
}
