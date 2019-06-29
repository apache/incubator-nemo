package org.apache.nemo.runtime.lambdaexecutor;

public final class ThpEvent {

  public final String taskId;
  public final long thp;

  public ThpEvent(final String taskId,
                  final long thp) {
    this.taskId = taskId;
    this.thp = thp;
  }
}
