package org.apache.nemo.runtime.executor.common.controlmessages.offloading;

public final class TaskFinish {

  public final String taskId;

  public TaskFinish(final String taskId) {
    this.taskId = taskId;
  }
}
