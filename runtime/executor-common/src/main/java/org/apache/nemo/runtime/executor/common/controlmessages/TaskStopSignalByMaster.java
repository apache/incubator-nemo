package org.apache.nemo.runtime.executor.common.controlmessages;

public class TaskStopSignalByMaster {

  public final String taskId;

  public TaskStopSignalByMaster(final String taskId) {
    this.taskId = taskId;
  }
}
