package org.apache.nemo.common;

public class TaskExecutorIdResponse {

  public final String taskId;
  public final String executorId;

  public TaskExecutorIdResponse(final String taskId,
                                final String executorId) {
    this.taskId = taskId;
    this.executorId = executorId;
  }
}
