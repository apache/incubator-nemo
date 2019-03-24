package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.task.TaskExecutor;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskExecutorMapWrapper {

  public final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  @Inject
  private TaskExecutorMapWrapper() {
    this.taskExecutorMap = new ConcurrentHashMap<>();
  }
}
