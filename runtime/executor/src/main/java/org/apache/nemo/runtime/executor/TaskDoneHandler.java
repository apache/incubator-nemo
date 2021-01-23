package org.apache.nemo.runtime.executor;

import org.apache.nemo.offloading.common.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public final class TaskDoneHandler implements EventHandler<String> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDoneHandler.class.getName());

  private TaskExecutorMapWrapper taskExecutorMapWrapper;

  @Inject
  private TaskDoneHandler(final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
  }

  @Override
  public void onNext(String taskId) {
    LOG.info("Handling complete deletion of task " + taskId);
    // taskExecutorMapWrapper.removeTask(taskId);
    // Send stop done signal to master
  }
}
