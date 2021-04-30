package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class PartialTaskDoneChecker {

  private final List<String> scheduledPartials;
  private final ScheduledExecutorService scheduler;

  @Inject
  private PartialTaskDoneChecker(final TaskExecutorMapWrapper taskExecutorThreadMap) {
    this.scheduledPartials = new LinkedList<>();
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(() -> {

      synchronized (scheduledPartials) {
        scheduledPartials.forEach(partialTask -> {
          taskExecutorThreadMap.getTaskExecutorThread(partialTask)
            .addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.R3_TASK_STATE_CHECK,
              -1, -1,
              partialTask, null));
        });
      }

    }, 100, 100, TimeUnit.MILLISECONDS);
  }

  public void registerPartialDoneReadyTask(final String taskId) {
    synchronized (scheduledPartials) {
      scheduledPartials.add(taskId);
    }
  }

  public void deregisterPartialDoneTask(final String taskId) {
    synchronized (scheduledPartials) {
      scheduledPartials.remove(taskId);
    }
  }
}
