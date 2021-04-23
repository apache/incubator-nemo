package org.apache.nemo.runtime.executor.common.monitoring;

import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class BackpressureSleepAlarm {
  private final ExecutorThread executorThread;
  private final long remainingSleepMs;
  private final long remainingWindow;
  private final ScheduledExecutorService scheduler;
  private final AlarmManager alarmManager;

  private boolean deactivated = false;

  public BackpressureSleepAlarm(final ExecutorThread executorThread,
                                final ScheduledExecutorService scheduler,
                                final AlarmManager alarmManager,
                                final long remainingSleepMs,
                                final long remainingWindow) {
    this.executorThread = executorThread;
    this.alarmManager = alarmManager;
    this.scheduler = scheduler;
    this.remainingSleepMs = remainingSleepMs;
    this.remainingWindow = remainingWindow;
    alarmManager.registerAlarm(this);
  }

  public void deactivate() {
    deactivated = true;
  }

  public void triggerNextSleep() {
    if (remainingSleepMs > 0 && !deactivated) {
      final long nextSleepTriggerTime = remainingWindow / (remainingSleepMs + 1);
      final long remaining = remainingSleepMs - 1;
      final long rw = remainingWindow - (nextSleepTriggerTime + 1);

      if (nextSleepTriggerTime == 0) {
        executorThread.addShortcutEvent(new TaskControlMessage(
          TaskControlMessage.TaskControlMessageType.SOURCE_SLEEP,
          -1, -1, "",
          new BackpressureSleepAlarm(executorThread, scheduler, alarmManager, remaining, rw)));
      } else {
        scheduler.schedule(() -> {
          executorThread.addShortcutEvent(new TaskControlMessage(
            TaskControlMessage.TaskControlMessageType.SOURCE_SLEEP,
            -1, -1, "",
            new BackpressureSleepAlarm(executorThread, scheduler, alarmManager, remaining, rw)));
        }, nextSleepTriggerTime, TimeUnit.MILLISECONDS);
      }
    }
  }
}
