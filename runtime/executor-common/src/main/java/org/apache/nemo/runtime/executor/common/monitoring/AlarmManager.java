package org.apache.nemo.runtime.executor.common.monitoring;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class AlarmManager {

  private final List<BackpressureSleepAlarm> activeAlarms;

  public AlarmManager() {
    this.activeAlarms = new LinkedList<>();
  }

  public synchronized void registerAlarm(final BackpressureSleepAlarm alarm) {
    this.activeAlarms.add(alarm);
  }

  public synchronized void deactivateAlarms() {
    activeAlarms.forEach(alarm -> {
      alarm.deactivate();
    });

    activeAlarms.clear();
  }
}
