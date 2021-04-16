package org.apache.nemo.runtime.executor.common.controlmessages;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskToBeStoppedMap {
  public final Map<String, Boolean> taskToBeStopped;

  @Inject
  private TaskToBeStoppedMap() {
    this.taskToBeStopped = new ConcurrentHashMap<>();
  }
}
