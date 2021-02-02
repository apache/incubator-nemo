package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface ControlEventHandler {
  void handleControlEvent(TaskHandlingEvent event);
}
