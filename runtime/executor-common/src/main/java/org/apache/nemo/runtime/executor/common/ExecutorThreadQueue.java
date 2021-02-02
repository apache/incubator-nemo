package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface ExecutorThreadQueue {

  void addEvent(TaskHandlingEvent event);
  boolean isEmpty();
}
