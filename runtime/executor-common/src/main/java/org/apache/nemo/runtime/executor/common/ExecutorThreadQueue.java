package org.apache.nemo.runtime.executor.common;

public interface ExecutorThreadQueue {

  void addEvent(TaskHandlingEvent event);
  boolean isEmpty();
}
