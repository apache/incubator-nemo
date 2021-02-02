package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface OffloadingManager {
  void createWorker(int num);
  void offloading(String taskId);
  void deoffloading(String taskId);
  void writeData(String taskId, TaskHandlingEvent data);
}
