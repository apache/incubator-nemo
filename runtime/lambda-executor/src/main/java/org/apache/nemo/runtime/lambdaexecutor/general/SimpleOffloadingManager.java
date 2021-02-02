package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.nemo.runtime.executor.common.OffloadingManager;
import org.apache.nemo.offloading.common.TaskHandlingEvent;

public final class SimpleOffloadingManager implements OffloadingManager {

  @Override
  public void createWorker(int num) {

  }

  @Override
  public void offloading(String taskId) {

  }

  @Override
  public void deoffloading(String taskId) {

  }

  @Override
  public void writeData(String taskId, TaskHandlingEvent data) {
    throw new RuntimeException("Not supported");
  }
}
