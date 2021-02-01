package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.nemo.runtime.executor.common.OffloadingManager;
import org.apache.nemo.runtime.executor.common.TaskHandlingEvent;

public final class SimpleOffloadingManager implements OffloadingManager {

  @Override
  public void offloading(String taskId, byte[] serializedDag) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void writeData(String taskId, TaskHandlingEvent data) {
    throw new RuntimeException("Not supported");
  }
}
