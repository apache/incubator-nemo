package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.runtime.executor.common.OffloadingManager;
import org.apache.nemo.runtime.executor.common.TaskHandlingEvent;

import javax.inject.Inject;

public final class OffloadingManagerImpl implements OffloadingManager {

  @Inject
  private OffloadingManagerImpl() {

  }

  @Override
  public void offloading(String taskId, byte[] serializedDag) {

  }

  @Override
  public void writeData(String taskId, TaskHandlingEvent event) {

  }
}
