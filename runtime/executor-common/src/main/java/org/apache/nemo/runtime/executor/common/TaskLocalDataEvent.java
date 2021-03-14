package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;


public final class TaskLocalDataEvent implements TaskHandlingEvent {

  private final String taskId;
  private final String edgeId;
  private final Object event;
  private final int index;

  public TaskLocalDataEvent(final String taskId,
                            final String edgeId,
                            final int index,
                            final Object event) {
    this.taskId = taskId;
    this.edgeId = edgeId;
    this.index = index;
    this.event = event;
  }

  @Override
  public boolean isControlMessage() {
    return false;
  }

  @Override
  public boolean isOffloadingMessage() {
    return false;
  }

  @Override
  public ByteBuf getDataByteBuf() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public String getEdgeId() {
    return edgeId;
  }

  @Override
  public Object getData() {
    return event;
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public int getInputPipeIndex() {
    return index;
  }

  @Override
  public Object getControl() {
    throw new RuntimeException("This is data event");
  }
}
