package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.nemo.offloading.common.TaskOffloadingEvent;

import java.io.IOException;

public final class TaskHandlingOffloadingEvent implements TaskHandlingEvent {

  private final String taskId;
  private final TaskOffloadingEvent event;

  public TaskHandlingOffloadingEvent(final String taskId,
                                     final TaskOffloadingEvent offloadingEvent) {
    this.taskId = taskId;
    this.event = offloadingEvent;
  }

  @Override
  public boolean isControlMessage() {
    return false;
  }

  @Override
  public boolean isOffloadingMessage() {
    return true;
  }

  @Override
  public ByteBuf getDataByteBuf() {
    throw new RuntimeException("exception");
  }

  @Override
  public DataFetcher getDataFetcher() {
    throw new RuntimeException("exception");
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
    throw new RuntimeException("exception");
  }

  @Override
  public Object getControl() {
    throw new RuntimeException("This is offloading event");
  }
}
