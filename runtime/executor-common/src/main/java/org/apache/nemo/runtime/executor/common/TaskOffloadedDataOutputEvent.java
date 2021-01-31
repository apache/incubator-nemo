package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;

public final class TaskOffloadedDataOutputEvent implements TaskHandlingEvent {

  private final String taskId;
  private final ByteBuf byteBuf;
  private final int pipeIndex;

  public TaskOffloadedDataOutputEvent(final String taskId,
                                      final int pipeIndex,
                                      final ByteBuf byteBuf) {
    this.taskId = taskId;
    this.byteBuf = byteBuf;
    this.pipeIndex = pipeIndex;
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
    return byteBuf;
  }

  @Override
  public DataFetcher getDataFetcher() {
    return null;
  }

  @Override
  public Object getData() {
    throw new RuntimeException("not support");
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public int getInputPipeIndex() {
    return pipeIndex;
  }

  @Override
  public Object getControl() {
    throw new RuntimeException("This is data event");
  }
}
