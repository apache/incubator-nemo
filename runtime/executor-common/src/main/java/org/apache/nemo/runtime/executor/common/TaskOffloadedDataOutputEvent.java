package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;

import java.util.List;

public final class TaskOffloadedDataOutputEvent implements TaskHandlingEvent {

  private final String taskId;
  private final String edgeId;
  private final List<String> dstIds;
  private final ByteBuf byteBuf;

  public TaskOffloadedDataOutputEvent(final String taskId,
                                      final String edgeId,
                                      final List<String> dstIds,
                                      final ByteBuf byteBuf) {
    this.taskId = taskId;
    this.byteBuf = byteBuf;
    this.edgeId = edgeId;
    this.dstIds = dstIds;
  }

  public List<String> getDstIds() {
    return dstIds;
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
  public String getEdgeId() {
    return edgeId;
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
  public int getRemoteInputPipeIndex() {
    throw new RuntimeException("not supported");
  }

  @Override
  public Object getControl() {
    throw new RuntimeException("This is data event");
  }
}
