package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;

public final class TaskOffloadingEvent implements TaskHandlingEvent {

  public enum ControlType {
    SEND_TO_OFFLOADING_WORKER,
    OFFLOAD_DONE,
    WORKER_READY,
    DEOFFLOADING,
    DEOFFLOADING_DONE,
  }

  private final ControlType type;
  private final Object event;
  private final String taskId;

  public TaskOffloadingEvent(final String taskId,
                             final ControlType type,
                             final Object event) {
    this.taskId = taskId;
    this.type = type;
    this.event = event;
  }

  public ControlType getType() {
    return type;
  }

  public Object getEvent() {
    return event;
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
    throw new RuntimeException("not support");
  }

  @Override
  public String getEdgeId() {
    return null;
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
    throw new RuntimeException("not support");
  }

  @Override
  public Object getControl() {
    throw new RuntimeException("not support");
  }
}
