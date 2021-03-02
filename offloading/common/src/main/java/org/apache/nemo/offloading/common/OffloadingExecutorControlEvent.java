package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

public final class OffloadingExecutorControlEvent implements Serializable {

  public enum Type {
    TASK_READY,
    TASK_FINISH_DONE,
    EXECUTOR_METRICS,
    ACTIVATE,
    DEACTIVATE,
  }

  private final Type type;
  private ByteBuf byteBuf;

  public OffloadingExecutorControlEvent(final Type type,
                                        final ByteBuf byteBuf) {
    this.type = type;
    this.byteBuf = byteBuf;
  }

  public ByteBuf getByteBuf() {
    return byteBuf;
  }

  public Type getType() {
    return type;
  }


  @Override
  public String toString() {
    return "OffloadingExecutorControlEvent:" + type.name();
  }
}
