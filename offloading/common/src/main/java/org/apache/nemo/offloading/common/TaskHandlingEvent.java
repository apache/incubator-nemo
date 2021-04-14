package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

public interface TaskHandlingEvent {

  boolean isControlMessage();

  boolean isOffloadingMessage();

  default int readableBytes() {
    return 0;
  }

  ByteBuf getDataByteBuf();

  String getEdgeId();

  Object getData();

  String getTaskId();

  int getRemoteInputPipeIndex();

  Object getControl();
}
