package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

public interface TaskHandlingEvent {

  boolean isControlMessage();

  boolean isOffloadingMessage();

  ByteBuf getDataByteBuf();

  String getEdgeId();

  Object getData();

  String getTaskId();

  int getInputPipeIndex();

  Object getControl();
}
