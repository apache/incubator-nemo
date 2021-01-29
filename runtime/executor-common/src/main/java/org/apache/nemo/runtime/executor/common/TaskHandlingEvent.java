package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.ir.edge.RuntimeEdge;

public interface TaskHandlingEvent {

  boolean isControlMessage();

  ByteBuf getDataByteBuf();

  DataFetcher getDataFetcher();

  Object getData();

  String getTaskId();

  int getInputPipeIndex();

  Object getControl();
}
