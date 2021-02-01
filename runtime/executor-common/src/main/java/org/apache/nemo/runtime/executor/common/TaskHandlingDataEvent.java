package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.nemo.common.ir.edge.RuntimeEdge;

import java.io.IOException;
import java.io.InputStream;

public final class TaskHandlingDataEvent implements TaskHandlingEvent {

  private final String taskId;
  private final ByteBuf byteBuf;
  private final int pipeIndex;
  private final String edgeId;
  private final Serializer serializer;

  public TaskHandlingDataEvent(final String taskId,
                               final String edgeId,
                               final int pipeIndex,
                               final ByteBuf byteBuf,
                               final Serializer serializer) {
    this.taskId = taskId;
    this.edgeId = edgeId;
    this.byteBuf = byteBuf;
    this.pipeIndex = pipeIndex;
    this.serializer = serializer;
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
    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    final Object t;
    try {
      t = serializer.getDecoderFactory().create(bis).decode();
      bis.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return t;
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
