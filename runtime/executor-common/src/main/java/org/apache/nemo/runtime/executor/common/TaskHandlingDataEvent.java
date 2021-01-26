package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.nemo.common.ir.edge.RuntimeEdge;

import java.io.IOException;

public final class TaskHandlingDataEvent implements TaskHandlingEvent {

  private final String taskId;
  private final ByteBuf byteBuf;
  private final DataFetcher dataFetcher;
  private final Serializer serializer;

  public TaskHandlingDataEvent(final String taskId,
                               final DataFetcher dataFetcher,
                               final ByteBuf byteBuf,
                               final Serializer serializer) {
    this.taskId = taskId;
    this.dataFetcher = dataFetcher;
    this.byteBuf = byteBuf;
    this.serializer = serializer;
  }

  @Override
  public boolean isControlMessage() {
    return false;
  }

  @Override
  public ByteBuf getDataByteBuf() {
    return byteBuf;
  }

  @Override
  public DataFetcher getDataFetcher() {
    return dataFetcher ;
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
  public Object getControl() {
    throw new RuntimeException("This is data event");
  }
}
