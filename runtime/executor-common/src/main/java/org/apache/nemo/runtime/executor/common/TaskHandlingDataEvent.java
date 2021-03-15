package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.offloading.common.TaskHandlingEvent;

import java.io.IOException;

public final class TaskHandlingDataEvent implements TaskHandlingEvent {

  private final String taskId;
  private final ByteBuf byteBuf;
  private final int pipeIndex;
  private final String edgeId;
  private final DecoderFactory decoderFactory;
  private final DecoderFactory valueDecoderFactory;
  private final boolean isStreamVertexEvent;

  public TaskHandlingDataEvent(final String taskId,
                               final String edgeId,
                               final int pipeIndex,
                               final ByteBuf byteBuf,
                               final DecoderFactory decoderFactory) {
    this.taskId = taskId;
    this.edgeId = edgeId;
    this.byteBuf = byteBuf;
    this.pipeIndex = pipeIndex;
    this.decoderFactory = decoderFactory;
    this.valueDecoderFactory = decoderFactory instanceof NemoEventDecoderFactory ?
      ((NemoEventDecoderFactory) decoderFactory).getValueDecoderFactory() : null;
    this.isStreamVertexEvent = decoderFactory instanceof BytesDecoderFactory;
  }

  @Override
  public int readableBytes() {
    return byteBuf.readableBytes();
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
    if (isStreamVertexEvent) {
      // for stream vertex !!
      final BytesDecoderFactory bytesDecoderFactory = (BytesDecoderFactory) decoderFactory;
      try {
        return bytesDecoderFactory.decode(byteBuf);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
      final Object t;
      try {
        t = decoderFactory.create(bis).decode();
        bis.close();
        byteBuf.release();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      return t;
    }
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
