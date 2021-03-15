package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.nemo.common.WatermarkWithIndex;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.TaskHandlingEvent;

import java.io.IOException;

public final class TaskRelayDataEvent implements TaskHandlingEvent {

  private final String taskId;
  private final Object event;
  private final int pipeIndex;
  private final String edgeId;
  public final DecoderFactory valueDecoderFactory;
  public final boolean remoteLocal;

  public TaskRelayDataEvent(final String taskId,
                            final String edgeId,
                            final int pipeIndex,
                            final Object event,
                            final DecoderFactory valueDecoderFactory) {
    this.taskId = taskId;
    this.edgeId = edgeId;
    this.event = event;
    this.pipeIndex = pipeIndex;
    this.valueDecoderFactory = valueDecoderFactory;
    this.remoteLocal = event instanceof TimestampAndValue && ((TimestampAndValue) event).value instanceof ByteBuf;

  }

  @Override
  public int readableBytes() {
    if (remoteLocal) {
      return ((ByteBuf) ((TimestampAndValue) event).value).readableBytes();
    }

    return 0;
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
    return null;
  }

  @Override
  public String getEdgeId() {
    return edgeId;
  }

  @Override
  public Object getData() {
    // Remote-Local
    if (remoteLocal) {
      final TimestampAndValue timestampAndValue = (TimestampAndValue) event;
      final ByteBufInputStream bis = new ByteBufInputStream((ByteBuf)timestampAndValue.value);

      try {
        final TimestampAndValue result = new TimestampAndValue<>(timestampAndValue.timestamp,
          valueDecoderFactory.create(bis).decode());

        ((ByteBuf) timestampAndValue.value).release();
        return result;

      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      // Local, Local
      return event;
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
