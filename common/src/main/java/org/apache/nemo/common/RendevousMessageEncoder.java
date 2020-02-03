package org.apache.nemo.common;

import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class RendevousMessageEncoder extends MessageToMessageEncoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(RendevousMessageEncoder.class);

  public enum Type {
    REQUEST,
    RESPONSE,
    REGISTER,
    WATERMARK_REQUEST,
    WATERMARK_RESPONSE,
    WATERMARK_SEND,

    REQUEST_TASK_EXECUTOR_ID,
    RESPONSE_TASK_EXECUTOR_ID,
  }

  public RendevousMessageEncoder() {

  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {

    final ByteBufOutputStream bos = new ByteBufOutputStream(ctx.alloc().buffer());

    if (msg instanceof RendevousRequest) {
      bos.writeInt(Type.REQUEST.ordinal());

      final RendevousRequest req = (RendevousRequest) msg;
      bos.writeUTF(req.dst);

      out.add(bos.buffer());

    } else if (msg instanceof RendevousResponse) {

      LOG.info("Encoding response");

      bos.writeInt(Type.RESPONSE.ordinal());

      final RendevousResponse res = (RendevousResponse) msg;
      bos.writeUTF(res.dst);
      bos.writeUTF(res.address);

      out.add(bos.buffer());

    } else if (msg instanceof  RendevousRegister) {
      bos.writeInt(Type.REGISTER.ordinal());

      final RendevousRegister req = (RendevousRegister) msg;
      bos.writeUTF(req.dst);

      out.add(bos.buffer());
    } else if (msg instanceof WatermarkRequest) {
      bos.writeInt(Type.WATERMARK_REQUEST.ordinal());

      final WatermarkRequest req = (WatermarkRequest) msg;
      bos.writeUTF(req.taskId);
      out.add(bos.buffer());

    } else if (msg instanceof WatermarkSend) {
      bos.writeInt(Type.WATERMARK_SEND.ordinal());

      final WatermarkSend req = (WatermarkSend) msg;
      bos.writeUTF(req.taskId);
      bos.writeLong(req.watermark);
      out.add(bos.buffer());

    } else if (msg instanceof WatermarkResponse) {
      bos.writeInt(Type.WATERMARK_RESPONSE.ordinal());

      final WatermarkResponse req = (WatermarkResponse) msg;
      bos.writeUTF(req.taskId);
      bos.writeLong(req.watermark);
      out.add(bos.buffer());

    } else if (msg instanceof TaskExecutorIdResponse) {
      bos.writeInt(Type.RESPONSE_TASK_EXECUTOR_ID.ordinal());
      final TaskExecutorIdResponse req = (TaskExecutorIdResponse) msg;
      bos.writeUTF(req.taskId);
      bos.writeUTF(req.executorId);
      out.add(bos.buffer());
    } else if (msg instanceof TaskExecutorIdRequest) {

      bos.writeInt(Type.REQUEST_TASK_EXECUTOR_ID.ordinal());
      final TaskExecutorIdRequest req = (TaskExecutorIdRequest) msg;
      bos.writeUTF(req.taskId);
      out.add(bos.buffer());

    } else {
      throw new RuntimeException("Unsupported type " + msg);
    }
  }


}
