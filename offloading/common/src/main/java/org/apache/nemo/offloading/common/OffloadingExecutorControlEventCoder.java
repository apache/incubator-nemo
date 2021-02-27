package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class OffloadingExecutorControlEventCoder {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutorControlEventCoder.class.getName());

  public static final class OffloadingEventEncoder extends MessageToMessageEncoder<OffloadingExecutorControlEvent> {

    @Override
    protected void encode(ChannelHandlerContext ctx, OffloadingExecutorControlEvent msg, List<Object> out) throws Exception {
      final ByteBuf buf = ctx.alloc().buffer();
      buf.writeByte(0);
      buf.writeByte(msg.getType().ordinal());

      if (msg.getByteBuf() != null) {
        buf.writeBytes(msg.getByteBuf());
      }

      // LOG.info("Encoding control event for type " + msg.getType().ordinal() + ", readable " + buf.readableBytes());

      out.add(buf);
    }
  }

}
