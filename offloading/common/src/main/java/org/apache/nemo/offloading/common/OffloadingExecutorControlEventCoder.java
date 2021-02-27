package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public final class OffloadingExecutorControlEventCoder {

  public static final class OffloadingEventEncoder extends MessageToMessageEncoder<OffloadingExecutorControlEvent> {

    @Override
    protected void encode(ChannelHandlerContext ctx, OffloadingExecutorControlEvent msg, List<Object> out) throws Exception {
      final ByteBuf buf = ctx.alloc().buffer(1 + Integer.BYTES);
      buf.writeByte(0);
      buf.writeInt(msg.getType().ordinal());

      if (msg.getByteBuf() != null) {
        final CompositeByteBuf compositeByteBuf =
          ctx.alloc().compositeBuffer(2)
            .addComponents(true, buf, msg.getByteBuf());

        out.add(compositeByteBuf);

      } else {
        out.add(buf);
      }
    }
  }

}
